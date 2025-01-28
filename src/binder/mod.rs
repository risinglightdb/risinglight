// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::vec::Vec;

use egg::{Id, Language};
use itertools::Itertools;

use crate::array;
use crate::catalog::function::FunctionCatalog;
use crate::catalog::{RootCatalog, RootCatalogRef, TableRefId};
use crate::parser::*;
use crate::planner::{Expr as Node, RecExpr, TypeSchemaAnalysis};
use crate::types::DataValue;

pub mod copy;
mod create_function;
mod create_index;
mod create_table;
mod create_view;
mod delete;
mod drop;
mod error;
mod expr;
mod insert;
mod select;
mod table;

pub use self::create_function::CreateFunction;
pub use self::create_index::{CreateIndex, IndexType, VectorDistance};
pub use self::create_table::CreateTable;
pub use self::error::BindError;
use self::error::ErrorKind;

pub type Result<T = Id> = std::result::Result<T, BindError>;

/// The binder resolves all expressions referring to schema objects such as
/// tables or views with their column names and types.
pub struct Binder {
    egraph: egg::EGraph<Node, TypeSchemaAnalysis>,
    catalog: Arc<RootCatalog>,
    contexts: Vec<Context>,
    /// The number of occurrences of each table in the query.
    table_occurrences: HashMap<TableRefId, u32>,
    /// The context used in sql udf binding
    udf_context: UdfContext,
}

#[derive(Clone, Debug, Default)]
pub struct UdfContext {
    /// The mapping from `sql udf parameters` to a bound `Id` generated from `ast
    /// expressions` Note: The expressions are constructed during runtime, correspond to the
    /// actual users' input
    udf_param_context: HashMap<String, Id>,

    /// The global counter that records the calling stack depth
    /// of the current binding sql udf chain
    udf_global_counter: u32,
}

impl UdfContext {
    pub fn new() -> Self {
        Self {
            udf_param_context: HashMap::new(),
            udf_global_counter: 0,
        }
    }

    pub fn global_count(&self) -> u32 {
        self.udf_global_counter
    }

    pub fn incr_global_count(&mut self) {
        self.udf_global_counter += 1;
    }

    pub fn _is_empty(&self) -> bool {
        self.udf_param_context.is_empty()
    }

    pub fn update_context(&mut self, context: HashMap<String, Id>) {
        self.udf_param_context = context;
    }

    pub fn _clear(&mut self) {
        self.udf_global_counter = 0;
        self.udf_param_context.clear();
    }

    pub fn get_expr(&self, name: &str) -> Option<&Id> {
        self.udf_param_context.get(name)
    }

    pub fn get_context(&self) -> HashMap<String, Id> {
        self.udf_param_context.clone()
    }

    /// A common utility function to extract sql udf
    /// expression out from the input `ast`
    pub fn extract_udf_expression(ast: Vec<Statement>) -> Result<Expr> {
        if ast.len() != 1 {
            return Err(ErrorKind::InvalidExpression(
                "the query for sql udf should contain only one statement".to_string(),
            )
            .into());
        }

        // Extract the expression out
        let Statement::Query(query) = ast[0].clone() else {
            return Err(ErrorKind::InvalidExpression(
                "invalid function definition, please recheck the syntax".to_string(),
            )
            .into());
        };

        let SetExpr::Select(select) = *query.body else {
            return Err(ErrorKind::InvalidExpression(
                "missing `select` body for sql udf expression, please recheck the syntax"
                    .to_string(),
            )
            .into());
        };

        if select.projection.len() != 1 {
            return Err(ErrorKind::InvalidExpression(
                "`projection` should contain only one `SelectItem`".to_string(),
            )
            .into());
        }

        let SelectItem::UnnamedExpr(expr) = select.projection[0].clone() else {
            return Err(ErrorKind::InvalidExpression(
                "expect `UnnamedExpr` for `projection`".to_string(),
            )
            .into());
        };

        Ok(expr)
    }

    pub fn create_udf_context(
        args: &[FunctionArg],
        catalog: &Arc<FunctionCatalog>,
    ) -> Result<HashMap<String, Expr>> {
        let mut ret: HashMap<String, Expr> = HashMap::new();
        for (i, current_arg) in args.iter().enumerate() {
            if let FunctionArg::Unnamed(_arg) = current_arg {
                match current_arg {
                    FunctionArg::Unnamed(arg) => {
                        let FunctionArgExpr::Expr(e) = arg else {
                            return Err(
                                ErrorKind::InvalidExpression("invalid syntax".into()).into()
                            );
                        };
                        if catalog.arg_names[i].is_empty() {
                            ret.insert(format!("${}", i + 1), e.clone());
                        } else {
                            // The index mapping here is accurate
                            // So that we could directly use the index
                            ret.insert(catalog.arg_names[i].clone(), e.clone());
                        }
                    }
                    _ => return Err(ErrorKind::InvalidExpression("invalid syntax".into()).into()),
                }
            }
        }
        Ok(ret)
    }
}

pub fn bind_header(mut chunk: array::Chunk, stmt: &Statement) -> array::Chunk {
    let header_values = match stmt {
        Statement::CreateTable { .. } => vec!["$create".to_string()],
        Statement::Drop { .. } => vec!["$drop".to_string()],
        Statement::Insert { .. } => vec!["$insert.row_counts".to_string()],
        Statement::Explain { .. } => vec!["$explain".to_string()],
        Statement::Delete { .. } => vec!["$delete.row_counts".to_string()],
        _ => Vec::new(),
    };

    if !header_values.is_empty() {
        chunk.set_header(header_values);
    }

    chunk
}

/// A set of current accessible table and column aliases.
///
/// Context can be nested to represent subqueries.
/// Binder maintains a stack of contexts.
#[derive(Debug, Default)]
struct Context {
    /// Defined CTEs.
    /// `cte_name` -> (`query_id`, `column_alias` -> id)
    ctes: HashMap<String, (Id, HashMap<String, Id>)>,
    /// Table aliases that can be accessed from the current query.
    table_aliases: HashSet<String>,
    /// Column aliases that can be accessed from the current query.
    /// `column_alias` -> (`table_alias` -> id)
    column_aliases: HashMap<String, HashMap<String, Id>>,
    /// Column aliases that can be accessed from the outside query.
    /// `column_alias` -> id
    output_aliases: HashMap<String, Id>,
}

impl Binder {
    /// Create a new binder.
    pub fn new(catalog: Arc<RootCatalog>) -> Self {
        Binder {
            catalog: catalog.clone(),
            egraph: egg::EGraph::new(TypeSchemaAnalysis { catalog }),
            contexts: vec![Context::default()],
            table_occurrences: HashMap::new(),
            udf_context: UdfContext::new(),
        }
    }

    /// Bind a statement.
    pub fn bind(&mut self, stmt: Statement) -> Result<RecExpr> {
        let id = self.bind_stmt(stmt)?;
        let extractor = egg::Extractor::new(&self.egraph, egg::AstSize);
        let (_, best) = extractor.find_best(id);
        Ok(best)
    }

    fn bind_stmt(&mut self, stmt: Statement) -> Result {
        match stmt {
            Statement::CreateIndex(create_index) => self.bind_create_index(create_index),
            Statement::CreateTable(create_table) => self.bind_create_table(create_table),
            Statement::CreateView {
                name,
                columns,
                query,
                ..
            } => self.bind_create_view(name, columns, *query),
            Statement::CreateFunction(create_function) => {
                self.bind_create_function(create_function)
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                ..
            } => self.bind_drop(object_type, if_exists, names, cascade),
            Statement::Insert(insert) => self.bind_insert(insert),
            Statement::Delete(delete) => self.bind_delete(delete),
            Statement::Copy {
                source,
                to,
                target,
                options,
                ..
            } => self.bind_copy(source, to, target, &options),
            Statement::Query(query) => self.bind_query(*query).map(|(id, _)| id),
            Statement::Explain {
                statement, analyze, ..
            } => self.bind_explain(*statement, analyze),
            Statement::Pragma { name, value, .. } => self.bind_pragma(name, value),
            Statement::SetVariable {
                variables, value, ..
            } => self.bind_set(variables.as_ref(), value),
            Statement::ShowVariable { .. }
            | Statement::ShowCreate { .. }
            | Statement::ShowColumns { .. } => Err(ErrorKind::NotSupportedTSQL.into()),
            _ => Err(ErrorKind::InvalidSQL.into()),
        }
    }

    /// Add an column alias to the current context.
    fn add_alias(&mut self, column_name: String, table_name: String, id: Id) {
        let context = self.contexts.last_mut().unwrap();
        context
            .column_aliases
            .entry(column_name)
            .or_default()
            .insert(table_name, id);
        // may override the same name
    }

    /// Add a table alias.
    fn add_table_alias(&mut self, table_name: &str) -> Result<()> {
        let context = self.contexts.last_mut().unwrap();
        if !context.table_aliases.insert(table_name.into()) {
            return Err(ErrorKind::DuplicatedAlias(table_name.into()).into());
        }
        Ok(())
    }

    /// Add an alias so that it can be accessed from the outside query.
    fn add_output_alias(&mut self, column_name: String, id: Id) {
        let context = self.contexts.last_mut().unwrap();
        context.output_aliases.insert(column_name, id);
    }

    /// Add a CTE to the current context.
    fn add_cte(
        &mut self,
        table_ident: &Ident,
        query: Id,
        columns: HashMap<String, Id>,
    ) -> Result<()> {
        let context = self.contexts.last_mut().unwrap();
        let table_name = table_ident.value.to_lowercase();
        if context
            .ctes
            .insert(table_name.clone(), (query, columns))
            .is_some()
        {
            return Err(ErrorKind::DuplicatedCteName(table_name).with_span(table_ident.span));
        }
        Ok(())
    }

    /// Find an alias.
    fn find_alias(&self, column_ident: &Ident, table_ident: Option<&Ident>) -> Result {
        for context in self.contexts.iter().rev() {
            if let Some(map) = context.column_aliases.get(&column_ident.value) {
                if let Some(table_ident) = table_ident {
                    if let Some(id) = map.get(&table_ident.value) {
                        return Ok(*id);
                    }
                } else if map.len() == 1 {
                    return Ok(*map.values().next().unwrap());
                } else {
                    let use_ = map
                        .keys()
                        .map(|table_name| format!("\"{table_name}.{column_ident}\""))
                        .join(" or ");
                    return Err(ErrorKind::AmbiguousColumn(column_ident.value.clone(), use_)
                        .with_span(column_ident.span));
                }
            }
        }
        Err(ErrorKind::InvalidColumn(column_ident.value.clone()).with_span(column_ident.span))
    }

    /// Find an CTE.
    fn find_cte(&self, cte_name: &str) -> Option<&(Id, HashMap<String, Id>)> {
        self.contexts
            .iter()
            .rev()
            .find_map(|ctx| ctx.ctes.get(cte_name))
    }

    fn type_(&self, id: Id) -> Result<crate::types::DataType> {
        Ok(self.egraph[id].data.type_.clone()?)
    }

    fn schema(&self, id: Id) -> Vec<Id> {
        self.egraph[id].data.schema.clone()
    }

    fn aggs(&self, id: Id) -> &[Node] {
        &self.egraph[id].data.aggs
    }

    fn overs(&self, id: Id) -> &[Node] {
        &self.egraph[id].data.overs
    }

    fn node(&self, id: Id) -> &Node {
        &self.egraph[id].nodes[0]
    }

    #[allow(dead_code)]
    fn recexpr(&self, id: Id) -> RecExpr {
        self.node(id).build_recexpr(|id| self.node(id).clone())
    }

    /// Wrap the node with `Ref` if it is not a column unit.
    fn wrap_ref(&mut self, id: Id) -> Id {
        match self.node(id) {
            Node::Column(_) | Node::Ref(_) => id,
            _ => self.egraph.add(Node::Ref(id)),
        }
    }

    fn _udf_context_mut(&mut self) -> &mut UdfContext {
        &mut self.udf_context
    }

    fn catalog(&self) -> RootCatalogRef {
        self.catalog.clone()
    }

    fn bind_explain(&mut self, query: Statement, analyze: bool) -> Result {
        let id = self.bind_stmt(query)?;
        let id = self.egraph.add(match analyze {
            false => Node::Explain(id),
            true => Node::Analyze(id),
        });
        Ok(id)
    }

    pub fn bind_pragma(&mut self, name: ObjectName, value: Option<Value>) -> Result {
        let name_string = name.to_string().to_lowercase();
        match name_string.as_str() {
            "enable_optimizer" | "disable_optimizer" => {}
            name_str => return Err(ErrorKind::NoPragma(name_str.into()).with_spanned(&name)),
        }
        let name_id = self.egraph.add(Node::Constant(name_string.into()));
        let value_id = self.egraph.add(Node::Constant(
            value.map_or(DataValue::Null, DataValue::from),
        ));
        let id = self.egraph.add(Node::Pragma([name_id, value_id]));
        Ok(id)
    }

    pub fn bind_set(&mut self, variables: &[ObjectName], values: Vec<Expr>) -> Result {
        if variables.len() != 1 || values.len() != 1 {
            return Err(ErrorKind::InvalidSQL.into());
        }
        let name_id = self
            .egraph
            .add(Node::Constant(variables[0].to_string().into()));
        let value_id = self.bind_expr(values.into_iter().next().unwrap())?;
        let id = self.egraph.add(Node::Set([name_id, value_id]));
        Ok(id)
    }
}

/// Split an object name into `(schema name, table name)`.
fn split_name(name: &ObjectName) -> Result<(&str, &str)> {
    Ok(match name.0.as_slice() {
        [table] => (RootCatalog::DEFAULT_SCHEMA_NAME, &table.value),
        [schema, table] => (&schema.value, &table.value),
        _ => return Err(ErrorKind::InvalidTableName(name.0.clone()).with_spanned(name)),
    })
}

/// Convert an object name into lower case
fn lower_case_name(name: &ObjectName) -> ObjectName {
    ObjectName(
        name.0
            .iter()
            .map(|ident| Ident::with_span(ident.span, ident.value.to_lowercase()))
            .collect::<Vec<_>>(),
    )
}
