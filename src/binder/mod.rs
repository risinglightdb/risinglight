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
use crate::planner::{Expr as Node, RecExpr, TypeError, TypeSchemaAnalysis};

pub mod copy;
mod create_function;
mod create_table;
mod create_view;
mod delete;
mod drop;
mod expr;
mod insert;
mod select;
mod table;
mod udf;

pub use self::create_function::*;
pub use self::create_table::*;
pub use self::udf::*;

pub type Result<T = Id> = std::result::Result<T, BindError>;

/// The error type of bind operations.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum BindError {
    #[error("invalid schema {0:?}")]
    InvalidSchema(String),
    #[error("invalid table {0:?}")]
    InvalidTable(String),
    #[error("invalid column {0:?}")]
    InvalidColumn(String),
    #[error("table {0:?} already exists")]
    TableExists(String),
    #[error("column {0:?} already exists")]
    ColumnExists(String),
    #[error("duplicated alias {0:?}")]
    DuplicatedAlias(String),
    #[error("duplicate CTE name {0:?}")]
    DuplicatedCteName(String),
    #[error("table {0:?} has {1} columns available but {2} columns specified")]
    ColumnCountMismatch(String, usize, usize),
    #[error("invalid expression {0}")]
    InvalidExpression(String),
    #[error("not nullable column {0:?}")]
    NotNullableColumn(String),
    #[error("ambiguous column {0:?} (use {1})")]
    AmbiguousColumn(String, String),
    #[error("invalid table name {0:?}")]
    InvalidTableName(Vec<Ident>),
    #[error("SQL not supported")]
    NotSupportedTSQL,
    #[error("invalid SQL")]
    InvalidSQL,
    #[error("cannot cast {0:?} to {1:?}")]
    CastError(crate::types::DataValue, crate::types::DataType),
    #[error("{0}")]
    BindFunctionError(String),
    #[error("type error: {0}")]
    TypeError(#[from] TypeError),
    #[error("aggregate function calls cannot be nested")]
    NestedAgg,
    #[error("WHERE clause cannot contain aggregates")]
    AggInWhere,
    #[error("GROUP BY clause cannot contain aggregates")]
    AggInGroupBy,
    #[error("window function calls cannot be nested")]
    NestedWindow,
    #[error("WHERE clause cannot contain window functions")]
    WindowInWhere,
    #[error("HAVING clause cannot contain window functions")]
    WindowInHaving,
    #[error("column {0:?} must appear in the GROUP BY clause or be used in an aggregate function")]
    ColumnNotInAgg(String),
    #[error("ORDER BY items must appear in the select list if DISTINCT is specified")]
    OrderKeyNotInDistinct,
    #[error("{0:?} is not an aggregate function")]
    NotAgg(String),
    #[error("unsupported object name: {0:?}")]
    UnsupportedObjectName(ObjectType),
    #[error("not supported yet: {0}")]
    Todo(String),
    #[error("can not copy to {0}")]
    CopyTo(String),
    #[error("can only insert into table")]
    CanNotInsert,
    #[error("can only delete from table")]
    CanNotDelete,
    #[error("VIEW aliases mismatch query result")]
    ViewAliasesMismatch,
    #[error("pragma does not exist: {0}")]
    NoPragma(String),
}

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
            return Err(BindError::InvalidExpression(
                "the query for sql udf should contain only one statement".to_string(),
            ));
        }

        // Extract the expression out
        let Statement::Query(query) = ast[0].clone() else {
            return Err(BindError::InvalidExpression(
                "invalid function definition, please recheck the syntax".to_string(),
            ));
        };

        let SetExpr::Select(select) = *query.body else {
            return Err(BindError::InvalidExpression(
                "missing `select` body for sql udf expression, please recheck the syntax"
                    .to_string(),
            ));
        };

        if select.projection.len() != 1 {
            return Err(BindError::InvalidExpression(
                "`projection` should contain only one `SelectItem`".to_string(),
            ));
        }

        let SelectItem::UnnamedExpr(expr) = select.projection[0].clone() else {
            return Err(BindError::InvalidExpression(
                "expect `UnnamedExpr` for `projection`".to_string(),
            ));
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
                            return Err(BindError::InvalidExpression("invalid syntax".to_string()));
                        };
                        if catalog.arg_names[i].is_empty() {
                            ret.insert(format!("${}", i + 1), e.clone());
                        } else {
                            // The index mapping here is accurate
                            // So that we could directly use the index
                            ret.insert(catalog.arg_names[i].clone(), e.clone());
                        }
                    }
                    _ => return Err(BindError::InvalidExpression("invalid syntax".to_string())),
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
    /// cte_name -> (query_id, column_alias -> id)
    ctes: HashMap<String, (Id, HashMap<String, Id>)>,
    /// Table aliases that can be accessed from the current query.
    table_aliases: HashSet<String>,
    /// Column aliases that can be accessed from the current query.
    /// column_alias -> (table_alias -> id)
    column_aliases: HashMap<String, HashMap<String, Id>>,
    /// Column aliases that can be accessed from the outside query.
    /// column_alias -> id
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
            Statement::CreateTable {
                name,
                columns,
                constraints,
                ..
            } => self.bind_create_table(name, &columns, &constraints),
            Statement::CreateView {
                name,
                columns,
                query,
                ..
            } => self.bind_create_view(name, columns, *query),
            Statement::CreateFunction {
                name,
                args,
                return_type,
                params,
                ..
            } => self.bind_create_function(name, args, return_type, params),
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                ..
            } => self.bind_drop(object_type, if_exists, names, cascade),
            Statement::Insert {
                table_name,
                columns,
                source: Some(source),
                ..
            } => self.bind_insert(table_name, columns, source),
            Statement::Delete {
                from, selection, ..
            } => self.bind_delete(from, selection),
            Statement::Copy {
                source,
                to,
                target,
                options,
                ..
            } => self.bind_copy(source, to, target, &options),
            Statement::Query(query) => self.bind_query(*query).map(|(id, _)| id),
            Statement::Explain { statement, .. } => self.bind_explain(*statement),
            Statement::ShowVariable { .. }
            | Statement::ShowCreate { .. }
            | Statement::ShowColumns { .. } => Err(BindError::NotSupportedTSQL),
            _ => Err(BindError::InvalidSQL),
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
            return Err(BindError::DuplicatedAlias(table_name.into()));
        }
        Ok(())
    }

    /// Add an alias so that it can be accessed from the outside query.
    fn add_output_alias(&mut self, column_name: String, id: Id) {
        let context = self.contexts.last_mut().unwrap();
        context.output_aliases.insert(column_name, id);
    }

    /// Add a CTE to the current context.
    fn add_cte(&mut self, table_name: &str, query: Id, columns: HashMap<String, Id>) -> Result<()> {
        let context = self.contexts.last_mut().unwrap();
        if context
            .ctes
            .insert(table_name.into(), (query, columns))
            .is_some()
        {
            return Err(BindError::DuplicatedCteName(table_name.into()));
        }
        Ok(())
    }

    /// Find an alias.
    fn find_alias(&self, column_name: &str, table_name: Option<&str>) -> Result {
        for context in self.contexts.iter().rev() {
            if let Some(map) = context.column_aliases.get(column_name) {
                if let Some(table_name) = table_name {
                    if let Some(id) = map.get(table_name) {
                        return Ok(*id);
                    }
                } else if map.len() == 1 {
                    return Ok(*map.values().next().unwrap());
                } else {
                    let use_ = map
                        .keys()
                        .map(|table_name| format!("\"{table_name}.{column_name}\""))
                        .join(" or ");
                    return Err(BindError::AmbiguousColumn(column_name.into(), use_));
                }
            }
        }
        Err(BindError::InvalidColumn(column_name.into()))
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

    fn bind_explain(&mut self, query: Statement) -> Result {
        let id = self.bind_stmt(query)?;
        let id = self.egraph.add(Node::Explain(id));
        Ok(id)
    }
}

/// Split an object name into `(schema name, table name)`.
fn split_name(name: &ObjectName) -> Result<(&str, &str)> {
    Ok(match name.0.as_slice() {
        [table] => (RootCatalog::DEFAULT_SCHEMA_NAME, &table.value),
        [schema, table] => (&schema.value, &table.value),
        _ => return Err(BindError::InvalidTableName(name.0.clone())),
    })
}

/// Convert an object name into lower case
fn lower_case_name(name: &ObjectName) -> ObjectName {
    ObjectName(
        name.0
            .iter()
            .map(|ident| Ident::new(ident.value.to_lowercase()))
            .collect::<Vec<_>>(),
    )
}
