// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::vec::Vec;

use egg::{Id, Language};
use itertools::Itertools;

use crate::catalog::{RootCatalog, TableRefId};
use crate::parser::*;
use crate::planner::{Expr as Node, RecExpr, TypeError, TypeSchemaAnalysis};
use crate::types::{DataTypeKind, DataValue};

pub mod copy;
mod create_table;
mod create_view;
mod delete;
mod drop;
mod expr;
mod insert;
mod select;
mod table;

pub use self::create_table::*;
pub use self::delete::*;
pub use self::drop::*;
pub use self::expr::*;
pub use self::insert::*;
pub use self::select::*;
pub use self::table::*;

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
    CastError(DataValue, DataTypeKind),
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
    #[error("not supported yet: {0}")]
    Todo(String),
    #[error("can not copy to {0}")]
    CopyTo(String),
    #[error("can only insert into table")]
    CanNotInsert,
    #[error("can only delete from table")]
    CanNotDelete,
}

/// The binder resolves all expressions referring to schema objects such as
/// tables or views with their column names and types.
pub struct Binder {
    egraph: egg::EGraph<Node, TypeSchemaAnalysis>,
    catalog: Arc<RootCatalog>,
    contexts: Vec<Context>,
    /// The number of occurrences of each table in the query.
    table_occurrences: HashMap<TableRefId, u32>,
}

/// The context of binder execution.
#[derive(Debug, Default)]
struct Context {
    /// Table names that can be accessed from the current query.
    table_aliases: HashSet<String>,
    /// Column names that can be accessed from the current query.
    /// column_name -> (table_name -> id)
    aliases: HashMap<String, HashMap<String, Id>>,
    /// Column names that can be accessed from the outside query.
    /// column_name -> id
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
                source,
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

    /// Add an alias to the current context.
    fn add_alias(&mut self, column_name: String, table_name: String, id: Id) {
        let context = self.contexts.last_mut().unwrap();
        context
            .aliases
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

    /// Find an alias.
    fn find_alias(&self, column_name: &str, table_name: Option<&str>) -> Result {
        for context in self.contexts.iter().rev() {
            if let Some(map) = context.aliases.get(column_name) {
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
