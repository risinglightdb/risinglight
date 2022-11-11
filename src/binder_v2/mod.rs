// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;
use std::vec::Vec;

use egg::{Id, Language};
use itertools::Itertools;

use crate::catalog::{RootCatalog, TableRefId, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use crate::parser::*;
use crate::planner::{Expr as Node, RecExpr, TypeError, TypeSchemaAnalysis};
use crate::types::{DataTypeKind, DataValue};

pub mod copy;
mod create_table;
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
    #[error("invalid database {0}")]
    InvalidDatabase(String),
    #[error("invalid schema {0}")]
    InvalidSchema(String),
    #[error("invalid table {0}")]
    InvalidTable(String),
    #[error("invalid column {0}")]
    InvalidColumn(String),
    #[error("duplicated table {0}")]
    DuplicatedTable(String),
    #[error("duplicated column {0}")]
    DuplicatedColumn(String),
    #[error("duplicated alias {0}")]
    DuplicatedAlias(String),
    #[error("invalid expression: {0}")]
    InvalidExpression(String),
    #[error("not nullable column: {0}")]
    NotNullableColumn(String),
    #[error("ambiguous column: {0}")]
    AmbiguousColumn(String),
    #[error("invalid table name: {0:?}")]
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
    #[error("column {0} must appear in the GROUP BY clause or be used in an aggregate function")]
    ColumnNotInAgg(String),
    #[error("ORDER BY items must appear in the select list if DISTINCT is specified")]
    OrderKeyNotInDistinct,
}

/// The binder resolves all expressions referring to schema objects such as
/// tables or views with their column names and types.
pub struct Binder {
    egraph: egg::EGraph<Node, TypeSchemaAnalysis>,
    catalog: Arc<RootCatalog>,
    contexts: Vec<Context>,
}

/// The context of binder execution.
#[derive(Debug, Default)]
struct Context {
    /// Mapping table name to its ID.
    tables: HashMap<String, TableRefId>,
    /// Mapping alias name to expression.
    aliases: HashMap<String, Id>,
}

impl Binder {
    /// Create a new binder.
    pub fn new(catalog: Arc<RootCatalog>) -> Self {
        Binder {
            catalog: catalog.clone(),
            egraph: egg::EGraph::new(TypeSchemaAnalysis { catalog }),
            contexts: vec![Context::default()],
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
                table_name,
                selection,
                ..
            } => self.bind_delete(table_name, selection),
            Statement::Copy {
                table_name,
                columns,
                to,
                target,
                options,
                ..
            } => self.bind_copy(table_name, &columns, to, target, &options),
            Statement::Query(query) => self.bind_query(*query),
            Statement::Explain { statement, .. } => self.bind_explain(*statement),
            Statement::ShowVariable { .. }
            | Statement::ShowCreate { .. }
            | Statement::ShowColumns { .. } => Err(BindError::NotSupportedTSQL),
            _ => Err(BindError::InvalidSQL),
        }
    }

    fn push_context(&mut self) {
        self.contexts.push(Context::default());
    }

    fn pop_context(&mut self) {
        self.contexts.pop();
    }

    fn current_ctx(&self) -> &Context {
        self.contexts.last().unwrap()
    }

    fn current_ctx_mut(&mut self) -> &mut Context {
        self.contexts.last_mut().unwrap()
    }

    /// Add an alias to the current context.
    fn add_alias(&mut self, alias: Ident, expr: Id) -> Result<()> {
        let context = self.contexts.last_mut().unwrap();
        context.aliases.insert(alias.value, expr);
        // may override the same name
        Ok(())
    }

    fn check_type(&self, id: Id) -> Result<crate::types::DataType> {
        Ok(self.egraph[id].data.type_.clone()?)
    }

    fn schema(&self, id: Id) -> Vec<Id> {
        self.egraph[id].data.schema.clone().expect("no schema")
    }

    fn aggs(&self, id: Id) -> &[Node] {
        &self.egraph[id].data.aggs
    }

    fn node(&self, id: Id) -> &Node {
        &self.egraph[id].nodes[0]
    }

    fn bind_explain(&mut self, query: Statement) -> Result {
        let id = self.bind_stmt(query)?;
        let id = self.egraph.add(Node::Explain(id));
        Ok(id)
    }
}

/// Split an object name into `(database name, schema name, table name)`.
fn split_name(name: &ObjectName) -> Result<(&str, &str, &str)> {
    Ok(match name.0.as_slice() {
        [table] => (DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, &table.value),
        [schema, table] => (DEFAULT_DATABASE_NAME, &schema.value, &table.value),
        [db, schema, table] => (&db.value, &schema.value, &table.value),
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
