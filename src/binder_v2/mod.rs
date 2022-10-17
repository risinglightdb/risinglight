// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;
use std::vec::Vec;

use egg::Id;
use itertools::Itertools;

use crate::catalog::{RootCatalog, TableRefId, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use crate::parser::*;
use crate::planner::{Expr as Node, ExprAnalysis, RecExpr, TypeError};
use crate::types::{DataTypeKind, DataValue};

mod expr;
mod select;
mod table;

pub use self::expr::*;
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
    #[error("binary operator types mismatch: {0} != {1}")]
    BinaryOpTypeMismatch(String, String),
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
}

/// The binder resolves all expressions referring to schema objects such as
/// tables or views with their column names and types.
pub struct Binder {
    egraph: egg::EGraph<Node, ExprAnalysis>,
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
            egraph: egg::EGraph::default(),
            catalog,
            contexts: vec![],
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
            Statement::CreateTable { .. } => todo!(),
            Statement::Drop { .. } => todo!(),
            Statement::Insert { .. } => todo!(),
            Statement::Delete { .. } => todo!(),
            Statement::Copy { .. } => todo!(),
            Statement::Query(query) => self.bind_query(*query),
            Statement::Explain { .. } => todo!(),
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
fn lower_case_name(name: ObjectName) -> ObjectName {
    ObjectName(
        name.0
            .iter()
            .map(|ident| Ident::new(ident.value.to_lowercase()))
            .collect::<Vec<_>>(),
    )
}
