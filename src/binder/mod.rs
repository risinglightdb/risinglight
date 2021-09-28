use crate::catalog::{RootCatalog, TableRefId, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use crate::parser::{Ident, ObjectName, Statement};
use crate::types::ColumnId;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    vec::Vec,
};

mod expression;
mod statement;
mod table_ref;

pub use self::expression::*;
pub use self::statement::*;
pub use self::table_ref::*;

#[derive(Debug, PartialEq, Clone)]
pub enum BoundStatement {
    CreateTable(BoundCreateTable),
    Drop(BoundDrop),
    Insert(BoundInsert),
    Select(Box<BoundSelect>),
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum BindError {
    #[error("invalid statment type ")]
    InvalidStmt,
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
    #[error("duplicated table name {0}")]
    DuplicatedTableName(String),
    #[error("duplicated column {0}")]
    DuplicatedColumn(String),
    #[error("invalid expression: {0}")]
    InvalidExpression(String),
    #[error("not nullable column: {0}")]
    NotNullableColumn(String),
    #[error("binary operator types mismatch")]
    BinaryOpTypeMismatch,
    #[error("ambiguous column")]
    AmbiguousColumn,
    #[error("invalid table name: {0:?}")]
    InvalidTableName(Vec<Ident>),
    #[error("invalid SQL")]
    InvalidSQL,
}

// TODO
#[derive(Debug, Default)]
struct BinderContext {
    pub regular_tables: HashMap<String, TableRefId>,
    // Mapping the table name to column names
    pub column_names: HashMap<String, HashSet<String>>,
    // Mapping table name to its column ids
    pub column_ids: HashMap<String, Vec<ColumnId>>,
}

pub struct Binder {
    catalog: Arc<RootCatalog>,
    context: BinderContext,
    upper_contexts: Vec<BinderContext>,
}

impl Binder {
    pub fn new(catalog: Arc<RootCatalog>) -> Self {
        Binder {
            catalog,
            upper_contexts: Vec::new(),
            context: BinderContext::default(),
        }
    }

    pub fn push_context(&mut self) {
        let new_context = std::mem::take(&mut self.context);
        self.upper_contexts.push(new_context);
    }

    pub fn pop_context(&mut self) {
        let old_context = self.upper_contexts.pop();
        self.context = old_context.unwrap();
    }

    pub fn bind(&mut self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        match stmt {
            Statement::CreateTable { .. } => {
                Ok(BoundStatement::CreateTable(self.bind_create_table(stmt)?))
            }
            Statement::Drop { .. } => Ok(BoundStatement::Drop(self.bind_drop(stmt)?)),
            Statement::Insert { .. } => Ok(BoundStatement::Insert(self.bind_insert(stmt)?)),
            Statement::Query(query) => Ok(BoundStatement::Select(self.bind_select(&*query)?)),
            _ => todo!("bind statement"),
        }
    }
}

fn split_name(name: &ObjectName) -> Result<(&str, &str, &str), BindError> {
    Ok(match name.0.as_slice() {
        [table] => (DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, &table.value),
        [schema, table] => (DEFAULT_DATABASE_NAME, &schema.value, &table.value),
        [db, schema, table] => (&db.value, &schema.value, &table.value),
        _ => return Err(BindError::InvalidTableName(name.0.clone())),
    })
}
