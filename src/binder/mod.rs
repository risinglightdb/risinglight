use crate::catalog::{RootCatalog, TableRefId, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use crate::types::ColumnId;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    vec::Vec,
};

mod expression;
mod statement;
mod table_ref;

trait Bind {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError>;
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
    #[error("invalid expression")]
    InvalidExpression,
    #[error("not nullable column")]
    NotNullableColumn,
    #[error("ambiguous column")]
    AmbiguousColumn,
}

// TODO
struct BinderContext {
    pub regular_tables: HashMap<String, TableRefId>,
    // Mapping the table name to column names
    pub column_names: HashMap<String, HashSet<String>>,
    // Mapping table name to its column ids
    pub column_ids: HashMap<String, Vec<ColumnId>>,
}

impl BinderContext {
    pub fn new() -> BinderContext {
        BinderContext {
            regular_tables: HashMap::new(),
            column_names: HashMap::new(),
            column_ids: HashMap::new(),
        }
    }
}

pub struct Binder {
    catalog: Arc<RootCatalog>,
    context: BinderContext,
    upper_contexts: Vec<BinderContext>,
}

impl Binder {
    pub fn new(catalog: Arc<RootCatalog>) -> Self {
        Binder {
            catalog: catalog,
            upper_contexts: Vec::new(),
            context: BinderContext::new(),
        }
    }

    pub fn push_context(&mut self) {
        let new_context = std::mem::replace(&mut self.context, BinderContext::new());
        self.upper_contexts.push(new_context);
    }

    pub fn pop_context(&mut self) {
        let old_context = self.upper_contexts.pop();
        let used = std::mem::replace(&mut self.context, old_context.unwrap());
    }
}
