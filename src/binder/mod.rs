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
    pub upper_context: Option<Box<BinderContext>>,
    pub regular_tables: HashMap<String, TableRefId>,
    // Mapping the table name to column names
    pub column_names: HashMap<String, HashSet<String>>,
    // Mapping table name to its column ids
    pub column_ids: HashMap<String, Arc<Mutex<Vec<ColumnId>>>>,
}

pub struct Binder {
    catalog: Arc<RootCatalog>,
    context: Box<BinderContext>,
}

impl Binder {
    pub fn new(catalog: Arc<RootCatalog>) -> Self {
        Binder {
            catalog,
            context: Box::new(BinderContext {
                upper_context: None,
                regular_tables: HashMap::new(),
                column_names: HashMap::new(),
                column_ids: HashMap::new(),
            }),
        }
    }
}
