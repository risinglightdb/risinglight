use crate::catalog::{RootCatalog, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use std::{collections::HashSet, sync::Arc};

mod statement;

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
    #[error("duplicated column {0}")]
    DuplicatedColumn(String),
    #[error("invalid expression")]
    InvalidExpression,
    #[error("not nullable column")]
    NotNullableColumn,
}

// TODO
struct BinderContext {
    pub regular_tables : HashSet<String, TableId>
}

pub(crate) struct Binder {
    catalog: Arc<RootCatalog>,
    context: BinderContext,
}

impl Binder {
    pub(crate) fn new(catalog: Arc<RootCatalog>) -> Self {
        Binder {
            catalog,
            context: BinderContext {},
        }
    }
}
