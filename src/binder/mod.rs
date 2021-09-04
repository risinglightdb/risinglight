use crate::catalog::{RootCatalogRef, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use std::collections::HashSet;

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
    #[error("duplicated table {0}")]
    DuplicatedTable(String),
    #[error("duplicated column {0}")]
    DuplicatedColumn(String),
}

// TODO
struct BinderContext {}

pub(crate) struct Binder {
    catalog: RootCatalogRef,
    context: BinderContext,
}

impl Binder {
    pub(crate) fn new(catalog: RootCatalogRef) -> Self {
        Binder {
            catalog,
            context: BinderContext {},
        }
    }
}
