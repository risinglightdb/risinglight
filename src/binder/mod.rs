use crate::catalog::{RootCatalogRef};
use crate::parser::{SQLStatement, SQLStatementEnum};

#[derive(thiserror::Error, Debug)]
pub enum BindError {
    #[error("invalid table: {0}")]
    InvalidTable(&'static str),
}


impl BindError {}

pub(crate) struct Binder {
    catalog: RootCatalogRef
}

impl Binder {
    pub(crate) fn new(catalog: RootCatalogRef) -> Self {
        Binder {
            catalog: catalog
        }
    }

    pub(crate) fn bind(stmt: &SQLStatement) -> Result<(), BindError> {
        Ok(())
    }
}