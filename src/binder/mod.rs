use crate::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, RootCatalogRef};
use crate::parser::{CreateTableStmt, InsertStmt, SQLStatement, SQLStatementEnum};

#[derive(thiserror::Error, Debug)]
pub enum BindError {
    #[error("invalid statment type ")]
    InvalidStmt,
}

impl BindError {}

// TODO
struct BinderContext {}

pub(crate) struct Binder {
    catalog: RootCatalogRef,
}

impl Binder {
    pub(crate) fn new(catalog: RootCatalogRef) -> Self {
        Binder { catalog: catalog }
    }

    pub(crate) fn bind(&self, stmt: &mut SQLStatement) -> Result<(), BindError> {
        match &mut stmt.statement {
            SQLStatementEnum::CreateTableStatment(create_stmt) => {
                self.bind_create_table_stmt(create_stmt)
            }
            SQLStatementEnum::InsertStatement(insert_stmt) => self.bind_insert_stmt(insert_stmt),
            _ => Err(BindError::InvalidStmt),
        }
    }

    pub(crate) fn bind_create_table_stmt(
        &self,
        create_table_stmt: &mut CreateTableStmt,
    ) -> Result<(), BindError> {
        Ok(())
    }

    pub(crate) fn bind_insert_stmt(&self, insert_table_stmt: &mut InsertStmt) -> Result<(), BindError> {
        Ok(())
    }
}

