use super::*;
use crate::parser::{ExprData, SelectStmt, TableRef};
use crate::types::{ColumnId, DataType};
use crate::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

impl Bind for SelectStmt {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        // Bind table ref
        if self.from_table.is_some() {
            self.from_table.as_mut().unwrap().bind(binder)?;
        }
        // TODO: process where, order by, group-by, limit and offset

        // Bind select list
        Ok(())
    }
}

impl Bind for TableRef {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        match self {
            TableRef::Base(base_ref) => {
                if base_ref.database_name.is_none()  {
                    base_ref.database_name = Some(DEFAULT_DATABASE_NAME.to_string());
                }

                if base_ref.schema_name.is_none() {
                    base_ref.schema_name = Some(DEFAULT_SCHEMA_NAME.to_string());
                }

                
            },
            _ => todo!(),
        }
    }
}

