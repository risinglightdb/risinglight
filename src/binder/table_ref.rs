use super::*;
use crate::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use crate::parser::{ExprData, SelectStmt, TableRef};
use crate::types::{ColumnId, DataType};

impl Bind for TableRef {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        match self {
            TableRef::Base(base_ref) => {
                if base_ref.database_name.is_none() {
                    base_ref.database_name = Some(DEFAULT_DATABASE_NAME.to_string());
                }

                if base_ref.schema_name.is_none() {
                    base_ref.schema_name = Some(DEFAULT_SCHEMA_NAME.to_string());
                }
                let table_name: String;
                match &base_ref.alias {
                    Some(name) => {
                        table_name = name.clone();
                    }
                    None => {
                        table_name = base_ref.table_name.clone();
                    }
                }

                if binder.context.regular_tables.contains_key(&table_name) {
                    return Err(BindError::DuplicatedTableName(table_name.clone()));
                }

                let table_ref_id_opt = binder.catalog.get_table_id(
                    base_ref.database_name.as_ref().unwrap(),
                    base_ref.schema_name.as_ref().unwrap(),
                    &table_name,
                );

                match table_ref_id_opt {
                    Some(id) => {
                        binder.context.regular_tables.insert(table_name, id);
                        base_ref.table_ref_id = Some(id);
                        Ok(())
                    }
                    None => Err(BindError::InvalidTable(table_name.clone())),
                }
            }
            _ => todo!(),
        }
    }
}
