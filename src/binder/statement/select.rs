use super::*;
use crate::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use crate::parser::{ExprData, Expression, SelectStmt, TableRef};
use crate::types::{ColumnId, DataType};

impl Bind for SelectStmt {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        // Bind table ref
        if self.from_table.is_some() {
            self.from_table.as_mut().unwrap().bind(binder)?;
        }
        // TODO: process where, order by, group-by, limit and offset

        // Bind select list, we only support column reference now
        for select_elem in self.select_list.iter_mut() {
            select_elem.bind(binder)?;
        }
        Ok(())
    }
}
