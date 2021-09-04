use super::*;
use crate::parser::{ExprData, Expression, InsertStmt};
use crate::types::{DataType, ColumnId, DataTypeEnum};
impl Bind for InsertStmt {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        let database_name = self
            .database_name
            .get_or_insert_with(|| DEFAULT_DATABASE_NAME.into());

        let schema_name = self
            .schema_name
            .get_or_insert_with(|| DEFAULT_SCHEMA_NAME.into());

        let root_lock = binder.catalog.as_ref().lock().unwrap();

        let db_arc = root_lock
            .get_database_by_name(database_name)
            .ok_or_else(|| BindError::InvalidDatabase(database_name.clone()))?;
        let db = db_arc.as_ref().lock().unwrap();
        let schema_arc = db
            .get_schema_by_name(schema_name)
            .ok_or_else(|| BindError::InvalidDatabase(database_name.clone()))?;
        let schema = schema_arc.as_ref().lock().unwrap();
        let table_arc = schema
            .get_table_by_name(&self.table_name)
            .ok_or_else(|| BindError::InvalidTable(self.table_name.clone()))?;

        let table = table_arc.as_ref().lock().unwrap();
        // If the query does not provide column information, get all columns info.
        if self.column_names.len() == 0 {
            for (id, col) in table.all_columns().iter() {
                self.column_names.push(col.name().to_string());
                self.column_ids.push(*id);
                self.column_types.push(col.datatype());
                self.column_nullables.push(col.is_nullable());
            }
        } else {
            // Otherwise, we get columns info from the query

            let mut column_ids: Vec<ColumnId> = Vec::new();
            let mut column_types: Vec<DataType> = Vec::new();
            let mut column_nullables: Vec<bool> = Vec::new();

            for col_name in self.column_names.iter() {
                let col = table
                    .get_column_by_name(&col_name)
                    .ok_or_else(|| BindError::InvalidColumn(col_name.clone()))?;

                column_ids.push(col.id());
                column_types.push(col.datatype());
                column_nullables.push(col.is_nullable());
            }

            self.column_ids = column_ids;
            self.column_types = column_types;
            self.column_nullables = column_nullables;
        }
        // Check inserted values, we only support inserting values now.
        for vals in self.values.iter() {
            for (idx, val) in vals.iter().enumerate() {
                match &val.data {
                    ExprData::Constant(const_val) => {}
                    _ => {
                        return Err(BindError::InvalidExpression);
                    }
                }
            }
        }

        Ok(())
    }
}
