use super::*;
use crate::parser::{ExprData, InsertStmt};
use crate::types::{ColumnId, DataType};
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
        if self.column_names.is_empty() {
            assert_eq!(self.values.len() > 0, true);
            let return_size = self.values[0].len();
            if return_size != table.all_columns().len() {
                return Err(BindError::InvalidExpression);
            }
            for (id, col) in table.all_columns().iter() {
                self.column_names.push(col.name().to_string());
                self.column_ids.push(*id);
                self.column_types.push(col.datatype());
                self.column_nullables.push(col.is_nullable());
            }
        } else {
            // Otherwise, we get columns info from the query.

            let mut column_ids: Vec<ColumnId> = Vec::new();
            let mut column_types: Vec<DataType> = Vec::new();
            let mut column_nullables: Vec<bool> = Vec::new();

            for col_name in self.column_names.iter() {
                let col = table
                    .get_column_by_name(col_name)
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
                    ExprData::Constant(const_val) => {
                        let data_type = const_val.data_type();
                        match data_type {
                            Some(t) => {
                                // TODO: support valid type cast
                                // table t1(a float, b float)
                                // for example: insert into values (1, 1);
                                // 1 should be casted to float.
                                if t != self.column_types[idx] {
                                    return Err(BindError::InvalidExpression);
                                }
                            }
                            None => {
                                // If the data value is null, the column must be nullable.
                                if !self.column_nullables[idx] {
                                    return Err(BindError::InvalidExpression);
                                }
                            }
                        };
                    }
                    _ => {
                        return Err(BindError::InvalidExpression);
                    }
                }
            }
        }

        // Check columns after transforming.

        let mut col_set: HashSet<ColumnId> = HashSet::new();
        for id in self.column_ids.iter() {
            assert_eq!(col_set.contains(id), false);
            col_set.insert(*id);
        }

        for (id, col) in table.all_columns().iter() {
            if !col_set.contains(id) && !col.is_nullable() {
                return Err(BindError::NotNullableColumn);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDesc, RootCatalog, TableCatalog};
    use crate::parser::*;
    use crate::types::{DataType, DataTypeEnum};
    use std::convert::TryFrom;
    use std::sync::{Arc, Mutex};

    #[test]
    fn bind_create_table() {
        let catalog = Arc::new(Mutex::new(RootCatalog::new()));
        let mut binder = Binder::new(catalog.clone());

        let col0 = ColumnDesc::new(DataType::new(DataTypeEnum::Int32, false), false);
        let col1 = ColumnDesc::new(DataType::new(DataTypeEnum::Int32, false), false);

        let col_names = vec!["a".into(), "b".into()];
        let col_descs = vec![col0, col1];

        let database = catalog.lock().unwrap().get_database_by_id(0).unwrap();
        let schema = database.lock().unwrap().get_schema_by_id(0).unwrap();
        schema
            .lock()
            .unwrap()
            .add_table("t".into(), col_names, col_descs, false)
            .unwrap();

        let sql =
            "insert into t values (1, 1); insert into t (a) values (1); insert into t values (1);";
        let nodes = parse(sql).unwrap();
        let mut stmt0 = InsertStmt::try_from(&nodes[0]).unwrap();

        stmt0.bind(&mut binder).unwrap();

        let mut stmt1 = InsertStmt::try_from(&nodes[1]).unwrap();

        assert_eq!(stmt1.bind(&mut binder), Err(BindError::NotNullableColumn));

        let mut stmt2 = InsertStmt::try_from(&nodes[2]).unwrap();

        assert_eq!(stmt2.bind(&mut binder), Err(BindError::InvalidExpression));
    }
}
