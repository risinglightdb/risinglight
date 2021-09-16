use super::*;
use crate::parser::InsertStmt;
use crate::types::ColumnId;

impl Bind for InsertStmt {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        let database_name = self
            .database_name
            .get_or_insert_with(|| DEFAULT_DATABASE_NAME.into());

        let schema_name = self
            .schema_name
            .get_or_insert_with(|| DEFAULT_SCHEMA_NAME.into());

        let table = binder
            .catalog
            .get_database_by_name(database_name)
            .ok_or_else(|| BindError::InvalidDatabase(database_name.clone()))?
            .get_schema_by_name(schema_name)
            .ok_or_else(|| BindError::InvalidSchema(schema_name.clone()))?
            .get_table_by_name(&self.table_name)
            .ok_or_else(|| BindError::InvalidTable(self.table_name.clone()))?;

        let table_ref_id = binder
            .catalog
            .get_table_id(
                self.database_name.as_ref().unwrap(),
                self.schema_name.as_ref().unwrap(),
                &table.name(),
            )
            .unwrap();

        self.table_ref_id = Some(table_ref_id);

        assert!(self.column_ids.is_empty(), "already bind");
        assert!(self.column_types.is_empty(), "already bind");
        // If the query does not provide column information, get all columns info.
        if self.column_names.is_empty() {
            assert!(!self.values.is_empty());
            let return_size = self.values[0].len();
            let columns = table.all_columns();
            if return_size != columns.len() {
                return Err(BindError::InvalidExpression(format!(
                    "Column length mismatched. Expected: {}, Actual: {}",
                    columns.len(),
                    return_size
                )));
            }
            for (id, col) in columns.iter() {
                self.column_names.push(col.name().to_string());
                self.column_ids.push(*id);
                self.column_types.push(col.datatype());
            }
        } else {
            // Otherwise, we get columns info from the query.
            for col_name in self.column_names.iter() {
                let col = table
                    .get_column_by_name(col_name)
                    .ok_or_else(|| BindError::InvalidColumn(col_name.clone()))?;

                self.column_ids.push(col.id());
                self.column_types.push(col.datatype());
            }
        }
        // TODO: Handle 'insert into .. select .. from ..' case.

        // Handle 'insert into .. values ..' case.
        // Check inserted values, we only support inserting values now.
        for exprs in self.values.iter_mut() {
            for (idx, expr) in exprs.iter_mut().enumerate() {
                // Bind expression
                expr.bind(binder)?;

                let data_type = expr.return_type.unwrap();
                // TODO: support valid type cast
                // table t1(a float, b float)
                // for example: insert into values (1, 1);
                // 1 should be casted to float.
                if data_type.kind() != self.column_types[idx].kind() && !data_type.kind().is_null()
                {
                    todo!("type cast");
                }
                // If the data value is null, the column must be nullable.
                if data_type.kind().is_null() && !self.column_types[idx].is_nullable() {
                    return Err(BindError::InvalidExpression(
                        "Can not insert null to non null column".into(),
                    ));
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
                return Err(BindError::NotNullableColumn(col.name().into()));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, RootCatalog};
    use crate::parser::SQLStatement;
    use crate::types::{DataType, DataTypeKind};
    use std::sync::Arc;

    #[test]
    fn bind_insert() {
        let catalog = Arc::new(RootCatalog::new());
        let mut binder = Binder::new(catalog.clone());

        let database = catalog.get_database_by_id(0).unwrap();
        let schema = database.get_schema_by_id(0).unwrap();
        schema
            .add_table(
                "t".into(),
                vec![
                    ColumnCatalog::new(
                        0,
                        "a".into(),
                        ColumnDesc::new(DataType::new(DataTypeKind::Int32, false), false),
                    ),
                    ColumnCatalog::new(
                        1,
                        "b".into(),
                        ColumnDesc::new(DataType::new(DataTypeKind::Int32, false), false),
                    ),
                ],
                false,
            )
            .unwrap();

        let sql = "
            insert into t values (1, 1);
            insert into t (a) values (1); 
            insert into t values (1);";
        let mut stmts = SQLStatement::parse(sql).unwrap();

        stmts[0].bind(&mut binder).unwrap();
        assert!(matches!(
            stmts[1].bind(&mut binder),
            Err(BindError::NotNullableColumn(_))
        ));
        assert!(matches!(
            stmts[2].bind(&mut binder),
            Err(BindError::InvalidExpression(_))
        ));
    }
}
