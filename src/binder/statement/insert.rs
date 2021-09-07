use super::*;
use crate::parser::{ExprKind, InsertStmt};
use crate::types::{ColumnId, DataType};

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
                &self.database_name.as_ref().unwrap(),
                &self.schema_name.as_ref().unwrap(),
                &table.name(),
            )
            .unwrap();

        self.table_ref_id = Some(table_ref_id);

        assert!(self.column_ids.is_empty(), "already bind");
        assert!(self.column_types.is_empty(), "already bind");
        // If the query does not provide column information, get all columns info.
        if self.column_names.is_empty() {
            assert!(self.values.len() > 0);
            let return_size = self.values[0].len();
            let columns = table.all_columns();
            if return_size != columns.len() {
                return Err(BindError::InvalidExpression);
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

                let expr = match &expr.kind {
                    ExprKind::Constant(v) => v,
                    _ => return Err(BindError::InvalidExpression),
                };
                let data_type = expr.data_type();
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
                        if !self.column_types[idx].is_nullable() {
                            return Err(BindError::InvalidExpression);
                        }
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
    use crate::catalog::{ColumnDesc, RootCatalog};
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
                vec!["a".into(), "b".into()],
                vec![
                    ColumnDesc::new(DataType::new(DataTypeKind::Int32, false), false),
                    ColumnDesc::new(DataType::new(DataTypeKind::Int32, false), false),
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
        assert_eq!(
            stmts[1].bind(&mut binder),
            Err(BindError::NotNullableColumn)
        );
        assert_eq!(
            stmts[2].bind(&mut binder),
            Err(BindError::InvalidExpression)
        );
    }
}
