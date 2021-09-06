use super::*;
use crate::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use crate::parser::{ExprData, Expression, SelectStmt, TableRef};
use crate::types::{ColumnId, DataType};

impl Bind for SelectStmt {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        // Bind table ref
        binder.push_context();
        if self.from_table.is_some() {
            self.from_table.as_mut().unwrap().bind(binder)?;
        }
        // TODO: process where, order by, group-by, limit and offset

        // Bind select list, we only support column reference now
        for select_elem in self.select_list.iter_mut() {
            select_elem.bind(binder)?;
        }

        // Add referred columns for base table reference
        if self.from_table.is_some() {
            match self.from_table.as_mut().unwrap() {
                TableRef::Base(base_ref) => {
                    base_ref.column_ids = binder
                        .context
                        .column_ids
                        .get(&base_ref.table_name)
                        .unwrap()
                        .to_vec();
                    //  assert_eq!(binder.context.column_ids.get(&base_ref.table_name).unwrap().len(), 2);
                }
                _ => {}
            }
        }
        binder.pop_context();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDesc, RootCatalog};
    use crate::parser::{BaseTableRef, SQLStatement};
    use crate::types::{DataType, DataTypeKind};
    use std::sync::Arc;

    #[test]
    fn bind_select() {
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

        let sql = "select a, b from t;  select c from t;";
        let mut stmts = SQLStatement::parse(sql).unwrap();
        stmts[0].bind(&mut binder).unwrap();
        let select_stmt = stmts[0].as_select_stmt();
        let table_ref = select_stmt.from_table.as_mut().unwrap().as_base_ref();
        assert_eq!(
            table_ref.database_name.as_ref().unwrap(),
            DEFAULT_DATABASE_NAME
        );
        assert_eq!(table_ref.schema_name.as_ref().unwrap(), DEFAULT_SCHEMA_NAME);
        assert_eq!(
            table_ref.table_ref_id.unwrap(),
            TableRefId {
                database_id: 0,
                schema_id: 0,
                table_id: 0
            }
        );
        assert_eq!(table_ref.column_ids, vec![0, 1]);

        assert_eq!(
            stmts[1].bind(&mut binder),
            Err(BindError::InvalidColumn("c".to_string()))
        );
    }

    impl SQLStatement {
        fn as_select_stmt(&mut self) -> &mut SelectStmt {
            match self {
                SQLStatement::Select(stmt) => stmt,
                _ => panic!("wrong statement type"),
            }
        }
    }

    impl TableRef {
        fn as_base_ref(&mut self) -> &mut BaseTableRef {
            match self {
                TableRef::Base(base_ref) => base_ref,
                _ => panic!("wrong statement type"),
            }
        }
    }
}
