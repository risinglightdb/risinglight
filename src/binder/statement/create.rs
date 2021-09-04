use super::*;
use crate::parser::CreateTableStmt;

impl Bind for CreateTableStmt {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        let database_name = self
            .database_name
            .get_or_insert_with(|| DEFAULT_DATABASE_NAME.into());
        let database_id = self.database_id.get_or_insert(0);

        let schema_name = self
            .schema_name
            .get_or_insert_with(|| DEFAULT_SCHEMA_NAME.into());
        let schema_id = self.schema_id.get_or_insert(0);

        let root_lock = binder.catalog.as_ref().lock().unwrap();

        let db_arc = root_lock
            .get_database_by_id(*database_id)
            .ok_or_else(|| BindError::InvalidDatabase(database_name.clone()))?;
        let db = db_arc.as_ref().lock().unwrap();
        let schema_arc = db
            .get_schema_by_id(*schema_id)
            .ok_or_else(|| BindError::InvalidDatabase(database_name.clone()))?;
        let schema = schema_arc.as_ref().lock().unwrap();
        if schema.get_table_by_name(&self.table_name).is_some() {
            return Err(BindError::DuplicatedTable(self.table_name.clone()));
        }
        let mut set = HashSet::new();
        for col in self.column_descs.iter() {
            if set.contains(col.name()) {
                return Err(BindError::DuplicatedColumn(col.name().to_string()));
            }
            set.insert(col.name().to_string());
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
        let sql = "create table t1 (v1 int not null, v2 int not null); 
                    create table t2 (a int not null, a int not null);
                    create table t3 (v1 int not null);";
        println!("{}", sql);
        let nodes = parse(sql).unwrap();
        let mut stmt = CreateTableStmt::try_from(&nodes[0]).unwrap();

        stmt.bind(&mut binder).unwrap();
        assert_eq!(stmt.database_id, Some(0));
        assert_eq!(stmt.schema_id, Some(0));
        assert_eq!(stmt.database_name, Some(DEFAULT_DATABASE_NAME.into()));
        assert_eq!(stmt.schema_name, Some(DEFAULT_SCHEMA_NAME.into()));

        let mut stmt2 = CreateTableStmt::try_from(&nodes[1]).unwrap();
        assert_eq!(
            stmt2.bind(&mut binder),
            Err(BindError::DuplicatedColumn("a".into()))
        );

        let col0 = ColumnDesc::new(DataType::new(DataTypeEnum::Int32, false), true);
        let col1 = ColumnDesc::new(DataType::new(DataTypeEnum::Bool, false), false);

        let col_names = vec!["a".into(), "b".into()];
        let col_descs = vec![col0, col1];

        let database = catalog.lock().unwrap().get_database_by_id(0).unwrap();
        let schema = database.lock().unwrap().get_schema_by_id(0).unwrap();
        schema
            .lock()
            .unwrap()
            .add_table("t3".into(), col_names, col_descs, false)
            .unwrap();

        let mut stmt3 = CreateTableStmt::try_from(&nodes[2]).unwrap();
        assert_eq!(
            stmt3.bind(&mut binder),
            Err(BindError::DuplicatedTable("t3".into()))
        );
    }
}
