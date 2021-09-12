use super::*;
use crate::parser::CreateTableStmt;

impl Bind for CreateTableStmt {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        let database_name = self
            .database_name
            .get_or_insert_with(|| DEFAULT_DATABASE_NAME.into());

        let schema_name = self
            .schema_name
            .get_or_insert_with(|| DEFAULT_SCHEMA_NAME.into());

        let db = binder
            .catalog
            .get_database_by_name(database_name)
            .ok_or_else(|| BindError::InvalidDatabase(database_name.clone()))?;
        self.database_id = Some(db.id());

        let schema = db
            .get_schema_by_name(schema_name)
            .ok_or_else(|| BindError::InvalidSchema(schema_name.clone()))?;
        self.schema_id = Some(schema.id());

        if schema.get_table_by_name(&self.table_name).is_some() {
            return Err(BindError::DuplicatedTable(self.table_name.clone()));
        }
        let mut set = HashSet::new();
        for (idx, col) in self.column_descs.iter_mut().enumerate() {
            if !set.insert(col.name().to_string()) {
                return Err(BindError::DuplicatedColumn(col.name().to_string()));
            }
            col.set_id(idx as ColumnId);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::RootCatalog;
    use crate::parser::SQLStatement;
    use std::sync::Arc;

    #[test]
    fn bind_create_table() {
        let catalog = Arc::new(RootCatalog::new());
        let mut binder = Binder::new(catalog.clone());
        let sql = "
            create table t1 (v1 int not null, v2 int not null); 
            create table t2 (a int not null, a int not null);
            create table t3 (v1 int not null);";
        let mut stmts = SQLStatement::parse(sql).unwrap();

        let stmt = stmts[0].as_create_table();
        stmt.bind(&mut binder).unwrap();
        assert_eq!(stmt.database_id, Some(0));
        assert_eq!(stmt.schema_id, Some(0));
        assert_eq!(stmt.database_name, Some(DEFAULT_DATABASE_NAME.into()));
        assert_eq!(stmt.schema_name, Some(DEFAULT_SCHEMA_NAME.into()));

        let stmt2 = stmts[1].as_create_table();
        assert_eq!(
            stmt2.bind(&mut binder),
            Err(BindError::DuplicatedColumn("a".into()))
        );

        let database = catalog.get_database_by_id(0).unwrap();
        let schema = database.get_schema_by_id(0).unwrap();
        schema.add_table("t3".into(), vec![], false).unwrap();

        let stmt3 = stmts[2].as_create_table();
        assert_eq!(
            stmt3.bind(&mut binder),
            Err(BindError::DuplicatedTable("t3".into()))
        );
    }

    impl SQLStatement {
        fn as_create_table(&mut self) -> &mut CreateTableStmt {
            match self {
                SQLStatement::CreateTable(stmt) => stmt,
                _ => panic!("wrong statement type"),
            }
        }
    }
}
