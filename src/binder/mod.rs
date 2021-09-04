use crate::catalog::{RootCatalogRef, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use crate::parser::{CreateTableStmt, InsertStmt, SQLStatement, SQLStatementEnum};
use std::collections::HashSet;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum BindError {
    #[error("invalid statment type ")]
    InvalidStmt,

    #[error("invalid database {0}")]
    InvalidDatabase(String),
    #[error("invalid schema {0}")]
    InvalidSchema(String),
    #[error("duplicated table {0}")]
    DuplicatedTable(String),

    #[error("duplicated column {0}")]
    DuplicatedColumn(String),
}

impl BindError {}

// TODO
struct BinderContext {}

pub(crate) struct Binder {
    catalog: RootCatalogRef,
}

impl Binder {
    pub(crate) fn new(catalog: RootCatalogRef) -> Self {
        Binder { catalog: catalog }
    }

    pub(crate) fn bind(&self, stmt: &mut SQLStatement) -> Result<(), BindError> {
        match &mut stmt.statement {
            SQLStatementEnum::CreateTableStatment(create_stmt) => {
                self.bind_create_table_stmt(create_stmt)
            }
            SQLStatementEnum::InsertStatement(insert_stmt) => self.bind_insert_stmt(insert_stmt),
            _ => Err(BindError::InvalidStmt),
        }
    }

    pub(crate) fn bind_create_table_stmt(
        &self,
        create_table_stmt: &mut CreateTableStmt,
    ) -> Result<(), BindError> {
        if create_table_stmt.database_name.is_none() {
            create_table_stmt.database_name = Some(String::from(DEFAULT_DATABASE_NAME));
            create_table_stmt.database_id = Some(0);
        }

        if create_table_stmt.schema_id.is_none() {
            create_table_stmt.schema_name = Some(String::from(DEFAULT_SCHEMA_NAME));
            create_table_stmt.schema_id = Some(0);
        }

        let root_lock = self.catalog.as_ref().lock().unwrap();

        match root_lock.get_database_by_id(create_table_stmt.database_id.unwrap()) {
            Some(db_arc) => {
                let db = db_arc.as_ref().lock().unwrap();
                match db.get_schema_by_id(create_table_stmt.schema_id.unwrap()) {
                    Some(schema_arc) => {
                        let schema = schema_arc.as_ref().lock().unwrap();
                        match schema.get_table_by_name(&create_table_stmt.table_name) {
                            Some(_) => Err(BindError::DuplicatedTable(
                                create_table_stmt.table_name.clone(),
                            )),
                            None => {
                                let mut set: HashSet<String> = HashSet::new();
                                for col in create_table_stmt.column_descs.iter() {
                                    if set.contains(col.name()) {
                                        return Err(BindError::DuplicatedColumn(
                                            col.name().to_string(),
                                        ));
                                    } else {
                                        set.insert(col.name().to_string());
                                    }
                                }
                                Ok(())
                            }
                        }
                    }
                    None => Err(BindError::InvalidDatabase(
                        create_table_stmt.database_name.as_ref().unwrap().clone(),
                    )),
                }
            }
            None => Err(BindError::InvalidDatabase(
                create_table_stmt.database_name.as_ref().unwrap().clone(),
            )),
        }
    }

    pub(crate) fn bind_insert_stmt(
        &self,
        insert_table_stmt: &mut InsertStmt,
    ) -> Result<(), BindError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDesc, RootCatalog, TableCatalog};
    use crate::parser::*;
    use crate::types::DataType;
    use std::convert::TryFrom;
    use std::sync::{Arc, Mutex};
    #[test]
    fn test_create_table() {
        let catalog = Arc::new(Mutex::new(RootCatalog::new()));
        let binder = Binder::new(catalog.clone());
        let sql = "create table t1 (v1 int not null, v2 int not null); 
                    create table t2 (a int not null, a int not null);
                    create table t3 (v1 int not null);";
        println!("{}", sql);
        let nodes = Parser::parse_sql(sql).unwrap();
        let mut stmt = CreateTableStmt::try_from(&nodes[0]).unwrap();

        binder.bind_create_table_stmt(&mut stmt).unwrap();
        assert_eq!(stmt.database_id, Some(0));
        assert_eq!(stmt.schema_id, Some(0));
        assert_eq!(
            stmt.database_name,
            Some(String::from(DEFAULT_DATABASE_NAME))
        );
        assert_eq!(stmt.schema_name, Some(String::from(DEFAULT_SCHEMA_NAME)));

        let mut stmt2 = CreateTableStmt::try_from(&nodes[1]).unwrap();
        assert_eq!(
            binder.bind_create_table_stmt(&mut stmt2),
            Err(BindError::DuplicatedColumn(String::from("a")))
        );

        let col0 = ColumnDesc::new(DataType::Int32, true, false);
        let col1 = ColumnDesc::new(DataType::Bool, false, false);

        let col_names = vec![String::from("a"), String::from("b")];
        let col_descs = vec![col0, col1];

        catalog
            .as_ref()
            .lock()
            .unwrap()
            .get_database_by_id(0)
            .unwrap()
            .as_ref()
            .lock()
            .unwrap()
            .get_schema_by_id(0)
            .unwrap()
            .as_ref()
            .lock()
            .unwrap()
            .add_table(String::from("t3"), col_names, col_descs, false);

        let mut stmt3 = CreateTableStmt::try_from(&nodes[2]).unwrap();
        assert_eq!(
            binder.bind_create_table_stmt(&mut stmt3),
            Err(BindError::DuplicatedTable(String::from("t3")))
        );
    }
}
