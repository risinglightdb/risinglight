use super::*;
use crate::types::{DatabaseId, TableId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct RootCatalog {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    database_idxs: HashMap<String, DatabaseId>,
    databases: HashMap<DatabaseId, Arc<DatabaseCatalog>>,
    next_database_id: DatabaseId,
}

impl RootCatalog {
    pub fn new() -> RootCatalog {
        let mut root_catalog = RootCatalog {
            inner: Mutex::new(Inner::default()),
        };
        root_catalog
            .add_database(DEFAULT_DATABASE_NAME.into())
            .unwrap();
        root_catalog
    }

    pub fn add_database(&self, name: String) -> Result<DatabaseId, CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.database_idxs.contains_key(&name) {
            return Err(CatalogError::Duplicated("database", name));
        }
        let database_id = inner.next_database_id;
        inner.next_database_id += 1;
        let database_catalog = Arc::new(DatabaseCatalog::new(database_id, name.clone()));
        inner.database_idxs.insert(name, database_id);
        inner.databases.insert(database_id, database_catalog);
        Ok(database_id)
    }

    pub fn delete_database(&self, name: &str) -> Result<(), CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        let id = inner
            .database_idxs
            .remove(name)
            .ok_or_else(|| CatalogError::NotFound("database", name.into()))?;
        inner.databases.remove(&id);
        Ok(())
    }

    pub fn all_databases(&self) -> HashMap<DatabaseId, Arc<DatabaseCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner.databases.clone()
    }

    pub fn get_database_id_by_name(&self, name: &str) -> Option<DatabaseId> {
        let inner = self.inner.lock().unwrap();
        inner.database_idxs.get(name).cloned()
    }

    pub fn get_database_by_id(&self, database_id: DatabaseId) -> Option<Arc<DatabaseCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner.databases.get(&database_id).cloned()
    }

    pub fn get_database_by_name(&self, name: &str) -> Option<Arc<DatabaseCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner
            .database_idxs
            .get(name)
            .and_then(|id| inner.databases.get(id))
            .cloned()
    }

    pub fn get_table_id(
        &self,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Option<TableRefId> {
        let db_opt = self.get_database_by_name(database_name);

        if db_opt.is_none() {
            return None;
        }
        let db = db_opt.unwrap();

        let database_id = db.id();

        let schema_opt = db.get_schema_by_name(schema_name);

        if schema_opt.is_none() {
            return None;
        }

        let schema = schema_opt.unwrap();
        let schema_id = schema.id();

        let table_opt = schema.get_table_by_name(table_name);

        if table_opt.is_none() {
            return None;
        }

        let table_id = table_opt.unwrap().as_ref().id();

        Some(TableRefId {
            database_id: database_id,
            schema_id: schema_id,
            table_id: table_id,
        })
    }
}
