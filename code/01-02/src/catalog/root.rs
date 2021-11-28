use super::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The root of all catalogs.
pub struct RootCatalog {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    database_idxs: HashMap<String, DatabaseId>,
    databases: HashMap<DatabaseId, Arc<DatabaseCatalog>>,
    next_database_id: DatabaseId,
}

impl Default for RootCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl RootCatalog {
    pub fn new() -> RootCatalog {
        let root_catalog = RootCatalog {
            inner: Mutex::new(Inner::default()),
        };
        root_catalog.add_database(DEFAULT_DATABASE_NAME).unwrap();
        root_catalog
    }

    pub fn add_database(&self, name: &str) -> Result<DatabaseId, CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.database_idxs.contains_key(name) {
            return Err(CatalogError::Duplicated("database", name.into()));
        }
        let id = inner.next_database_id;
        inner.next_database_id += 1;
        let database_catalog = Arc::new(DatabaseCatalog::new(id, name.into()));
        inner.database_idxs.insert(name.into(), id);
        inner.databases.insert(id, database_catalog);
        Ok(id)
    }

    pub fn del_database(&self, name: &str) -> Result<(), CatalogError> {
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

    pub fn get_database(&self, database_id: DatabaseId) -> Option<Arc<DatabaseCatalog>> {
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

    pub fn get_table(&self, table_ref_id: TableRefId) -> Option<Arc<TableCatalog>> {
        let db = self.get_database(table_ref_id.database_id)?;
        let schema = db.get_schema(table_ref_id.schema_id)?;
        schema.get_table(table_ref_id.table_id)
    }

    pub fn get_table_id_by_name(
        &self,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Option<TableRefId> {
        let db = self.get_database_by_name(database_name)?;
        let schema = db.get_schema_by_name(schema_name)?;
        let table = schema.get_table_by_name(table_name)?;

        Some(TableRefId {
            database_id: db.id(),
            schema_id: schema.id(),
            table_id: table.id(),
        })
    }
}
