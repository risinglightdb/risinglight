use super::*;
use crate::types::{DataType, DatabaseId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) struct RootCatalog {
    database_idxs: HashMap<String, DatabaseId>,
    databases: HashMap<DatabaseId, DatabaseCatalogRef>,
    next_database_id: DatabaseId,
}

impl RootCatalog {
    pub(crate) fn add_database(&mut self, name: String) -> Result<DatabaseId, CatalogError> {
        if self.database_idxs.contains_key(&name) {
            return Err(CatalogError::Duplicated("database", name));
        }
        let database_id = self.next_database_id;
        self.next_database_id += 1;
        let database_catalog =
            Arc::new(Mutex::new(DatabaseCatalog::new(database_id, name.clone())));
        self.database_idxs.insert(name, database_id);
        self.databases.insert(database_id, database_catalog);
        Ok(database_id)
    }

    pub(crate) fn delete_database(&mut self, name: &str) -> Result<(), CatalogError> {
        let id = self
            .database_idxs
            .remove(name)
            .ok_or_else(|| CatalogError::NotFound("database", name.into()))?;
        self.databases.remove(&id);
        Ok(())
    }

    pub(crate) fn get_all_databases(&self) -> &HashMap<DatabaseId, DatabaseCatalogRef> {
        &self.databases
    }

    pub(crate) fn get_database_id_by_name(&self, name: &str) -> Option<DatabaseId> {
        self.database_idxs.get(name).cloned()
    }

    pub(crate) fn get_database_by_id(&self, database_id: DatabaseId) -> Option<DatabaseCatalogRef> {
        self.databases.get(&database_id).cloned()
    }

    pub(crate) fn get_database_by_name(&self, name: &str) -> Option<DatabaseCatalogRef> {
        match self.get_database_id_by_name(name) {
            Some(v) => self.get_database_by_id(v),
            None => None,
        }
    }

    pub(crate) fn new() -> RootCatalog {
        let mut root_catalog = RootCatalog {
            database_idxs: HashMap::new(),
            databases: HashMap::new(),
            next_database_id: 0,
        };
        root_catalog
            .add_database(DEFAULT_DATABASE_NAME.to_string())
            .unwrap();
        root_catalog
    }
}
