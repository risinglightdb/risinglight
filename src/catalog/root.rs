use crate::catalog::{
    DatabaseCatalog, DatabaseCatalogRef, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
};
use crate::types::{DataType, DatabaseId};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

pub(crate) struct RootCatalog {
    database_idxs: HashMap<String, DatabaseId>,
    databases: BTreeMap<DatabaseId, DatabaseCatalogRef>,
    next_database_id: DatabaseId,
}

impl RootCatalog {
    pub(crate) fn add_database(&mut self, database_name: String) -> Result<DatabaseId, String> {
        if self.database_idxs.contains_key(&database_name) {
            Err(String::from("Duplicated database name!"))
        } else {
            let database_id = self.next_database_id;
            self.next_database_id += 1;
            let database_catalog = Arc::new(Mutex::new(DatabaseCatalog::new(
                database_id,
                database_name.clone(),
            )));
            database_catalog
                .as_ref()
                .lock()
                .unwrap()
                .add_schema(DEFAULT_SCHEMA_NAME.to_string());
            self.database_idxs.insert(database_name, database_id);
            self.databases.insert(database_id, database_catalog);
            Ok(database_id)
        }
    }

    pub(crate) fn delete_database(&mut self, database_name: &String) -> Result<(), String> {
        if self.database_idxs.contains_key(database_name) {
            let id = self.database_idxs.remove(database_name).unwrap();
            self.databases.remove(&id);
            Ok(())
        } else {
            Err(String::from("Database does not exist: ") + database_name)
        }
    }

    pub(crate) fn get_all_databases(&self) -> &BTreeMap<DatabaseId, DatabaseCatalogRef> {
        &self.databases
    }

    pub(crate) fn get_database_id_by_name(&self, name: &String) -> Option<DatabaseId> {
        self.database_idxs.get(name).cloned()
    }

    pub(crate) fn get_database_by_id(&self, database_id: DatabaseId) -> Option<DatabaseCatalogRef> {
        self.databases.get(&database_id).cloned()
    }

    pub(crate) fn get_database_by_name(&self, name: &String) -> Option<DatabaseCatalogRef> {
        match self.get_database_id_by_name(name) {
            Some(v) => self.get_database_by_id(v),
            None => None,
        }
    }

    pub(crate) fn new() -> RootCatalog {
        let mut root_catalog = RootCatalog {
            database_idxs: HashMap::new(),
            databases: BTreeMap::new(),
            next_database_id: 0,
        };
        root_catalog.add_database(DEFAULT_DATABASE_NAME.to_string());
        root_catalog
    }
}
