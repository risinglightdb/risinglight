use super::*;
use crate::types::{DataType, DatabaseId, SchemaId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) struct DatabaseCatalog {
    database_id: DatabaseId,
    database_name: String,
    schema_idxs: HashMap<String, SchemaId>,
    schemas: HashMap<SchemaId, SchemaCatalogRef>,
    next_schema_id: SchemaId,
}

impl DatabaseCatalog {
    pub(crate) fn add_schema(&mut self, name: String) -> Result<SchemaId, CatalogError> {
        if self.schema_idxs.contains_key(&name) {
            return Err(CatalogError::Duplicated("schema", name));
        }
        let schema_id = self.next_schema_id;
        self.next_schema_id += 1;
        let schema_catalog = Arc::new(Mutex::new(SchemaCatalog::new(schema_id, name.clone())));
        self.schema_idxs.insert(name, schema_id);
        self.schemas.insert(schema_id, schema_catalog);
        Ok(schema_id)
    }

    pub(crate) fn delete_schema(&mut self, name: &str) -> Result<(), CatalogError> {
        let id = self
            .schema_idxs
            .remove(name)
            .ok_or_else(|| CatalogError::NotFound("schema", name.into()))?;
        self.schemas.remove(&id);
        Ok(())
    }

    pub(crate) fn get_all_schemas(&self) -> &HashMap<SchemaId, SchemaCatalogRef> {
        &self.schemas
    }

    pub(crate) fn get_schema_id_by_name(&self, name: &str) -> Option<SchemaId> {
        self.schema_idxs.get(name).cloned()
    }

    pub(crate) fn get_schema_by_id(&self, schema_id: SchemaId) -> Option<SchemaCatalogRef> {
        self.schemas.get(&schema_id).cloned()
    }

    pub(crate) fn get_schema_by_name(&self, name: &str) -> Option<SchemaCatalogRef> {
        match self.get_schema_id_by_name(name) {
            Some(v) => self.get_schema_by_id(v),
            None => None,
        }
    }

    pub(crate) fn get_database_name(&self) -> String {
        self.database_name.clone()
    }

    pub(crate) fn get_database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub(crate) fn new(database_id: DatabaseId, database_name: String) -> DatabaseCatalog {
        let mut db_catalog = DatabaseCatalog {
            database_id: database_id,
            database_name: database_name,
            schema_idxs: HashMap::new(),
            schemas: HashMap::new(),
            next_schema_id: 0,
        };
        db_catalog
            .add_schema(DEFAULT_SCHEMA_NAME.to_string())
            .unwrap();
        db_catalog
    }
}
