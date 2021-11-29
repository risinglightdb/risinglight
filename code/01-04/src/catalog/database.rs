use super::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The catalog of a database.
pub struct DatabaseCatalog {
    id: DatabaseId,
    inner: Mutex<Inner>,
}

struct Inner {
    name: String,
    schema_idxs: HashMap<String, SchemaId>,
    schemas: HashMap<SchemaId, Arc<SchemaCatalog>>,
    next_schema_id: SchemaId,
}

impl DatabaseCatalog {
    pub(super) fn new(id: DatabaseId, name: String) -> Self {
        let db_catalog = DatabaseCatalog {
            id,
            inner: Mutex::new(Inner {
                name,
                schema_idxs: HashMap::new(),
                schemas: HashMap::new(),
                next_schema_id: 0,
            }),
        };
        db_catalog.add_schema(DEFAULT_SCHEMA_NAME).unwrap();
        db_catalog
    }

    pub fn id(&self) -> DatabaseId {
        self.id
    }

    pub fn name(&self) -> String {
        let inner = self.inner.lock().unwrap();
        inner.name.clone()
    }

    pub fn add_schema(&self, name: &str) -> Result<SchemaId, CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.schema_idxs.contains_key(name) {
            return Err(CatalogError::Duplicated("schema", name.into()));
        }
        let id = inner.next_schema_id;
        inner.next_schema_id += 1;
        let schema_catalog = Arc::new(SchemaCatalog::new(id, name.into()));
        inner.schema_idxs.insert(name.into(), id);
        inner.schemas.insert(id, schema_catalog);
        Ok(id)
    }

    pub fn del_schema(&self, name: &str) -> Result<(), CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        let id = inner
            .schema_idxs
            .remove(name)
            .ok_or_else(|| CatalogError::NotFound("schema", name.into()))?;
        inner.schemas.remove(&id);
        Ok(())
    }

    pub fn all_schemas(&self) -> HashMap<SchemaId, Arc<SchemaCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner.schemas.clone()
    }

    pub fn get_schema(&self, schema_id: SchemaId) -> Option<Arc<SchemaCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner.schemas.get(&schema_id).cloned()
    }

    pub fn get_schema_by_name(&self, name: &str) -> Option<Arc<SchemaCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner
            .schema_idxs
            .get(name)
            .and_then(|id| inner.schemas.get(id))
            .cloned()
    }
}
