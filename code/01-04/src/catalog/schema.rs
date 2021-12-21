use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::*;

/// The catalog of a schema.
pub struct SchemaCatalog {
    id: SchemaId,
    inner: Mutex<Inner>,
}

struct Inner {
    name: String,
    table_idxs: HashMap<String, TableId>,
    tables: HashMap<TableId, Arc<TableCatalog>>,
    next_table_id: TableId,
}

impl SchemaCatalog {
    pub(super) fn new(id: SchemaId, name: String) -> SchemaCatalog {
        SchemaCatalog {
            id,
            inner: Mutex::new(Inner {
                name,
                table_idxs: HashMap::new(),
                tables: HashMap::new(),
                next_table_id: 0,
            }),
        }
    }

    pub fn id(&self) -> SchemaId {
        self.id
    }

    pub fn name(&self) -> String {
        let inner = self.inner.lock().unwrap();
        inner.name.clone()
    }

    pub fn add_table(&self, name: &str) -> Result<TableId, CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.table_idxs.contains_key(name) {
            return Err(CatalogError::Duplicated("table", name.into()));
        }
        let id = inner.next_table_id;
        inner.next_table_id += 1;
        let table_catalog = Arc::new(TableCatalog::new(id, name.into()));
        inner.table_idxs.insert(name.into(), id);
        inner.tables.insert(id, table_catalog);
        Ok(id)
    }

    pub fn del_table_by_name(&self, name: &str) -> Result<(), CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        let id = inner
            .table_idxs
            .remove(name)
            .ok_or_else(|| CatalogError::NotFound("table", name.into()))?;
        inner.tables.remove(&id);
        Ok(())
    }

    pub fn del_table(&self, id: TableId) {
        let mut inner = self.inner.lock().unwrap();
        let catalog = inner.tables.remove(&id).unwrap();
        inner.table_idxs.remove(&catalog.name()).unwrap();
    }

    pub fn all_tables(&self) -> HashMap<TableId, Arc<TableCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner.tables.clone()
    }

    pub fn get_table(&self, table_id: TableId) -> Option<Arc<TableCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner.tables.get(&table_id).cloned()
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<Arc<TableCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner
            .table_idxs
            .get(name)
            .and_then(|id| inner.tables.get(id))
            .cloned()
    }
}
