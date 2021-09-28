use super::{CatalogError, ColumnCatalog, TableCatalog};
use crate::types::{SchemaId, TableId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
    pub fn new(id: SchemaId, name: String) -> SchemaCatalog {
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

    pub fn add_table(
        &self,
        name: String,
        columns: Vec<ColumnCatalog>,
        is_materialized_view: bool,
    ) -> Result<TableId, CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.table_idxs.contains_key(&name) {
            return Err(CatalogError::Duplicated("table", name));
        }
        let table_id = inner.next_table_id;
        inner.next_table_id += 1;
        let table_catalog = Arc::new(TableCatalog::new(
            table_id,
            name.clone(),
            columns,
            is_materialized_view,
        ));
        inner.table_idxs.insert(name, table_id);
        inner.tables.insert(table_id, table_catalog);
        Ok(table_id)
    }

    pub fn delete_table_by_name(&self, name: &str) -> Result<(), CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        let id = inner
            .table_idxs
            .remove(name)
            .ok_or_else(|| CatalogError::NotFound("table", name.into()))?;
        inner.tables.remove(&id);
        Ok(())
    }

    pub fn delete_table(&self, id: TableId) {
        let mut inner = self.inner.lock().unwrap();
        inner.tables.remove(&id);
    }

    pub fn all_tables(&self) -> HashMap<TableId, Arc<TableCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner.tables.clone()
    }

    pub fn get_table_id_by_name(&self, name: &str) -> Option<TableId> {
        let inner = self.inner.lock().unwrap();
        inner.table_idxs.get(name).cloned()
    }

    pub fn get_table_by_id(&self, table_id: TableId) -> Option<Arc<TableCatalog>> {
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

    pub fn name(&self) -> String {
        let inner = self.inner.lock().unwrap();
        inner.name.clone()
    }

    pub fn id(&self) -> SchemaId {
        self.id
    }
}
