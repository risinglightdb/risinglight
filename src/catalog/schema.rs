// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use super::*;

/// The catalog of a schema.
#[derive(Clone)]
pub struct SchemaCatalog {
    id: SchemaId,
    name: String,
    table_idxs: HashMap<String, TableId>,
    tables: HashMap<TableId, Arc<TableCatalog>>,
    next_table_id: TableId,
}

impl SchemaCatalog {
    pub fn new(id: SchemaId, name: String) -> SchemaCatalog {
        SchemaCatalog {
            id,
            name,
            table_idxs: HashMap::new(),
            tables: HashMap::new(),
            next_table_id: 0,
        }
    }

    pub(in crate::catalog) fn add_table(
        &mut self,
        name: String,
        columns: Vec<ColumnCatalog>,
        is_materialized_view: bool,
        ordered_pk_ids: Vec<ColumnId>,
    ) -> Result<TableId, CatalogError> {
        if self.table_idxs.contains_key(&name) {
            return Err(CatalogError::Duplicated("table", name));
        }
        let table_id = self.next_table_id;
        self.next_table_id += 1;
        let table_catalog = Arc::new(TableCatalog::new(
            table_id,
            name.clone(),
            columns,
            is_materialized_view,
            ordered_pk_ids,
        ));
        self.table_idxs.insert(name, table_id);
        self.tables.insert(table_id, table_catalog);
        Ok(table_id)
    }

    pub(in crate::catalog) fn delete_table(&mut self, id: TableId) {
        let catalog = self.tables.remove(&id).unwrap();
        self.table_idxs.remove(&catalog.name()).unwrap();
    }

    pub fn all_tables(&self) -> HashMap<TableId, Arc<TableCatalog>> {
        self.tables.clone()
    }

    pub fn get_table_id_by_name(&self, name: &str) -> Option<TableId> {
        self.table_idxs.get(name).cloned()
    }

    pub fn get_table_by_id(&self, table_id: TableId) -> Option<Arc<TableCatalog>> {
        self.tables.get(&table_id).cloned()
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<Arc<TableCatalog>> {
        self.table_idxs
            .get(name)
            .and_then(|id| self.tables.get(id))
            .cloned()
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn id(&self) -> SchemaId {
        self.id
    }
}
