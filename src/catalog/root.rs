// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::*;

/// The root of all catalogs.
pub struct RootCatalog {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    database_idxs: HashMap<String, DatabaseId>,
    databases: HashMap<DatabaseId, DatabaseCatalog>,
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
        let database_catalog = DatabaseCatalog::new(database_id, name.clone());
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

    pub fn all_databases(&self) -> HashMap<DatabaseId, DatabaseCatalog> {
        let inner = self.inner.lock().unwrap();
        inner.databases.clone()
    }

    pub fn get_database_id_by_name(&self, name: &str) -> Option<DatabaseId> {
        let inner = self.inner.lock().unwrap();
        inner.database_idxs.get(name).cloned()
    }

    pub fn get_database_by_id(&self, database_id: DatabaseId) -> Option<Arc<DatabaseCatalog>> {
        let inner = self.inner.lock().unwrap();
        Some(Arc::new(
            inner.databases.get(&database_id).cloned().unwrap(),
        ))
    }

    pub fn get_database_by_name(&self, name: &str) -> Option<Arc<DatabaseCatalog>> {
        let inner = self.inner.lock().unwrap();
        Some(Arc::new(
            inner
                .database_idxs
                .get(name)
                .and_then(|id| inner.databases.get(id))
                .cloned()
                .unwrap(),
        ))
    }

    pub fn get_table(&self, table_ref_id: &TableRefId) -> Option<Arc<TableCatalog>> {
        let db = self.get_database_by_id(table_ref_id.database_id)?;
        let schema = db.get_schema_by_id(table_ref_id.schema_id)?;
        schema.get_table_by_id(table_ref_id.table_id)
    }

    pub fn get_column(&self, column_ref_id: &ColumnRefId) -> Option<ColumnCatalog> {
        self.get_table(&column_ref_id.table())?
            .get_column_by_id(column_ref_id.column_id)
    }

    pub fn add_table(
        &self,
        table_ref_id: TableRefId,
        name: String,
        columns: Vec<ColumnCatalog>,
        is_materialized_view: bool,
        ordered_pk_ids: Vec<ColumnId>,
    ) -> Result<TableId, CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        let database = inner.databases.get_mut(&table_ref_id.database_id).unwrap();
        let schema = database.get_schema_mut(table_ref_id.schema_id).unwrap();
        schema.add_table(name, columns, is_materialized_view, ordered_pk_ids)
    }

    pub fn drop_table(&self, table_ref_id: TableRefId) {
        let mut inner = self.inner.lock().unwrap();
        let database = inner.databases.get_mut(&table_ref_id.database_id).unwrap();
        let schema = database.get_schema_mut(table_ref_id.schema_id).unwrap();
        schema.delete_table(table_ref_id.table_id);
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
