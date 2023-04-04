// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::*;

/// The root of all catalogs.
pub struct RootCatalog {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    schema_idxs: HashMap<String, SchemaId>,
    schemas: HashMap<SchemaId, SchemaCatalog>,
    next_schema_id: SchemaId,
}

impl Default for RootCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl RootCatalog {
    pub fn new() -> RootCatalog {
        let mut inner = Inner::default();
        inner.add_schema(DEFAULT_SCHEMA_NAME.into()).unwrap();
        inner.add_internals();
        RootCatalog {
            inner: Mutex::new(inner),
        }
    }

    pub fn all_schemas(&self) -> HashMap<SchemaId, SchemaCatalog> {
        let inner = self.inner.lock().unwrap();
        inner.schemas.clone()
    }

    pub fn get_schema_id_by_name(&self, name: &str) -> Option<SchemaId> {
        let inner = self.inner.lock().unwrap();
        inner.schema_idxs.get(name).cloned()
    }

    pub fn get_schema_by_id(&self, schema_id: SchemaId) -> Option<SchemaCatalog> {
        let inner = self.inner.lock().unwrap();
        inner.schemas.get(&schema_id).cloned()
    }

    pub fn get_schema_by_name(&self, name: &str) -> Option<SchemaCatalog> {
        let inner = self.inner.lock().unwrap();
        let id = inner.schema_idxs.get(name)?;
        inner.schemas.get(id).cloned()
    }

    pub fn get_table(&self, table_ref_id: &TableRefId) -> Option<Arc<TableCatalog>> {
        let schema = self.get_schema_by_id(table_ref_id.schema_id)?;
        schema.get_table_by_id(table_ref_id.table_id)
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<Arc<TableCatalog>> {
        let name = name.to_lowercase();
        let (schema_name, table_name) = split_name(&name)?;
        let ref_id = self.get_table_id_by_name(schema_name, table_name)?;
        self.get_table(&ref_id)
    }

    pub fn get_column(&self, column_ref_id: &ColumnRefId) -> Option<ColumnCatalog> {
        self.get_table(&column_ref_id.table())?
            .get_column_by_id(column_ref_id.column_id)
    }

    pub fn add_table(
        &self,
        schema_id: SchemaId,
        name: String,
        columns: Vec<ColumnCatalog>,
        is_materialized_view: bool,
        ordered_pk_ids: Vec<ColumnId>,
    ) -> Result<TableId, CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        let schema = inner.schemas.get_mut(&schema_id).unwrap();
        schema.add_table(name, columns, is_materialized_view, ordered_pk_ids)
    }

    pub fn drop_table(&self, table_ref_id: TableRefId) {
        let mut inner = self.inner.lock().unwrap();
        let schema = inner.schemas.get_mut(&table_ref_id.schema_id).unwrap();
        schema.delete_table(table_ref_id.table_id);
    }

    pub fn get_table_id_by_name(&self, schema_name: &str, table_name: &str) -> Option<TableRefId> {
        let schema = self.get_schema_by_name(schema_name)?;
        let table = schema.get_table_by_name(table_name)?;

        Some(TableRefId {
            schema_id: schema.id(),
            table_id: table.id(),
        })
    }
}

impl Inner {
    fn add_schema(&mut self, name: String) -> Result<SchemaId, CatalogError> {
        if self.schema_idxs.contains_key(&name) {
            return Err(CatalogError::Duplicated("schema", name));
        }
        let schema_id = self.next_schema_id;
        self.next_schema_id += 1;
        let schema_catalog = SchemaCatalog::new(schema_id, name.clone());
        self.schema_idxs.insert(name, schema_id);
        self.schemas.insert(schema_id, schema_catalog);
        Ok(schema_id)
    }

    fn add_internals(&mut self) {
        let schema_id = self.add_schema(INTERNAL_SCHEMA_NAME.into()).unwrap();
        let table_id = self
            .schemas
            .get_mut(&schema_id)
            .unwrap()
            .add_table(
                CONTRIBUTORS_TABLE_NAME.to_string(),
                vec![ColumnCatalog::new(
                    0,
                    DataTypeKind::String
                        .not_null()
                        .to_column("github_id".into()),
                )],
                false,
                vec![],
            )
            .unwrap();
        assert_eq!(table_id, CONTRIBUTORS_TABLE_ID);
    }
}

fn split_name(name: &str) -> Option<(&str, &str)> {
    match name.split('.').collect::<Vec<&str>>()[..] {
        [table] => Some((DEFAULT_SCHEMA_NAME, table)),
        [schema, table] => Some((schema, table)),
        _ => None,
    }
}
