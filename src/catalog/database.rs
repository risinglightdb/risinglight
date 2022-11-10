// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use super::*;

/// The catalog of a database.
#[derive(Clone)]
pub struct DatabaseCatalog {
    id: DatabaseId,
    name: String,
    schema_idxs: HashMap<String, SchemaId>,
    schemas: HashMap<SchemaId, SchemaCatalog>,
    next_schema_id: SchemaId,
}

impl DatabaseCatalog {
    pub fn new(id: DatabaseId, name: String) -> Self {
        let mut db_catalog = DatabaseCatalog {
            id,
            name,
            schema_idxs: HashMap::new(),
            schemas: HashMap::new(),
            next_schema_id: 0,
        };
        db_catalog.add_schema(DEFAULT_SCHEMA_NAME.into()).unwrap();
        db_catalog.add_internals();
        db_catalog
    }

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

    pub fn all_schemas(&self) -> HashMap<SchemaId, SchemaCatalog> {
        self.schemas.clone()
    }

    pub fn get_schema_id_by_name(&self, name: &str) -> Option<SchemaId> {
        self.schema_idxs.get(name).cloned()
    }

    pub fn get_schema_by_id(&self, schema_id: SchemaId) -> Option<Arc<SchemaCatalog>> {
        Some(Arc::new(self.schemas.get(&schema_id).cloned().unwrap()))
    }

    pub fn get_schema_by_name(&self, name: &str) -> Option<Arc<SchemaCatalog>> {
        Some(Arc::new(
            self.schema_idxs
                .get(name)
                .and_then(|id| self.schemas.get(id))
                .cloned()
                .unwrap(),
        ))
    }

    pub(in crate::catalog) fn get_schema_mut(
        &mut self,
        schema_id: SchemaId,
    ) -> Option<&mut SchemaCatalog> {
        self.schemas.get_mut(&schema_id)
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn id(&self) -> DatabaseId {
        self.id
    }

    fn add_internals(&mut self) {
        self.add_schema(INTERNAL_SCHEMA_NAME.into()).unwrap();
        let schema_id = self.get_schema_id_by_name(INTERNAL_SCHEMA_NAME).unwrap();
        let schema = self.get_schema_mut(schema_id).unwrap();
        schema
            .add_table(
                "contributors".to_string(),
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
    }
}
