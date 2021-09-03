use crate::catalog::{SchemaCatalog, SchemaCatalogRef};
use crate::types::{DataType, DatabaseId, SchemaId};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub(crate) struct DatabaseCatalog {
    database_id: DatabaseId,
    database_name: String,
    schema_idxs: HashMap<String, SchemaId>,
    schemas: BTreeMap<SchemaId, SchemaCatalogRef>,
    next_schema_id: SchemaId,
}

impl DatabaseCatalog {
    pub(crate) fn add_schema(
        &mut self,
        schema_name: String,
        schema_catalog: SchemaCatalog,
    ) -> Result<SchemaId, String> {
        if self.schema_idxs.contains_key(&schema_name) {
            Err(String::from("Duplicated schema name!"))
        } else {
            let schema_id = self.next_schema_id;
            self.next_schema_id += 1;
            let schema_catalog = Arc::new(schema_catalog);
            self.schema_idxs.insert(schema_name, schema_id);
            self.schemas.insert(schema_id, schema_catalog);
            Ok(schema_id)
        }
    }

    pub(crate) fn delete_schema(&mut self, schema_name: &String) -> Result<(), String> {
        if self.schema_idxs.contains_key(schema_name) {
            let id = self.schema_idxs.remove(schema_name).unwrap();
            self.schemas.remove(&id);
            Ok(())
        } else {
            Err(String::from("Schema does not exist: ") + schema_name)
        }
    }

    pub(crate) fn get_all_schemas(&self) -> &BTreeMap<SchemaId, SchemaCatalogRef> {
        &self.schemas
    }

    pub(crate) fn get_schema_id_by_name(&self, name: &String) -> Option<SchemaId> {
        self.schema_idxs.get(name).cloned()
    }

    pub(crate) fn get_schema_by_id(&self, schema_id: SchemaId) -> Option<SchemaCatalogRef> {
        self.schemas.get(&schema_id).cloned()
    }

    pub(crate) fn get_schema_by_name(&self, name: &String) -> Option<SchemaCatalogRef> {
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
        DatabaseCatalog {
            database_id: database_id,
            database_name: database_name,
            schema_idxs: HashMap::new(),
            schemas: BTreeMap::new(),
            next_schema_id: 0,
        }
    }
}
