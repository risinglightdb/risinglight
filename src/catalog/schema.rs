use crate::catalog::{TableCatalog, TableCatalogRef};
use crate::types::{BoolType, DataTypeEnum, DataTypeRef, Int32Type, SchemaId, TableId};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub(crate) struct SchemaCatalog {
    schema_id: SchemaId,
    schema_name: String,
    table_idxs: HashMap<String, TableId>,
    tables: BTreeMap<TableId, TableCatalogRef>,
    next_table_id: TableId,
}

impl SchemaCatalog {
    pub(crate) fn add_table(
        &mut self,
        table_name: String,
        table_catalog: TableCatalog,
    ) -> Result<TableId, String> {
        if self.table_idxs.contains_key(&table_name) {
            Err(String::from("Duplicated table name!"))
        } else {
            let table_id = self.next_table_id;
            self.next_table_id += 1;
            let table_catalog = Arc::new(table_catalog);
            self.table_idxs.insert(table_name, table_id);
            self.tables.insert(table_id, table_catalog);
            Ok(table_id)
        }
    }

    pub(crate) fn delete_table(&mut self, table_name: &String) -> Option<TableCatalogRef> {
        let table_id = self.table_idxs.remove(table_name);
        match table_id {
            Some(v) => self.tables.remove(&v),
            None => None,
        }
    }

    pub(crate) fn get_all_tables(&self) -> &BTreeMap<TableId, TableCatalogRef> {
        &self.tables
    }

    pub(crate) fn get_table_id_by_name(&self, name: &String) -> Option<TableId> {
        self.table_idxs.get(name).cloned()
    }

    pub(crate) fn get_table_by_id(&self, table_id: TableId) -> Option<TableCatalogRef> {
        self.tables.get(&table_id).cloned()
    }

    pub(crate) fn get_table_by_name(&self, name: &String) -> Option<TableCatalogRef> {
        match self.get_table_id_by_name(name) {
            Some(v) => self.get_table_by_id(v),
            None => None,
        }
    }

    pub(crate) fn get_schema_name(&self) -> String {
        self.schema_name.clone()
    }

    pub(crate) fn get_schema_id(&self) -> SchemaId {
        self.schema_id
    }

    pub(crate) fn new(schema_id: SchemaId, schema_name: String) -> SchemaCatalog {
        SchemaCatalog {
            schema_id: schema_id,
            schema_name: schema_name,
            table_idxs: HashMap::new(),
            tables: BTreeMap::new(),
            next_table_id: 0,
        }
    }
}
