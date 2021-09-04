use crate::catalog::{ColumnDesc, TableCatalog, TableCatalogRef};
use crate::types::{DataType, SchemaId, TableId};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

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
        column_names: Vec<String>,
        columns: Vec<ColumnDesc>,
        is_materialized_view: bool,
    ) -> Result<TableId, String> {
        if self.table_idxs.contains_key(&table_name) {
            Err(String::from("Duplicated table name!"))
        } else {
            let table_id = self.next_table_id;
            self.next_table_id += 1;
            let table_catalog = Arc::new(Mutex::new(TableCatalog::new(
                table_id,
                table_name.clone(),
                column_names,
                columns,
                is_materialized_view,
            )));
            self.table_idxs.insert(table_name, table_id);
            self.tables.insert(table_id, table_catalog);
            Ok(table_id)
        }
    }

    pub(crate) fn delete_table(&mut self, table_name: &String) -> Result<(), String> {
        if self.table_idxs.contains_key(table_name) {
            let id = self.table_idxs.remove(table_name).unwrap();
            self.tables.remove(&id);
            Ok(())
        } else {
            Err(String::from("Table does not exist: ") + table_name)
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
