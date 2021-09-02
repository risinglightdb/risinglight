use crate::types::{schema_id_t, table_id_t, BoolType, DataTypeRef, Int32Type, DataTypeEnum};
use crate::catalog::{TableCatalog, TableCatalogRef};
use std::collections::{BTreeMap, HashMap};

struct SchemaCatalog {
    schema_id: schema_id_t,
    schema_name: String,
    table_idxs: HashMap<String, table_id_t>,
    tables: BTreeMap<table_id_t, TableCatalogRef>,
    next_table_id: table_id_t
}

impl SchemaCatalog {
    pub(crate) fn add_table(
        &mut self,
        table_name: String,
        table_catalog: TableCatalog,
    ) -> Result<table_id_t, String> {
        if self.table_idxs.contains_key(&column_name) {
            Err(String::from("Duplicated column names!"))
        } else {
            
            let table_id = self.next_table_id;
            self.next_table_id += 1;
            let table_catalog = Arc::new(table_catalog);
            self.table_idxs.insert(table_name, table_id);
            self.tables.insert(table_id, table_catalog);
            Ok(table_id)
        }
    }

    pub(crate) fn get_all_tables(&self) -> &BTreeMap<table_id_t, TableCatalogRef> {
        &self.tables
    }

    pub(crate) fn get_table_id_by_name(&self, name: &String) -> Option<schema_id_t> {
        match self.table_idxs.get(name) {
            Some(v) => Some(*v),
            None => None,
        }
    }

    pub(crate) fn get_table_by_id(&self, table_id: table_id_t) -> Option<TableCatalogRef> {
        match self.tables.get(&table_id) {
            Some(v) => Some(v.clone()),
            None => None,
        }
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

    pub(crate) fn get_schema_id(&self) -> schema_id_t {
        self.schema_id
    }
    
    pub(crate) fn new(schema_id: schema_id_t,
        schema_name: String) -> SchemaCatalog {
        SchemaCatalog{
            schema_id: schema_id,
            schema_name: schema_name,
            table_idxs: HashMap::new(),
            tables: BTreeMap::new(),
            next_table_id: 0
        }
    }
}