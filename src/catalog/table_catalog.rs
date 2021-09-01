use crate::catalog::{ColumnCatalog, ColumnCatalogRef};
use crate::types::{column_id_t, table_id_t, DataTypeRef, Int32Type};
use std::collections::{BTreeMap, HashMap};

pub(crate) struct TableCatalog {
    table_id: table_id_t,
    table_name: String,
    // Mapping from column names to column ids
    column_idxs: HashMap<String, column_id_t>,
    columns: BTreeMap<column_id_t, ColumnCatalogRef>,
    is_materialized_view: bool,
    next_column_id: column_id_t,
}

impl TableCatalog {

    pub(crate) fn add_column(
        &mut self,
        column_catalog: ColumnCatalogRef,
    ) -> Result<column_id_t, String> {
        let column_name = column_catalog.get_column_name();
        if self.column_idxs.contains_key(&column_name) {
            Err(String::from("Duplicated column names!"))
        } else {
            let column_id = self.next_column_id;
            self.next_column_id += 1;
            self.column_idxs.insert(column_name, column_id);
            self.columns.insert(column_id, column_catalog);
            Ok(column_id)
        }
    }

    pub(crate) fn contains_column(&self, name: &String) -> bool {
        self.column_idxs.contains_key(name)
    }

    pub(crate) fn get_all_columns(&self) -> &BTreeMap<table_id_t, ColumnCatalogRef> {
        &self.columns
    }

    pub(crate) fn get_column_id_by_name(&self, name: &String) -> Option<column_id_t> {
        match self.column_idxs.get(name) {
            Some(v) => Some(*v),
            None => None
        }
    }

    pub(crate) fn get_column_by_id(&self, table_id: &table_id_t) -> Option<ColumnCatalogRef> {
        match self.columns.get(table_id) {
            Some(v) => Some(v.clone()),
            None => None
        }
    }

    pub(crate) fn get_column_by_name(&self, name: &String) -> Option<ColumnCatalogRef> {
        match self.get_column_id_by_name(name) {
            Some(v) => {
                self.get_column_by_id(&v)
            }
            None => None
        }
    }

    pub(crate) fn get_table_name(&self) -> String {
        self.table_name.clone()
    }

    pub(crate) fn get_table_id(&self) -> table_id_t {
        self.table_id
    }

    pub(crate) fn new(
        table_id: table_id_t,
        table_name: String,
        columns: &Vec<ColumnCatalogRef>,
        is_materialized_view: bool,
    ) -> TableCatalog {
        let mut table_catalog = TableCatalog {
            table_id: table_id,
            table_name: table_name,
            column_idxs: HashMap::new(),
            columns: BTreeMap::new(),
            is_materialized_view: is_materialized_view,
            next_column_id: 0,
        };

        for col_catalog in columns.iter() {
            table_catalog.add_column(col_catalog.clone()).unwrap();
        }

        table_catalog
    }
}
