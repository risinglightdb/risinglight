use crate::catalog::{ColumnCatalog, ColumnCatalogRef, ColumnDesc};
use crate::types::{column_id_t, table_id_t, BoolType, DataTypeRef, Int32Type, DataTypeEnum};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
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
        column_name: String,
        column_desc: ColumnDesc,
    ) -> Result<column_id_t, String> {
        if self.column_idxs.contains_key(&column_name) {
            Err(String::from("Duplicated column names!"))
        } else {
            
            let column_id = self.next_column_id;
            self.next_column_id += 1;
            let col_catalog = Arc::new(ColumnCatalog::new(
                column_id,
                column_name.clone(),
                column_desc));
            self.column_idxs.insert(column_name, column_id);
            self.columns.insert(column_id, col_catalog);
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
            None => None,
        }
    }

    pub(crate) fn get_column_by_id(&self, table_id: table_id_t) -> Option<ColumnCatalogRef> {
        match self.columns.get(&table_id) {
            Some(v) => Some(v.clone()),
            None => None,
        }
    }

    pub(crate) fn get_column_by_name(&self, name: &String) -> Option<ColumnCatalogRef> {
        match self.get_column_id_by_name(name) {
            Some(v) => self.get_column_by_id(v),
            None => None,
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
        column_names: Vec<String>,
        columns: Vec<ColumnDesc>,
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
        assert_eq!(column_names.len(), columns.len());
        for (name, desc) in column_names.into_iter().zip(columns.into_iter()) {
            table_catalog.add_column(name, desc).unwrap();
        }

        table_catalog
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_table_catalog() {
       let col0 = ColumnDesc::new(Int32Type::new(false), true);
       let col1 = ColumnDesc::new(BoolType::new(false), false);

       let col_names = vec![String::from("a"), String::from("b")];
       let col_descs = vec![col0, col1];
       let table_catalog = TableCatalog::new(0, String::from("t"), col_names, col_descs, false);
       
       assert_eq!(table_catalog.contains_column(&String::from("c")), false);
       assert_eq!(table_catalog.contains_column(&String::from("a")), true);
       assert_eq!(table_catalog.contains_column(&String::from("b")), true);

       assert_eq!(table_catalog.get_column_id_by_name(&String::from("a")).unwrap(), 0);
       assert_eq!(table_catalog.get_column_id_by_name(&String::from("b")).unwrap(), 1);
       let col0_catalog = table_catalog.get_column_by_id(0).unwrap();

       assert_eq!(col0_catalog.get_column_name(), String::from("a"));
       assert_eq!(col0_catalog.get_column_datatype().as_ref().get_type(), DataTypeEnum::Int32);

       let col1_catalog = table_catalog.get_column_by_id(1).unwrap();
       assert_eq!(col1_catalog.get_column_name(), String::from("b"));
       assert_eq!(col1_catalog.get_column_datatype().as_ref().get_type(), DataTypeEnum::Bool);
    }
}
