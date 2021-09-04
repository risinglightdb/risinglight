use super::*;
use crate::types::{ColumnId, DataType, TableId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) struct TableCatalog {
    table_id: TableId,
    table_name: String,
    /// Mapping from column names to column ids
    column_idxs: HashMap<String, ColumnId>,
    columns: HashMap<ColumnId, ColumnCatalogRef>,
    is_materialized_view: bool,
    next_column_id: ColumnId,
}

impl TableCatalog {
    pub(crate) fn add_column(
        &mut self,
        column_name: String,
        column_desc: ColumnDesc,
    ) -> Result<ColumnId, CatalogError> {
        if self.column_idxs.contains_key(&column_name) {
            return Err(CatalogError::Duplicated("column", column_name));
        }
        let column_id = self.next_column_id;
        self.next_column_id += 1;
        let col_catalog = Arc::new(Mutex::new(ColumnCatalog::new(
            column_id,
            column_name.clone(),
            column_desc,
        )));
        self.column_idxs.insert(column_name, column_id);
        self.columns.insert(column_id, col_catalog);
        Ok(column_id)
    }

    pub(crate) fn contains_column(&self, name: &str) -> bool {
        self.column_idxs.contains_key(name)
    }

    pub(crate) fn all_columns(&self) -> &HashMap<TableId, ColumnCatalogRef> {
        &self.columns
    }

    pub(crate) fn get_column_id_by_name(&self, name: &str) -> Option<ColumnId> {
        self.column_idxs.get(name).cloned()
    }

    pub(crate) fn get_column_by_id(&self, table_id: TableId) -> Option<ColumnCatalogRef> {
        self.columns.get(&table_id).cloned()
    }

    pub(crate) fn get_column_by_name(&self, name: &str) -> Option<ColumnCatalogRef> {
        match self.get_column_id_by_name(name) {
            Some(v) => self.get_column_by_id(v),
            None => None,
        }
    }

    pub(crate) fn table_name(&self) -> &str {
        &self.table_name
    }

    pub(crate) fn table_id(&self) -> TableId {
        self.table_id
    }

    pub(crate) fn new(
        table_id: TableId,
        table_name: String,
        column_names: Vec<String>,
        columns: Vec<ColumnDesc>,
        is_materialized_view: bool,
    ) -> TableCatalog {
        let mut table_catalog = TableCatalog {
            table_id,
            table_name,
            column_idxs: HashMap::new(),
            columns: HashMap::new(),
            is_materialized_view,
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
        let col0 = ColumnDesc::new(DataType::Int32, true, false);
        let col1 = ColumnDesc::new(DataType::Bool, false, false);

        let col_names = vec![String::from("a"), String::from("b")];
        let col_descs = vec![col0, col1];
        let table_catalog = TableCatalog::new(0, String::from("t"), col_names, col_descs, false);

        assert_eq!(table_catalog.contains_column("c"), false);
        assert_eq!(table_catalog.contains_column("a"), true);
        assert_eq!(table_catalog.contains_column("b"), true);

        assert_eq!(table_catalog.get_column_id_by_name("a"), Some(0));
        assert_eq!(table_catalog.get_column_id_by_name("b"), Some(1));
        let arc_col0 = table_catalog.get_column_by_id(0).unwrap();
        let col0_catalog = &arc_col0.as_ref().lock().unwrap();

        assert_eq!(col0_catalog.name(), "a");
        assert_eq!(col0_catalog.datatype(), DataType::Int32);
        let arc_col1 = table_catalog.get_column_by_id(1).unwrap();
        let col1_catalog = &arc_col1.as_ref().lock().unwrap();
        assert_eq!(col1_catalog.name(), "b");
        assert_eq!(col1_catalog.datatype(), DataType::Bool);
    }
}
