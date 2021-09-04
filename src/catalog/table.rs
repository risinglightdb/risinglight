use super::*;
use crate::types::{ColumnId, DataType, DataTypeEnum, TableId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) struct TableCatalog {
    table_id: TableId,
    table_name: String,
    /// Mapping from column names to column ids
    column_idxs: HashMap<String, ColumnId>,
    columns: HashMap<ColumnId, ColumnCatalog>,
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
        let col_catalog = ColumnCatalog::new(column_id, column_name.clone(), column_desc);
        self.column_idxs.insert(column_name, column_id);
        self.columns.insert(column_id, col_catalog);
        Ok(column_id)
    }

    pub(crate) fn contains_column(&self, name: &str) -> bool {
        self.column_idxs.contains_key(name)
    }

    pub(crate) fn all_columns(&self) -> &HashMap<TableId, ColumnCatalog> {
        &self.columns
    }

    pub(crate) fn get_column_id_by_name(&self, name: &str) -> Option<ColumnId> {
        self.column_idxs.get(name).cloned()
    }

    pub(crate) fn get_column_by_id(&self, table_id: TableId) -> Option<&ColumnCatalog> {
        self.columns.get(&table_id)
    }

    pub(crate) fn get_column_by_name(&self, name: &str) -> Option<&ColumnCatalog> {
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
        let col0 = ColumnDesc::new(DataType::new(DataTypeEnum::Int32, false), false);
        let col1 = ColumnDesc::new(DataType::new(DataTypeEnum::Bool, false), false);

        let col_names = vec![String::from("a"), String::from("b")];
        let col_descs = vec![col0, col1];
        let table_catalog = TableCatalog::new(0, String::from("t"), col_names, col_descs, false);

        assert_eq!(table_catalog.contains_column("c"), false);
        assert_eq!(table_catalog.contains_column("a"), true);
        assert_eq!(table_catalog.contains_column("b"), true);

        assert_eq!(table_catalog.get_column_id_by_name("a"), Some(0));
        assert_eq!(table_catalog.get_column_id_by_name("b"), Some(1));

        let col0_catalog = table_catalog.get_column_by_id(0).unwrap();
        assert_eq!(col0_catalog.name(), "a");
        assert_eq!(col0_catalog.datatype().data_type_info(), DataTypeEnum::Int32);

        let col1_catalog = table_catalog.get_column_by_id(1).unwrap();
        assert_eq!(col1_catalog.name(), "b");
        assert_eq!(col1_catalog.datatype().data_type_info(), DataTypeEnum::Bool);
    }
}
