use super::*;
use crate::types::{ColumnId, TableId};
use std::collections::HashMap;
use std::sync::{Mutex};

pub struct TableCatalog {
    id: TableId,
    inner: Mutex<Inner>,
}

struct Inner {
    name: String,
    /// Mapping from column names to column ids
    column_idxs: HashMap<String, ColumnId>,
    columns: HashMap<ColumnId, ColumnCatalog>,
    is_materialized_view: bool,
    next_column_id: ColumnId,
}

impl TableCatalog {
    pub fn new(
        id: TableId,
        name: String,
        column_names: Vec<String>,
        columns: Vec<ColumnDesc>,
        is_materialized_view: bool,
    ) -> TableCatalog {
        assert_eq!(column_names.len(), columns.len());
        let table_catalog = TableCatalog {
            id,
            inner: Mutex::new(Inner {
                name,
                column_idxs: HashMap::new(),
                columns: HashMap::new(),
                is_materialized_view,
                next_column_id: 0,
            }),
        };
        for (name, desc) in column_names.into_iter().zip(columns.into_iter()) {
            table_catalog.add_column(name, desc).unwrap();
        }
        table_catalog
    }

    pub fn add_column(&self, name: String, desc: ColumnDesc) -> Result<ColumnId, CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.column_idxs.contains_key(&name) {
            return Err(CatalogError::Duplicated("column", name));
        }
        let column_id = inner.next_column_id;
        inner.next_column_id += 1;
        let col_catalog = ColumnCatalog::new(column_id, name.clone(), desc);
        inner.column_idxs.insert(name, column_id);
        inner.columns.insert(column_id, col_catalog);
        Ok(column_id)
    }

    pub fn contains_column(&self, name: &str) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.column_idxs.contains_key(name)
    }

    pub fn all_columns(&self) -> HashMap<ColumnId, ColumnCatalog> {
        let inner = self.inner.lock().unwrap();
        inner.columns.clone()
    }

    pub fn get_column_id_by_name(&self, name: &str) -> Option<ColumnId> {
        let inner = self.inner.lock().unwrap();
        inner.column_idxs.get(name).cloned()
    }

    pub fn get_column_by_id(&self, id: ColumnId) -> Option<ColumnCatalog> {
        let inner = self.inner.lock().unwrap();
        inner.columns.get(&id).cloned()
    }

    pub fn get_column_by_name(&self, name: &str) -> Option<ColumnCatalog> {
        let inner = self.inner.lock().unwrap();
        inner
            .column_idxs
            .get(name)
            .and_then(|id| inner.columns.get(id))
            .cloned()
    }

    pub fn name(&self) -> String {
        let inner = self.inner.lock().unwrap();
        inner.name.clone()
    }

    pub fn id(&self) -> TableId {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_table_catalog() {
        let col0 = ColumnDesc::new(DataType::new(DataTypeKind::Int32, false), false);
        let col1 = ColumnDesc::new(DataType::new(DataTypeKind::Bool, false), false);

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
        assert_eq!(col0_catalog.datatype().kind(), DataTypeKind::Int32);

        let col1_catalog = table_catalog.get_column_by_id(1).unwrap();
        assert_eq!(col1_catalog.name(), "b");
        assert_eq!(col1_catalog.datatype().kind(), DataTypeKind::Bool);
    }
}
