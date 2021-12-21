use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use super::*;

/// The catalog of a table.
pub struct TableCatalog {
    id: TableId,
    inner: Mutex<Inner>,
}

struct Inner {
    name: String,
    /// Mapping from column names to column ids
    column_idxs: HashMap<String, ColumnId>,
    columns: BTreeMap<ColumnId, ColumnCatalog>,
    next_column_id: ColumnId,
}

impl TableCatalog {
    pub(super) fn new(id: TableId, name: String) -> TableCatalog {
        TableCatalog {
            id,
            inner: Mutex::new(Inner {
                name,
                column_idxs: HashMap::new(),
                columns: BTreeMap::new(),
                next_column_id: 0,
            }),
        }
    }

    pub fn id(&self) -> TableId {
        self.id
    }

    pub fn name(&self) -> String {
        let inner = self.inner.lock().unwrap();
        inner.name.clone()
    }

    pub fn add_column(&self, name: &str, desc: ColumnDesc) -> Result<ColumnId, CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.column_idxs.contains_key(name) {
            return Err(CatalogError::Duplicated("column", name.into()));
        }
        let id = inner.next_column_id;
        inner.next_column_id += 1;
        inner.column_idxs.insert(name.into(), id);
        inner
            .columns
            .insert(id, ColumnCatalog::new(id, name.into(), desc));
        Ok(id)
    }

    pub fn contains_column(&self, name: &str) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.column_idxs.contains_key(name)
    }

    pub fn all_columns(&self) -> BTreeMap<ColumnId, ColumnCatalog> {
        let inner = self.inner.lock().unwrap();
        inner.columns.clone()
    }

    pub fn get_column(&self, id: ColumnId) -> Option<ColumnCatalog> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataTypeExt, DataTypeKind};

    #[test]
    fn test_table_catalog() {
        let table_catalog = TableCatalog::new(0, "t".into());
        table_catalog
            .add_column("a", DataTypeKind::Int(None).not_null().to_column())
            .unwrap();
        table_catalog
            .add_column("b", DataTypeKind::Boolean.not_null().to_column())
            .unwrap();

        assert!(!table_catalog.contains_column("c"));
        assert!(table_catalog.contains_column("a"));
        assert!(table_catalog.contains_column("b"));

        assert_eq!(table_catalog.get_column_by_name("a").unwrap().id(), 0);
        assert_eq!(table_catalog.get_column_by_name("b").unwrap().id(), 1);

        let col0_catalog = table_catalog.get_column(0).unwrap();
        assert_eq!(col0_catalog.name(), "a");
        assert_eq!(col0_catalog.datatype().kind(), DataTypeKind::Int(None));

        let col1_catalog = table_catalog.get_column(1).unwrap();
        assert_eq!(col1_catalog.name(), "b");
        assert_eq!(col1_catalog.datatype().kind(), DataTypeKind::Boolean);
    }
}
