// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use super::*;
use crate::types::{ColumnId, TableId};

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

    #[allow(dead_code)]
    is_materialized_view: bool,
    next_column_id: ColumnId,
    primary_key_ids: Vec<ColumnId>,
}

impl TableCatalog {
    pub fn new(
        id: TableId,
        name: String,
        columns: Vec<ColumnCatalog>,
        is_materialized_view: bool,
    ) -> TableCatalog {
        let table_catalog = TableCatalog {
            id,
            inner: Mutex::new(Inner {
                name,
                column_idxs: HashMap::new(),
                columns: BTreeMap::new(),
                is_materialized_view,
                next_column_id: 0,
                primary_key_ids: Vec::new(),
            }),
        };
        let mut pk_ids = vec![];
        for col_catalog in columns {
            if col_catalog.is_primary() {
                pk_ids.push(col_catalog.id());
            }
            table_catalog.add_column(col_catalog).unwrap();
        }

        table_catalog.set_primary_key_ids(&pk_ids);
        table_catalog
    }

    pub fn set_primary_key_ids(&self, pk_ids: &[ColumnId]) {
        let mut inner = self.inner.lock().unwrap();
        inner.primary_key_ids = pk_ids.to_owned();
    }

    pub fn get_primary_key_ids(&self) -> Vec<ColumnId> {
        let inner = self.inner.lock().unwrap();
        inner.primary_key_ids.clone()
    }

    pub fn add_column(&self, col_catalog: ColumnCatalog) -> Result<ColumnId, CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.column_idxs.contains_key(col_catalog.name()) {
            return Err(CatalogError::Duplicated(
                "column",
                col_catalog.name().into(),
            ));
        }
        inner.next_column_id += 1;
        let id = col_catalog.id();
        inner
            .column_idxs
            .insert(col_catalog.name().to_string(), col_catalog.id());
        inner.columns.insert(id, col_catalog);
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
        let col0 = ColumnCatalog::new(0, DataTypeKind::Int(None).not_null().to_column("a".into()));
        let col1 = ColumnCatalog::new(1, DataTypeKind::Boolean.not_null().to_column("b".into()));

        let col_catalogs = vec![col0, col1];
        let table_catalog = TableCatalog::new(0, "t".into(), col_catalogs, false);

        assert!(!table_catalog.contains_column("c"));
        assert!(table_catalog.contains_column("a"));
        assert!(table_catalog.contains_column("b"));

        assert_eq!(table_catalog.get_column_id_by_name("a"), Some(0));
        assert_eq!(table_catalog.get_column_id_by_name("b"), Some(1));

        let col0_catalog = table_catalog.get_column_by_id(0).unwrap();
        assert_eq!(col0_catalog.name(), "a");
        assert_eq!(col0_catalog.datatype().kind(), DataTypeKind::Int(None));

        let col1_catalog = table_catalog.get_column_by_id(1).unwrap();
        assert_eq!(col1_catalog.name(), "b");
        assert_eq!(col1_catalog.datatype().kind(), DataTypeKind::Boolean);

        // test with two primary key
        let col0 = ColumnCatalog::new(
            0,
            DataTypeKind::Int(None)
                .not_null()
                .to_column_primary_key("a".into()),
        );
        let col1 = ColumnCatalog::new(
            1,
            DataTypeKind::Int(None)
                .not_null()
                .to_column_primary_key("b".into()),
        );
        let col2 = ColumnCatalog::new(2, DataTypeKind::Int(None).nullable().to_column("c".into()));
        let col3 = ColumnCatalog::new(3, DataTypeKind::Int(None).nullable().to_column("d".into()));

        let col_catalogs = vec![col0, col1, col2, col3];
        let table_catalog = TableCatalog::new(0, "t".into(), col_catalogs, false);
        assert_eq!(table_catalog.get_primary_key_ids(), vec![0, 1]);
    }
}
