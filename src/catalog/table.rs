// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::{BTreeMap, HashMap};

use super::*;
use crate::planner::RecExpr;

/// The catalog of a table.
pub struct TableCatalog {
    id: TableId,
    name: String,
    /// Mapping from column names to column ids
    column_idxs: HashMap<String, ColumnId>,
    columns: BTreeMap<ColumnId, ColumnCatalog>,

    kind: TableKind,
    next_column_id: ColumnId,
    primary_key: Vec<ColumnId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableKind {
    Table,
    View(RecExpr),
}

impl TableCatalog {
    pub fn new(
        id: TableId,
        name: String,
        columns: Vec<ColumnCatalog>,
        primary_key: Vec<ColumnId>,
    ) -> TableCatalog {
        Self::new_(id, name, columns, TableKind::Table, primary_key)
    }

    pub fn new_view(
        id: TableId,
        name: String,
        columns: Vec<ColumnCatalog>,
        query: RecExpr,
    ) -> TableCatalog {
        Self::new_(id, name, columns, TableKind::View(query), vec![])
    }

    fn new_(
        id: TableId,
        name: String,
        columns: Vec<ColumnCatalog>,
        kind: TableKind,
        primary_key: Vec<ColumnId>,
    ) -> TableCatalog {
        let mut table_catalog = TableCatalog {
            id,
            name,
            column_idxs: HashMap::new(),
            columns: BTreeMap::new(),
            kind,
            next_column_id: 0,
            primary_key,
        };
        table_catalog
            .add_column(ColumnCatalog::new(
                u32::MAX,
                ColumnDesc::new("_rowid_", DataType::Int64, false),
            ))
            .unwrap();
        for col_catalog in columns {
            table_catalog.add_column(col_catalog).unwrap();
        }

        table_catalog
    }

    fn add_column(&mut self, col_catalog: ColumnCatalog) -> Result<ColumnId, CatalogError> {
        if self.column_idxs.contains_key(col_catalog.name()) {
            return Err(CatalogError::Duplicated(
                "column",
                col_catalog.name().into(),
            ));
        }
        self.next_column_id += 1;
        let id = col_catalog.id();
        self.column_idxs
            .insert(col_catalog.name().to_string(), col_catalog.id());
        self.columns.insert(id, col_catalog);
        Ok(id)
    }

    pub fn contains_column(&self, name: &str) -> bool {
        self.column_idxs.contains_key(name)
    }

    pub fn all_columns(&self) -> BTreeMap<ColumnId, ColumnCatalog> {
        let mut columns = self.columns.clone();
        columns.remove(&u32::MAX); // remove rowid
        columns
    }

    pub fn all_columns_with_rowid(&self) -> BTreeMap<ColumnId, ColumnCatalog> {
        self.columns.clone()
    }

    pub fn get_column_id_by_name(&self, name: &str) -> Option<ColumnId> {
        self.column_idxs.get(name).cloned()
    }

    pub fn get_column_by_id(&self, id: ColumnId) -> Option<ColumnCatalog> {
        self.columns.get(&id).cloned()
    }

    pub fn get_column_by_name(&self, name: &str) -> Option<ColumnCatalog> {
        self.column_idxs
            .get(name)
            .and_then(|id| self.columns.get(id))
            .cloned()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> TableId {
        self.id
    }

    pub fn primary_keys(&self) -> Vec<ColumnId> {
        self.primary_key.clone()
    }

    pub fn is_view(&self) -> bool {
        matches!(self.kind, TableKind::View(_))
    }

    /// Returns the query if it is a view.
    pub fn query(&self) -> Option<&RecExpr> {
        match &self.kind {
            TableKind::Table => None,
            TableKind::View(query) => Some(query),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_catalog() {
        let col0 = ColumnCatalog::new(0, ColumnDesc::new("a", DataType::Int32, false));
        let col1 = ColumnCatalog::new(1, ColumnDesc::new("b", DataType::Bool, false));

        let col_catalogs = vec![col0, col1];
        let table_catalog = TableCatalog::new(0, "t".into(), col_catalogs, vec![]);

        assert!(!table_catalog.contains_column("c"));
        assert!(table_catalog.contains_column("a"));
        assert!(table_catalog.contains_column("b"));

        assert_eq!(table_catalog.get_column_id_by_name("a"), Some(0));
        assert_eq!(table_catalog.get_column_id_by_name("b"), Some(1));

        let col0_catalog = table_catalog.get_column_by_id(0).unwrap();
        assert_eq!(col0_catalog.name(), "a");
        assert_eq!(col0_catalog.data_type(), DataType::Int32);

        let col1_catalog = table_catalog.get_column_by_id(1).unwrap();
        assert_eq!(col1_catalog.name(), "b");
        assert_eq!(col1_catalog.data_type(), DataType::Bool);
    }
}
