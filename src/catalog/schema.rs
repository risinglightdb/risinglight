// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use super::function::FunctionCatalog;
use super::*;
use crate::binder::IndexType;
use crate::planner::RecExpr;

/// The catalog of a schema.
#[derive(Clone)]
pub struct SchemaCatalog {
    id: SchemaId,
    name: String,
    table_idxs: HashMap<String, TableId>,
    tables: HashMap<TableId, Arc<TableCatalog>>,
    indexes_idxs: HashMap<String, IndexId>,
    indexes: HashMap<IndexId, Arc<IndexCatalog>>,
    next_id: u32,
    /// Currently indexed by function name
    functions: HashMap<String, Arc<FunctionCatalog>>,
}

impl SchemaCatalog {
    pub fn new(id: SchemaId, name: String) -> SchemaCatalog {
        SchemaCatalog {
            id,
            name,
            table_idxs: HashMap::new(),
            tables: HashMap::new(),
            indexes_idxs: HashMap::new(),
            indexes: HashMap::new(),
            next_id: 0,
            functions: HashMap::new(),
        }
    }

    pub(super) fn add_table(
        &mut self,
        name: String,
        columns: Vec<ColumnCatalog>,
        ordered_pk_ids: Vec<ColumnId>,
    ) -> Result<TableId, CatalogError> {
        if self.table_idxs.contains_key(&name) {
            return Err(CatalogError::Duplicated("table", name));
        }
        let table_id = self.next_id;
        self.next_id += 1;
        let table_catalog = Arc::new(TableCatalog::new(
            table_id,
            name.clone(),
            columns,
            ordered_pk_ids,
        ));
        self.table_idxs.insert(name, table_id);
        self.tables.insert(table_id, table_catalog);
        Ok(table_id)
    }

    pub(super) fn add_index(
        &mut self,
        name: String,
        table_id: TableId,
        columns: Vec<ColumnId>,
        index_type: &IndexType,
    ) -> Result<IndexId, CatalogError> {
        if self.indexes_idxs.contains_key(&name) {
            return Err(CatalogError::Duplicated("index", name));
        }
        let index_id = self.next_id;
        self.next_id += 1;
        let index_catalog = Arc::new(IndexCatalog::new(
            index_id,
            name.clone(),
            table_id,
            columns,
            index_type.clone(),
        ));
        self.indexes_idxs.insert(name, index_id);
        self.indexes.insert(index_id, index_catalog);
        Ok(index_id)
    }

    pub(super) fn add_view(
        &mut self,
        name: String,
        columns: Vec<ColumnCatalog>,
        query: RecExpr,
    ) -> Result<TableId, CatalogError> {
        if self.table_idxs.contains_key(&name) {
            return Err(CatalogError::Duplicated("view", name));
        }
        let table_id = self.next_id;
        self.next_id += 1;
        let table_catalog = Arc::new(TableCatalog::new_view(
            table_id,
            name.clone(),
            columns,
            query,
        ));
        self.table_idxs.insert(name, table_id);
        self.tables.insert(table_id, table_catalog);
        Ok(table_id)
    }

    pub(super) fn delete_table(&mut self, id: TableId) {
        let catalog = self.tables.remove(&id).unwrap();
        self.table_idxs.remove(catalog.name()).unwrap();
    }

    pub fn all_tables(&self) -> HashMap<TableId, Arc<TableCatalog>> {
        self.tables.clone()
    }

    pub fn all_indexes(&self) -> HashMap<IndexId, Arc<IndexCatalog>> {
        self.indexes.clone()
    }

    pub fn get_table_id_by_name(&self, name: &str) -> Option<TableId> {
        self.table_idxs.get(name).cloned()
    }

    pub fn get_table_by_id(&self, table_id: TableId) -> Option<Arc<TableCatalog>> {
        self.tables.get(&table_id).cloned()
    }

    pub fn get_indexes_on_table(&self, table_id: TableId) -> Vec<IndexId> {
        self.indexes
            .iter()
            .filter(|(_, index)| index.table_id() == table_id)
            .map(|(id, _)| *id)
            .collect()
    }

    pub fn get_index_by_id(&self, index_id: IndexId) -> Option<Arc<IndexCatalog>> {
        self.indexes.get(&index_id).cloned()
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<Arc<TableCatalog>> {
        self.table_idxs
            .get(name)
            .and_then(|id| self.tables.get(id))
            .cloned()
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn id(&self) -> SchemaId {
        self.id
    }

    pub fn get_function_by_name(&self, name: &str) -> Option<Arc<FunctionCatalog>> {
        self.functions.get(name).cloned()
    }

    pub fn create_function(
        &mut self,
        name: String,
        arg_types: Vec<DataType>,
        arg_names: Vec<String>,
        return_type: DataType,
        language: String,
        body: String,
    ) {
        self.functions.insert(
            name.clone(),
            Arc::new(FunctionCatalog {
                name: name.clone(),
                arg_types,
                arg_names,
                return_type,
                language,
                body,
            }),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_catalog() {
        // column
        let col0 = ColumnCatalog::new(0, ColumnDesc::new("a", DataType::Int32, false));
        let col1 = ColumnCatalog::new(1, ColumnDesc::new("b", DataType::Bool, false));
        let col_catalogs = vec![col0, col1];

        // schema
        let mut schema_catalog = SchemaCatalog::new(0, "test".into());
        assert_eq!(schema_catalog.id(), 0);
        assert_eq!(schema_catalog.name(), "test");

        let table_id = schema_catalog
            .add_table("t".into(), col_catalogs, vec![])
            .unwrap();
        assert_eq!(table_id, 0);

        let table_catalog = schema_catalog.get_table_by_id(0).unwrap();
        assert!(!table_catalog.contains_column("c"));
        assert!(table_catalog.contains_column("a"));
        assert!(table_catalog.contains_column("b"))
    }
}
