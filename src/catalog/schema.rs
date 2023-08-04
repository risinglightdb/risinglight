// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use super::function::FunctionCatalog;
use super::*;

/// The catalog of a schema.
#[derive(Clone)]
pub struct SchemaCatalog {
    id: SchemaId,
    name: String,
    table_idxs: HashMap<String, TableId>,
    tables: HashMap<TableId, Arc<TableCatalog>>,
    next_table_id: TableId,
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
            next_table_id: 0,
            functions: HashMap::new(),
        }
    }

    pub(super) fn add_table(
        &mut self,
        name: String,
        columns: Vec<ColumnCatalog>,
        is_materialized_view: bool,
        ordered_pk_ids: Vec<ColumnId>,
    ) -> Result<TableId, CatalogError> {
        if self.table_idxs.contains_key(&name) {
            return Err(CatalogError::Duplicated("table", name));
        }
        let table_id = self.next_table_id;
        self.next_table_id += 1;
        let table_catalog = Arc::new(TableCatalog::new(
            table_id,
            name.clone(),
            columns,
            is_materialized_view,
            ordered_pk_ids,
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

    pub fn get_table_id_by_name(&self, name: &str) -> Option<TableId> {
        self.table_idxs.get(name).cloned()
    }

    pub fn get_table_by_id(&self, table_id: TableId) -> Option<Arc<TableCatalog>> {
        self.tables.get(&table_id).cloned()
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
        let col0 = ColumnCatalog::new(0, DataTypeKind::Int32.not_null().to_column("a".into()));
        let col1 = ColumnCatalog::new(1, DataTypeKind::Bool.not_null().to_column("b".into()));
        let col_catalogs = vec![col0, col1];

        // schema
        let mut schema_catalog = SchemaCatalog::new(0, "test".into());
        assert_eq!(schema_catalog.id(), 0);
        assert_eq!(schema_catalog.name(), "test");

        let table_id = schema_catalog
            .add_table("t".into(), col_catalogs, false, vec![])
            .unwrap();
        assert_eq!(table_id, 0);

        let table_catalog = schema_catalog.get_table_by_id(0).unwrap();
        assert!(!table_catalog.contains_column("c"));
        assert!(table_catalog.contains_column("a"));
        assert!(table_catalog.contains_column("b"))
    }
}
