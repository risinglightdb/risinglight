// Copyright 2025 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::catalog::{ColumnId, IndexId, SchemaId, TableId};

pub trait InMemoryIndex: 'static + Send + Sync {}

pub struct InMemoryIndexes {}

impl InMemoryIndexes {
    pub fn new() -> Self {
        Self {}
    }

    pub fn add_index(
        &self,
        schema_id: SchemaId,
        index_id: IndexId,
        table_id: TableId,
        column_idxs: &[ColumnId],
    ) {
        let _ = (schema_id, index_id, table_id, column_idxs);
    }

    pub fn get_index(
        &self,
        schema_id: SchemaId,
        index_id: IndexId,
    ) -> Option<Arc<dyn InMemoryIndex>> {
        let _ = (schema_id, index_id);
        None
    }
}
