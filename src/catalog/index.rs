// Copyright 2025 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;

/// The catalog of an index.
pub struct IndexCatalog {
    id: IndexId,
    name: String,
    table_id: TableId,
    column_idxs: Vec<ColumnId>,
}

impl IndexCatalog {
    pub fn new(id: IndexId, name: String, table_id: TableId, column_idxs: Vec<ColumnId>) -> Self {
        Self {
            id,
            name,
            table_id,
            column_idxs,
        }
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn column_idxs(&self) -> &[ColumnId] {
        &self.column_idxs
    }

    pub fn id(&self) -> IndexId {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
