// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::catalog::{RootCatalogRef, TableRefId};

/// The executor of `drop` statement.
pub struct DropExecutor {
    pub tables: Vec<TableRefId>,
    pub catalog: RootCatalogRef,
    pub storage: StorageRef,
}

impl DropExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        for table in self.tables {
            if self.catalog.get_table(&table).unwrap().is_view() {
                self.catalog.drop_table(table);
            } else {
                self.storage.drop_table(table).await?;
            }
        }
        yield DataChunk::single(1);
    }
}
