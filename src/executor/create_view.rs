// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::binder::CreateTable;
use crate::catalog::RootCatalogRef;

/// The executor of `create view` statement.
pub struct CreateViewExecutor {
    pub table: Box<CreateTable>,
    pub query: RecExpr,
    pub catalog: RootCatalogRef,
}

impl CreateViewExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        self.catalog.add_view(
            self.table.schema_id,
            self.table.table_name,
            self.table.columns,
            self.query,
        )?;

        yield DataChunk::single(1);
    }
}
