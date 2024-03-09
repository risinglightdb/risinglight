// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::binder::{CreateMView, CreateTable};
use crate::catalog::{ColumnCatalog, ColumnDesc};
use crate::storage::Storage;
use crate::streaming::StreamManager;

/// The executor of `create table` statement.
pub struct CreateTableExecutor<S: Storage> {
    pub plan: CreateTable,
    pub storage: Arc<S>,
    pub stream: Arc<StreamManager>,
}

impl<S: Storage> CreateTableExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let id = self
            .storage
            .create_table(
                self.plan.schema_id,
                &self.plan.table_name,
                &self.plan.columns,
                &self.plan.ordered_pk_ids,
            )
            .await?;

        // if self.plan.with.contains_key("connector") {
        //     self.stream.create_source(id, &self.plan.with).await?;
        // }

        let chunk = DataChunk::single(1);
        yield chunk
    }
}

/// The executor of `create materialized view` statement.
pub struct CreateMViewExecutor<S: Storage> {
    pub args: CreateMView,
    pub column_types: Vec<DataType>,
    pub query: RecExpr,
    pub storage: Arc<S>,
    pub stream: Arc<StreamManager>,
}

impl<S: Storage> CreateMViewExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let column_descs = self
            .column_types
            .into_iter()
            .enumerate()
            .map(|(i, ty)| {
                ColumnCatalog::new(
                    i as u32,
                    ColumnDesc::new(
                        ty,
                        format!("col{}", i), // TODO: use name defined by `as`
                        false,
                    ),
                )
            })
            .collect::<Vec<_>>();
        let id = self
            .storage
            .create_table(self.args.schema_id, &self.args.name, &column_descs, &[])
            .await?;
        // self.stream.create_mview(id, self.query)?;
        yield DataChunk::single(1);
    }
}
