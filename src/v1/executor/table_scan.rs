// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::array::{ArrayBuilder, ArrayBuilderImpl, DataChunk, I64ArrayBuilder};
use crate::storage::{ScanOptions, Storage, StorageColumnRef, Table, Transaction, TxnIterator};
use crate::v1::binder::BoundExpr;
use crate::v1::optimizer::plan_nodes::PhysicalTableScan;

/// The executor of table scan operation.
pub struct TableScanExecutor<S: Storage> {
    pub plan: PhysicalTableScan,
    pub expr: Option<BoundExpr>,
    pub storage: Arc<S>,
}

impl<S: Storage> TableScanExecutor<S> {
    /// Some executors will fail if no chunk is returned from `SeqScanExecutor`. After we have
    /// schema information in executors, this function can be removed.
    fn build_empty_chunk(&self, table: &impl Table) -> Result<DataChunk, ExecutorError> {
        let columns = table.columns()?;
        let mut col_idx = self
            .plan
            .logical()
            .column_ids()
            .iter()
            .map(|x| StorageColumnRef::Idx(*x))
            .collect_vec();

        // Add an extra column for RowHandler at the end
        if self.plan.logical().with_row_handler() {
            col_idx.push(StorageColumnRef::RowHandler);
        }

        // Get n array builders
        let mut builders = self
            .plan
            .logical()
            .column_ids()
            .iter()
            .map(|&id| columns.iter().find(|col| col.id() == id).unwrap())
            .map(|col| ArrayBuilderImpl::new(&col.datatype()))
            .collect::<Vec<ArrayBuilderImpl>>();

        if self.plan.logical().with_row_handler() {
            builders.push(ArrayBuilderImpl::Int64(I64ArrayBuilder::new()));
        }

        let chunk = builders.into_iter().collect();

        Ok(chunk)
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute_inner(self) {
        let table = self.storage.get_table(self.plan.logical().table_ref_id())?;

        // TODO: remove this when we have schema
        let empty_chunk = self.build_empty_chunk(&table)?;

        let mut col_idx = self
            .plan
            .logical()
            .column_ids()
            .iter()
            .map(|x| StorageColumnRef::Idx(*x))
            .collect_vec();

        // Add an extra column for RowHandler at the end
        if self.plan.logical().with_row_handler() {
            col_idx.push(StorageColumnRef::RowHandler);
        }

        let txn = table.read().await?;

        let mut it = txn
            .scan(
                &col_idx,
                ScanOptions::default()
                    .with_filter_opt(self.expr)
                    .with_sorted(self.plan.logical().is_sorted()),
            )
            .await?;

        let mut have_chunk = false;
        while let Some(x) = it.next_batch(None).await? {
            yield x;
            have_chunk = true;
        }
        if !have_chunk {
            yield empty_chunk;
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        // Buffer at most 128 chunks in memory
        let (tx, mut rx) = tokio::sync::mpsc::channel(128);
        // # Cancellation
        // When this stream is dropped, the `rx` is dropped, the spawned task will fail to send to
        // `tx`, then the task will finish.
        let handler = tokio::spawn(async move {
            let mut stream = self.execute_inner();
            while let Some(result) = stream.next().await {
                if tx.send(result).await.is_err() {
                    // the receiver is dropped due to the task is cancelled
                    return;
                }
            }
        });

        while let Some(item) = rx.recv().await {
            yield item?;
        }
        handler.await.expect("failed to join scan thread");
    }
}
