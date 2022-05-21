// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::*;
use crate::array::{ArrayBuilder, ArrayBuilderImpl, DataChunk, I64ArrayBuilder};
use crate::binder::BoundExpr;
use crate::optimizer::plan_nodes::PhysicalTableScan;
use crate::storage::{Storage, StorageColumnRef, Table, Transaction, TxnIterator};

/// The executor of table scan operation.
pub struct TableScanExecutor<S: Storage> {
    pub context: Arc<Context>,
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

    pub async fn execute_inner(
        self,
        tx: mpsc::Sender<DataChunk>,
        token: CancellationToken,
    ) -> Result<(), ExecutorError> {
        let table = self.storage.get_table(self.plan.logical().table_ref_id())?;

        // TODO: remove this when we have schema
        let empty_chunk = self.build_empty_chunk(&table)?;
        let mut have_chunk = false;

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

        if token.is_cancelled() {
            return Err(ExecutorError::Abort);
        }
        let txn = table.read().await?;

        let mut it = match unified_select_with_token(
            &token,
            txn.scan(
                &[],
                &[],
                &col_idx,
                self.plan.logical().is_sorted(),
                false,
                self.expr,
            ),
        )
        .await
        {
            Ok(it) => it,
            Err(err) => {
                txn.abort().await?;
                return Err(err);
            }
        };

        loop {
            let chunk = match select_with_token(&token, it.next_batch(None)).await {
                Ok(chunk) => chunk,
                Err(err) => {
                    txn.abort().await?;
                    return Err(err);
                }
            };
            match chunk {
                Ok(x) => {
                    if let Some(x) = x {
                        if tx.send(x).await.is_err() {
                            txn.abort().await?;
                            return Err(ExecutorError::Abort);
                        }
                        have_chunk = true;
                    } else {
                        break;
                    }
                }
                Err(err) => {
                    txn.abort().await?;
                    return Err(err.into());
                }
            }
        }

        txn.abort().await?;

        if !have_chunk && tx.send(empty_chunk).await.is_err() {
            return Err(ExecutorError::Abort);
        }

        Ok(())
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        // Buffer at most 128 chunks in memory
        let (tx, mut rx) = tokio::sync::mpsc::channel(128);

        let context = self.context.clone();
        match context.spawn(|token| async {
            let result = self.execute_inner(tx, token).await;
            if let Err(ExecutorError::Abort) = result {
                warn!("Abort!")
            }
            result
        }) {
            Some(handler) => {
                while let Some(item) = rx.recv().await {
                    yield item;
                }
                handler.await.expect("failed to join scan thread")?;
            }
            None => {
                return Err(ExecutorError::Abort);
            }
        }
    }
}
