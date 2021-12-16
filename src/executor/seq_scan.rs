use std::sync::Arc;

use itertools::Itertools;

use super::*;
use crate::array::{ArrayBuilder, ArrayBuilderImpl, DataChunk, I64ArrayBuilder};
use crate::logical_optimizer::plan_nodes::PhysicalSeqScan;
use crate::storage::{Storage, StorageColumnRef, Table, Transaction, TxnIterator};

/// The executor of sequential scan operation.
pub struct SeqScanExecutor<S: Storage> {
    pub plan: PhysicalSeqScan,
    pub storage: Arc<S>,
}

impl<S: Storage> SeqScanExecutor<S> {
    async fn execute_inner(self) -> Result<DataChunk, ExecutorError> {
        let table = self.storage.get_table(self.plan.table_ref_id)?;
        let columns = table.columns()?;
        let mut col_idx = self
            .plan
            .column_ids
            .iter()
            .map(|x| StorageColumnRef::Idx(*x))
            .collect_vec();

        // Add an extra column for RowHandler at the end
        if self.plan.with_row_handler {
            col_idx.push(StorageColumnRef::RowHandler);
        }

        // Get n array builders
        let mut builders = self
            .plan
            .column_ids
            .iter()
            .map(|&id| columns.iter().find(|col| col.id() == id).unwrap())
            .map(|col| ArrayBuilderImpl::new(&col.datatype()))
            .collect::<Vec<ArrayBuilderImpl>>();

        if self.plan.with_row_handler {
            builders.push(ArrayBuilderImpl::Int64(I64ArrayBuilder::new()));
        }

        let txn = table.read().await?;
        let mut it = txn
            .scan(None, None, &col_idx, self.plan.is_sorted, false)
            .await?;

        // Notice: The column ids may not be ordered.
        while let Some(chunk) = it.next_batch(None).await? {
            for (idx, builder) in builders.iter_mut().enumerate() {
                builder.append(chunk.array_at(idx as usize));
            }
        }

        let chunk: DataChunk = builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect();

        txn.abort().await?;

        Ok(chunk)
    }
}

impl<S: Storage> SeqScanExecutor<S> {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            let chunk = self.execute_inner().await?;
            yield chunk;
        }
    }
}
