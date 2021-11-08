use super::*;
use crate::array::{ArrayBuilder, ArrayBuilderImpl, DataChunk, I64ArrayBuilder};
use crate::physical_planner::PhysicalSeqScan;
use crate::storage::{Storage, StorageColumnRef, Table, Transaction, TxnIterator};
use itertools::Itertools;
use std::sync::Arc;

/// The executor of sequential scan operation.
pub struct SeqScanExecutor<S: Storage> {
    pub plan: PhysicalSeqScan,
    pub storage: Arc<S>,
}

impl<S: Storage> SeqScanExecutor<S> {
    async fn execute_inner(self) -> Result<DataChunk, ExecutorError> {
        let table = self.storage.get_table(self.plan.table_ref_id)?;
        let col_descs = table.column_descs(&self.plan.column_ids)?;
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
        let mut builders = col_descs
            .iter()
            .map(|desc| ArrayBuilderImpl::new(desc.datatype()))
            .collect::<Vec<ArrayBuilderImpl>>();

        if self.plan.with_row_handler {
            builders.push(ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0)));
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
            if chunk.cardinality() != 0 {
                yield chunk;
            }
        }
    }
}
