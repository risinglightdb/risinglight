use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk};
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
        let col_idx = self
            .plan
            .column_ids
            .iter()
            .map(|x| StorageColumnRef::Idx(*x))
            .collect_vec();
        // Get n array builders
        let mut builders = col_descs
            .iter()
            .map(|desc| ArrayBuilderImpl::new(desc.datatype().clone()))
            .collect::<Vec<ArrayBuilderImpl>>();

        let txn = table.read().await?;
        let mut it = txn.scan(None, None, &col_idx, false).await?;

        let mut cardinality: usize = 0;

        // Notice: The column ids may not be ordered.
        while let Some(chunk) = it.next_batch().await? {
            cardinality += chunk.cardinality();

            for (idx, builder) in builders.iter_mut().enumerate() {
                builder.append(chunk.array_at(idx as usize));
            }
        }

        let arrays = builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect_vec();

        txn.commit().await?;

        Ok(DataChunk::builder()
            .cardinality(cardinality)
            .arrays(arrays.into())
            .build())
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
