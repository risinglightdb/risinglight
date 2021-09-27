use super::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl, DataChunk};
use crate::physical_plan::PhysicalSeqScan;
use crate::storage::StorageRef;

pub struct SeqScanExecutor {
    pub plan: PhysicalSeqScan,
    pub storage: StorageRef,
}

impl SeqScanExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            let table = self.storage.get_table(self.plan.table_ref_id)?;
            let col_descs = table.column_descs(&self.plan.column_ids)?;
            // Get n array builders
            let mut builders = col_descs
                .iter()
                .map(|desc| ArrayBuilderImpl::new(desc.datatype().clone()))
                .collect::<Vec<ArrayBuilderImpl>>();

            let chunks = table.get_all_chunks()?;
            let mut cardinality: usize = 0;
            // Notice: The column ids may not be ordered.
            for chunk in chunks {
                cardinality += chunk.cardinality();

                for (idx, column_id) in self.plan.column_ids.iter().enumerate() {
                    // For idx-th builder, we need column_id-th array in the chunk
                    builders[idx].append(chunk.array_at(*column_id as usize));
                }
            }
            let arrays = builders
                .into_iter()
                .map(|builder| builder.finish())
                .collect::<Vec<ArrayImpl>>();
            yield DataChunk::builder()
                .cardinality(cardinality)
                .arrays(arrays.into())
                .build();
        }
    }
}
