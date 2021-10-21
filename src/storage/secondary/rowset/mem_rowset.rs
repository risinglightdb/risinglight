use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::catalog::ColumnCatalog;
use crate::storage::StorageResult;
use itertools::Itertools;

pub struct SecondaryMemRowset {
    builders: Vec<ArrayBuilderImpl>,
}

impl SecondaryMemRowset {
    pub fn new(columns: &[ColumnCatalog]) -> Self {
        Self {
            builders: columns
                .iter()
                .map(|column| ArrayBuilderImpl::new(column.desc().datatype()))
                .collect_vec(),
        }
    }

    /// Add data to mem table.
    pub async fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        for idx in 0..columns.column_count() {
            self.builders[idx].append(columns.array_at(idx));
        }
        Ok(())
    }

    /// Flush memtable to disk and return a handler
    pub async fn flush(&mut self) -> StorageResult<DataChunk> {
        let arrays = self
            .builders
            .drain(..)
            .map(|builder| builder.finish())
            .collect_vec();
        // TODO: should sort before flushing
        Ok(DataChunk::builder().arrays(arrays.into()).build())
    }
}
