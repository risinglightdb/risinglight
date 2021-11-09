use std::path::Path;
use std::sync::Arc;

use crate::array::{ArrayBuilderImpl, ArrayImplBuilderPickExt, ArrayImplSortExt, DataChunk};
use crate::catalog::{find_sort_key_id, ColumnCatalog};
use crate::storage::StorageResult;
use itertools::Itertools;

use super::rowset_builder::RowsetBuilder;
use crate::storage::secondary::ColumnBuilderOptions;

pub struct SecondaryMemRowset {
    columns: Arc<[ColumnCatalog]>,
    builders: Vec<ArrayBuilderImpl>,
}

impl SecondaryMemRowset {
    pub fn new(columns: Arc<[ColumnCatalog]>) -> Self {
        Self {
            builders: columns
                .iter()
                .map(|column| ArrayBuilderImpl::new(column.desc().datatype()))
                .collect_vec(),
            columns,
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
    pub async fn flush(
        self,
        directory: impl AsRef<Path>,
        column_options: ColumnBuilderOptions,
    ) -> StorageResult<()> {
        let chunk = if let Some(sort_key_idx) = find_sort_key_id(&*self.columns) {
            let arrays = self
                .builders
                .into_iter()
                .map(|builder| builder.finish())
                .collect_vec();
            let sorted_index = arrays[sort_key_idx].get_sorted_indices();
            arrays
                .into_iter()
                .map(|array| {
                    let mut builder = ArrayBuilderImpl::from_type_of_array(&array);
                    builder.pick_from(&array, &sorted_index);
                    builder.finish()
                })
                .collect::<DataChunk>()
        } else {
            self.builders
                .into_iter()
                .map(|builder| builder.finish())
                .collect::<DataChunk>()
        };

        let directory = directory.as_ref().to_path_buf();
        let mut builder = RowsetBuilder::new(self.columns, &directory, column_options);
        builder.append(chunk);
        builder.finish_and_flush().await?;
        // TODO(chi): do not reload index from disk, we can directly fetch it from cache.
        Ok(())
    }
}
