use smallvec::SmallVec;
use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;

use crate::array::{ArrayBuilderImpl, ArrayImplBuilderPickExt, ArrayImplSortExt, DataChunk};
use crate::catalog::{find_sort_key_id, ColumnCatalog};
use crate::storage::StorageResult;
use crate::types::DataValue;
use itertools::Itertools;

use super::rowset_builder::RowsetBuilder;
use crate::storage::secondary::ColumnBuilderOptions;
use btreemultimap::BTreeMultiMap;

type Row = Vec<DataValue>;

pub trait MemTable {
    /// add data to row-format store
    fn append(&mut self, columns: DataChunk) -> StorageResult<()>;

    /// flush row-format data to DataChunk
    fn flush(&mut self) -> StorageResult<DataChunk>;
}

pub struct BTreeMapMemTable {
    primary_key_idx: usize,
    columns_meta: Arc<[ColumnCatalog]>,
    multi_btree_map: BTreeMultiMap<DataValue, Row>,
}

impl BTreeMapMemTable {
    fn new(primary_key_idx: usize, columns_meta: Arc<[ColumnCatalog]>) -> Self {
        Self {
            primary_key_idx,
            columns_meta,
            multi_btree_map: BTreeMultiMap::new(),
        }
    }
}

impl MemTable for BTreeMapMemTable {
    fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        for row_idx in 0..columns.cardinality() {
            self.multi_btree_map.insert(
                columns.array_at(self.primary_key_idx).get(row_idx),
                columns.get_row_by_idx(row_idx),
            );
        }
        Ok(())
    }

    fn flush(self) -> StorageResult<DataChunk> {

        todo!()
    }
}

impl BTreeMapMemTable {
    fn flush_by_order(&mut self) -> StorageResult<DataChunk> {
        todo!()
    }
}

pub struct ColumnMemTable {
    columns: Arc<[ColumnCatalog]>,
    builders: Vec<ArrayBuilderImpl>,
}

impl ColumnMemTable {
    pub fn new(columns: Arc<[ColumnCatalog]>) -> Self {
        Self {
            builders: columns
                .iter()
                .map(|column| ArrayBuilderImpl::new(column.desc().datatype()))
                .collect_vec(),
            columns,
        }
    }
}

impl MemTable for ColumnMemTable {
    fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        for idx in 0..columns.column_count() {
            self.builders[idx].append(columns.array_at(idx));
        }
        Ok(())
    }

    fn flush(&mut self) -> StorageResult<DataChunk> {
        Ok(self
            .builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<DataChunk>())
    }
}

pub struct SecondaryMemRowset {
    mem_table: Box<dyn MemTable>,
}

impl SecondaryMemRowset {
    pub fn new(columns: Arc<[ColumnCatalog]>) -> Self {
        if let Some(sort_key_idx) = find_sort_key_id(&columns) {
            Self {
                mem_table: Box::new(BTreeMapMemTable::new(sort_key_idx, columns)),
            }
        } else {
            Self {
                mem_table: Box::new(ColumnMemTable::new(columns)),
            }
        }
    }

    /// Add data to mem table.
    pub async fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        self.mem_table.append(columns)
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
