use std::path::Path;
use std::sync::Arc;

use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::catalog::{find_sort_key_id, ColumnCatalog};
use crate::storage::StorageResult;
use crate::types::DataValue;
use itertools::Itertools;

use super::rowset_builder::RowsetBuilder;
use crate::storage::secondary::ColumnBuilderOptions;
use btreemultimap::BTreeMultiMap;

type Row = Vec<DataValue>;

pub trait MemTable {
    /// add data to memory table
    fn append(&mut self, columns: DataChunk) -> StorageResult<()>;

    /// flush data to DataChunk
    fn flush(self: Box<Self>) -> StorageResult<DataChunk>;
}

pub struct BTreeMapMemTable {
    columns: Arc<[ColumnCatalog]>,
    primary_key_idx: usize,
    multi_btree_map: BTreeMultiMap<DataValue, Row>,
}

impl BTreeMapMemTable {
    fn new(columns: Arc<[ColumnCatalog]>, primary_key_idx: usize) -> Self {
        Self {
            columns,
            primary_key_idx,
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

    /// flush row-format data ordered by primary key to DataChunk
    fn flush(self: Box<Self>) -> StorageResult<DataChunk> {
        let mut builders = self
            .columns
            .iter()
            .map(|column| ArrayBuilderImpl::new(column.desc().datatype()))
            .collect_vec();
        for (_, row_vec) in self.multi_btree_map.into_iter() {
            for row in row_vec.into_iter() {
                for idx in 0..self.columns.len() {
                    builders[idx].push(&row[idx]);
                }
            }
        }
        Ok(builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<DataChunk>())
    }
}

pub struct ColumnMemTable {
    builders: Vec<ArrayBuilderImpl>,
}

impl ColumnMemTable {
    pub fn new(columns: Arc<[ColumnCatalog]>) -> Self {
        Self {
            builders: columns
                .iter()
                .map(|column| ArrayBuilderImpl::new(column.desc().datatype()))
                .collect_vec(),
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

    fn flush(self: Box<Self>) -> StorageResult<DataChunk> {
        Ok(self
            .builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<DataChunk>())
    }
}

pub struct SecondaryMemRowset {
    columns: Arc<[ColumnCatalog]>,
    mem_table: Box<dyn MemTable + Send + Sync>,
}

impl SecondaryMemRowset {
    pub fn new(columns: Arc<[ColumnCatalog]>) -> Self {
        if let Some(sort_key_idx) = find_sort_key_id(&columns) {
            Self {
                columns: columns.clone(),
                mem_table: Box::new(BTreeMapMemTable::new(columns, sort_key_idx)),
            }
        } else {
            Self {
                columns: columns.clone(),
                mem_table: Box::new(ColumnMemTable::new(columns)),
            }
        }
    }

    /// Add data to memory table.
    pub async fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        self.mem_table.append(columns)
    }

    /// Flush memory table to disk and return a handler
    pub async fn flush(
        self,
        directory: impl AsRef<Path>,
        column_options: ColumnBuilderOptions,
    ) -> StorageResult<()> {
        let chunk = self.mem_table.flush()?;
        let directory = directory.as_ref().to_path_buf();
        let mut builder = RowsetBuilder::new(self.columns, &directory, column_options);
        builder.append(chunk);
        builder.finish_and_flush().await?;
        // TODO(chi): do not reload index from disk, we can directly fetch it from cache.
        Ok(())
    }
}
