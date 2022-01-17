use std::cmp::Ordering;
use std::path::Path;
use std::sync::Arc;

use btreemultimap::BTreeMultiMap;
use itertools::Itertools;

use super::rowset_builder::RowsetBuilder;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::catalog::{find_sort_key_id, ColumnCatalog};
use crate::storage::secondary::ColumnBuilderOptions;
use crate::storage::StorageResult;
use crate::types::{DataValue, Row};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComparableDataValue(DataValue);

impl PartialOrd for ComparableDataValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for ComparableDataValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub trait MemTable {
    /// add data to memory table
    fn append(&mut self, columns: DataChunk) -> StorageResult<()>;

    /// flush data to [`DataChunk`]
    fn flush(self) -> StorageResult<DataChunk>;
}

pub struct BTreeMapMemTable {
    columns: Arc<[ColumnCatalog]>,
    primary_key_idx: usize,
    multi_btree_map: BTreeMultiMap<ComparableDataValue, Row>,
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
                ComparableDataValue(columns.array_at(self.primary_key_idx).get(row_idx)),
                columns.get_row_by_idx(row_idx),
            );
        }
        Ok(())
    }

    /// flush row-format data ordered by primary key to [`DataChunk`]
    fn flush(self) -> StorageResult<DataChunk> {
        let mut builders = self
            .columns
            .iter()
            .map(|column| ArrayBuilderImpl::new(column.desc().datatype()))
            .collect_vec();
        for (_, row_vec) in self.multi_btree_map {
            for row in row_vec {
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

    fn flush(self) -> StorageResult<DataChunk> {
        Ok(self
            .builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<DataChunk>())
    }
}

pub struct SecondaryMemRowset<M: MemTable> {
    columns: Arc<[ColumnCatalog]>,
    mem_table: M,
}

impl<M: MemTable> SecondaryMemRowset<M> {
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

pub enum SecondaryMemRowsetImpl {
    BTree(SecondaryMemRowset<BTreeMapMemTable>),
    Column(SecondaryMemRowset<ColumnMemTable>),
}

impl SecondaryMemRowsetImpl {
    pub fn new(columns: Arc<[ColumnCatalog]>) -> Self {
        if let Some(sort_key_idx) = find_sort_key_id(&columns) {
            Self::BTree(SecondaryMemRowset::<BTreeMapMemTable> {
                columns: columns.clone(),
                mem_table: BTreeMapMemTable::new(columns, sort_key_idx),
            })
        } else {
            Self::Column(SecondaryMemRowset::<ColumnMemTable> {
                columns: columns.clone(),
                mem_table: ColumnMemTable::new(columns),
            })
        }
    }

    pub async fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        match self {
            Self::BTree(btree_table) => btree_table.append(columns).await,
            Self::Column(column_table) => column_table.append(columns).await,
        }
    }

    pub async fn flush(
        self,
        directory: impl AsRef<Path>,
        column_options: ColumnBuilderOptions,
    ) -> StorageResult<()> {
        match self {
            Self::BTree(btree_table) => btree_table.flush(directory, column_options).await,
            Self::Column(column_table) => column_table.flush(directory, column_options).await,
        }
    }
}
