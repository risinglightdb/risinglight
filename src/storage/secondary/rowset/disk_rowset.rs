use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use moka::future::Cache;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;

use super::super::{Block, BlockCacheKey, Column, ColumnIndex, ColumnSeekPosition, IOBackend};
use super::{path_of_data_column, path_of_index_column, RowSetIterator};
use crate::catalog::ColumnCatalog;
use crate::storage::secondary::column::ColumnReadableFile;
use crate::storage::secondary::DeleteVector;
use crate::storage::{StorageColumnRef, StorageResult};

/// Represents a column in Secondary.
///
/// [`DiskRowset`] contains all necessary information, e.g. column info, rowset location.
pub struct DiskRowset {
    column_infos: Arc<[ColumnCatalog]>,
    columns: Vec<Column>,
    rowset_id: u32,
}

impl DiskRowset {
    pub async fn open(
        directory: PathBuf,
        column_infos: Arc<[ColumnCatalog]>,
        block_cache: Cache<BlockCacheKey, Block>,
        rowset_id: u32,
        io_backend: IOBackend,
    ) -> StorageResult<Self> {
        let mut columns = vec![];

        for (id, column_info) in column_infos.iter().enumerate() {
            let file = OpenOptions::default()
                .read(true)
                .write(false)
                .open(path_of_data_column(&directory, column_info))
                .await?;

            let mut index = OpenOptions::default()
                .read(true)
                .write(false)
                .open(path_of_index_column(&directory, column_info))
                .await?;

            // TODO(chi): add an index cache later
            let mut index_content = vec![];
            index.read_to_end(&mut index_content).await?;

            let column = Column::new(
                ColumnIndex::from_bytes(&index_content),
                match io_backend {
                    IOBackend::NormalRead => {
                        ColumnReadableFile::NormalRead(Arc::new(Mutex::new(file.into_std().await)))
                    }
                    IOBackend::PositionedRead => {
                        ColumnReadableFile::PositionedRead(Arc::new(file.into_std().await))
                    }
                },
                block_cache.clone(),
                BlockCacheKey::default().rowset(rowset_id).column(id as u32),
            );
            columns.push(column);
        }

        Ok(Self {
            column_infos,
            columns,
            rowset_id,
        })
    }

    pub fn column(&self, storage_column_id: usize) -> Column {
        self.columns[storage_column_id].clone()
    }

    pub fn column_info(&self, storage_column_id: usize) -> &ColumnCatalog {
        &self.column_infos[storage_column_id]
    }

    pub fn rowset_id(&self) -> u32 {
        self.rowset_id
    }

    pub async fn iter(
        self: &Arc<Self>,
        column_refs: Arc<[StorageColumnRef]>,
        dvs: Vec<Arc<DeleteVector>>,
        seek_pos: ColumnSeekPosition,
    ) -> RowSetIterator {
        RowSetIterator::new(self.clone(), column_refs, dvs, seek_pos).await
    }

    pub fn on_disk_size(&self) -> u64 {
        self.columns
            .iter()
            .map(|x| x.on_disk_size())
            .sum1()
            .unwrap_or(0)
    }
}

#[cfg(test)]
pub mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::array::I32Array;
    use crate::storage::secondary::rowset::rowset_builder::RowsetBuilder;
    use crate::storage::secondary::ColumnBuilderOptions;
    use crate::types::{DataTypeExt, DataTypeKind};

    pub async fn helper_build_rowset(tempdir: &TempDir, nullable: bool, len: usize) -> DiskRowset {
        let columns = vec![
            ColumnCatalog::new(
                0,
                "v1".to_string(),
                if nullable {
                    DataTypeKind::Int(None).nullable().to_column()
                } else {
                    DataTypeKind::Int(None).not_null().to_column()
                },
            ),
            ColumnCatalog::new(
                1,
                "v2".to_string(),
                if nullable {
                    DataTypeKind::Int(None).nullable().to_column()
                } else {
                    DataTypeKind::Int(None).not_null().to_column()
                },
            ),
            ColumnCatalog::new(
                2,
                "v3".to_string(),
                if nullable {
                    DataTypeKind::Int(None).nullable().to_column()
                } else {
                    DataTypeKind::Int(None).not_null().to_column()
                },
            ),
        ];

        let mut builder = RowsetBuilder::new(
            columns.clone().into(),
            tempdir.path(),
            ColumnBuilderOptions::default_for_test(),
        );

        for _ in 0..100 {
            builder.append(
                [
                    I32Array::from_iter([1, 2, 3].iter().cycle().cloned().take(len).map(Some))
                        .into(),
                    I32Array::from_iter(
                        [1, 3, 5, 7, 9].iter().cycle().cloned().take(len).map(Some),
                    )
                    .into(),
                    I32Array::from_iter(
                        [2, 3, 3, 3, 3, 3, 3]
                            .iter()
                            .cycle()
                            .cloned()
                            .take(len)
                            .map(Some),
                    )
                    .into(),
                ]
                .into_iter()
                .collect(),
            )
        }

        builder.finish_and_flush().await.unwrap();

        DiskRowset::open(
            tempdir.path().to_path_buf(),
            columns.into(),
            Cache::new(2333),
            0,
            IOBackend::NormalRead,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_get_block() {
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = helper_build_rowset(&tempdir, true, 1000).await;
        let column = rowset.column(0);
        column.get_block(0).await;
    }
}
