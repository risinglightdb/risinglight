// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use moka::future::Cache;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;

use super::super::{Block, BlockCacheKey, Column, ColumnIndex, ColumnSeekPosition, IOBackend};
use super::{path_of_data_column, path_of_index_column, RowSetIterator};
use crate::binder::BoundExpr;
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
                ColumnIndex::from_bytes(&index_content)?,
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
        expr: Option<BoundExpr>,
    ) -> StorageResult<RowSetIterator> {
        RowSetIterator::new(self.clone(), column_refs, dvs, seek_pos, expr).await
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
    use crate::array::ArrayImpl;
    use crate::storage::secondary::rowset::rowset_builder::RowsetBuilder;
    use crate::storage::secondary::rowset::RowsetWriter;
    use crate::storage::secondary::ColumnBuilderOptions;
    use crate::types::{DataTypeExt, DataTypeKind};

    pub async fn helper_build_rowset(tempdir: &TempDir, nullable: bool, len: usize) -> DiskRowset {
        let columns = vec![
            ColumnCatalog::new(
                0,
                if nullable {
                    DataTypeKind::Int(None)
                        .nullable()
                        .to_column("v1".to_string())
                } else {
                    DataTypeKind::Int(None)
                        .not_null()
                        .to_column("v1".to_string())
                },
            ),
            ColumnCatalog::new(
                1,
                if nullable {
                    DataTypeKind::Int(None)
                        .nullable()
                        .to_column("v2".to_string())
                } else {
                    DataTypeKind::Int(None)
                        .not_null()
                        .to_column("v2".to_string())
                },
            ),
            ColumnCatalog::new(
                2,
                if nullable {
                    DataTypeKind::Int(None)
                        .nullable()
                        .to_column("v3".to_string())
                } else {
                    DataTypeKind::Int(None)
                        .not_null()
                        .to_column("v3".to_string())
                },
            ),
        ];

        let mut builder = RowsetBuilder::new(
            columns.clone().into(),
            ColumnBuilderOptions::default_for_test(),
        );

        for _ in 0..100 {
            builder.append(
                [
                    ArrayImpl::Int32([1, 2, 3].into_iter().cycle().take(len).collect()),
                    ArrayImpl::Int32([1, 3, 5, 7, 9].into_iter().cycle().take(len).collect()),
                    ArrayImpl::Int32(
                        [2, 3, 3, 3, 3, 3, 3]
                            .into_iter()
                            .cycle()
                            .take(len)
                            .collect(),
                    ),
                ]
                .into_iter()
                .collect(),
            )
        }

        let writer = RowsetWriter::new(tempdir.path());
        writer.flush(builder.finish()).await.unwrap();

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

    pub async fn helper_build_rle_rowset(
        tempdir: &TempDir,
        nullable: bool,
        len: usize,
    ) -> DiskRowset {
        let columns = vec![ColumnCatalog::new(
            0,
            if nullable {
                DataTypeKind::Int(None)
                    .nullable()
                    .to_column("v1".to_string())
            } else {
                DataTypeKind::Int(None)
                    .not_null()
                    .to_column("v1".to_string())
            },
        )];
        let mut column_options = ColumnBuilderOptions::default_for_test();
        column_options.is_rle = true;
        let mut builder = RowsetBuilder::new(columns.clone().into(), column_options);

        for _ in 0..100 {
            builder.append(
                [ArrayImpl::Int32(
                    [1, 1, 2, 2, 2].into_iter().cycle().take(len).collect(),
                )]
                .into_iter()
                .collect(),
            )
        }

        let writer = RowsetWriter::new(tempdir.path());
        writer.flush(builder.finish()).await.unwrap();

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
        column.get_block(0).await.unwrap();
    }
}
