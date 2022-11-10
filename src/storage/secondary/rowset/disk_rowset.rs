// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use itertools::Itertools;
use moka::future::Cache;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;

use super::super::{Block, BlockCacheKey, Column, ColumnIndex, ColumnSeekPosition, IOBackend};
use super::{path_of_data_column, path_of_index_column, RowSetIterator};
use crate::catalog::ColumnCatalog;
use crate::storage::secondary::column::ColumnReadableFile;
use crate::storage::secondary::encode::PrimitiveFixedWidthEncode;
use crate::storage::secondary::DeleteVector;
use crate::storage::{StorageColumnRef, StorageResult};
use crate::types::DataValue;
use crate::v1::binder::BoundExpr;

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
            let path_of_index_column = path_of_index_column(&directory, column_info);

            let index_content = match &io_backend {
                IOBackend::NormalRead | IOBackend::PositionedRead => {
                    let mut index = OpenOptions::default()
                        .read(true)
                        .write(false)
                        .open(path_of_index_column)
                        .await?;

                    // TODO(chi): add an index cache later
                    let mut index_content = vec![];
                    index.read_to_end(&mut index_content).await?;
                    Bytes::from(index_content)
                }
                IOBackend::InMemory(map) => {
                    let guard = map.lock();
                    guard.get(&path_of_index_column).expect("not found").clone()
                }
            };

            let column_index = ColumnIndex::from_bytes(&index_content)?;

            let path_of_data_column = path_of_data_column(&directory, column_info);

            let column_file = match &io_backend {
                IOBackend::NormalRead => {
                    let file = OpenOptions::default()
                        .read(true)
                        .write(false)
                        .open(&path_of_data_column)
                        .await?;
                    ColumnReadableFile::NormalRead(Arc::new(Mutex::new(file.into_std().await)))
                }
                IOBackend::PositionedRead => {
                    let file = OpenOptions::default()
                        .read(true)
                        .write(false)
                        .open(&path_of_data_column)
                        .await?;
                    ColumnReadableFile::PositionedRead(Arc::new(file.into_std().await))
                }
                IOBackend::InMemory(map) => {
                    let guard = map.lock();
                    let file = guard.get(&path_of_data_column).expect("not found").clone();
                    ColumnReadableFile::InMemory(file)
                }
            };

            let column = Column::new(
                column_index,
                column_file,
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

    pub fn get_columns(&self) -> &[Column] {
        &self.columns
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
        begin_keys: &[DataValue],
        end_keys: &[DataValue],
    ) -> StorageResult<RowSetIterator> {
        RowSetIterator::new(
            self.clone(),
            column_refs,
            dvs,
            seek_pos,
            expr,
            begin_keys,
            end_keys,
        )
        .await
    }

    pub fn on_disk_size(&self) -> u64 {
        self.columns
            .iter()
            .map(|x| x.on_disk_size())
            .sum1()
            .unwrap_or(0)
    }

    /// Get the start row id to begin with for later table scanning.
    /// If `begin_keys` is empty, we return `ColumnSeekPosition::RowId(0)` to indicate scanning
    /// from the beginning, otherwise we scan the rowsets' first column indexes, find the first
    /// block who contains data greater than or equal to `begin_key` and return the row id of
    /// the block's first key. Currently, only the first column of the rowsets can be used to get
    /// the start row id and this column should be primary key.
    /// If `begin_key` is greater than all blocks' `first_key`, we return the `first_key` of the
    /// last block.
    /// Todo: support multi sort-keys range filter
    pub async fn start_rowid(&self, begin_keys: &[DataValue]) -> ColumnSeekPosition {
        if begin_keys.is_empty() {
            return ColumnSeekPosition::RowId(0);
        }

        // for now, we only use the first column to get the start row id, which means the length
        // of `begin_keys` can only be 0 or 1.
        let begin_key = begin_keys[0].borrow();
        let column = self.column(0);
        let column_index = column.index();

        let start_row_id = match *begin_key {
            DataValue::Int32(begin_val) => {
                let mut pre_block_first_key = 0;
                for index in column_index.indexes() {
                    let mut first_key: &[u8] = &index.first_key;
                    let first_val: i32 = PrimitiveFixedWidthEncode::decode(&mut first_key);

                    if first_val > begin_val {
                        break;
                    }
                    pre_block_first_key = index.first_rowid;
                }
                pre_block_first_key
            }
            // Todo: support ohter type
            _ => panic!("for now support range-filter scan by sort key type of int32"),
        };
        ColumnSeekPosition::RowId(start_row_id)
    }
}

#[cfg(test)]
pub mod tests {
    use std::borrow::Borrow;

    use tempfile::TempDir;

    use super::*;
    use crate::array::ArrayImpl;
    use crate::storage::secondary::rowset::rowset_builder::RowsetBuilder;
    use crate::storage::secondary::rowset::RowsetWriter;
    use crate::storage::secondary::{ColumnBuilderOptions, EncodeType};
    use crate::types::DataTypeKind;

    pub async fn helper_build_rowset(tempdir: &TempDir, nullable: bool, len: usize) -> DiskRowset {
        let columns = vec![
            ColumnCatalog::new(
                0,
                if nullable {
                    DataTypeKind::Int32.nullable().to_column("v1".to_string())
                } else {
                    DataTypeKind::Int32.not_null().to_column("v1".to_string())
                },
            ),
            ColumnCatalog::new(
                1,
                if nullable {
                    DataTypeKind::Int32.nullable().to_column("v2".to_string())
                } else {
                    DataTypeKind::Int32.not_null().to_column("v2".to_string())
                },
            ),
            ColumnCatalog::new(
                2,
                if nullable {
                    DataTypeKind::Int32.nullable().to_column("v3".to_string())
                } else {
                    DataTypeKind::Int32.not_null().to_column("v3".to_string())
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
                    ArrayImpl::new_int32([1, 2, 3].into_iter().cycle().take(len).collect()),
                    ArrayImpl::new_int32([1, 3, 5, 7, 9].into_iter().cycle().take(len).collect()),
                    ArrayImpl::new_int32(
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

        let backend = IOBackend::in_memory();

        let writer = RowsetWriter::new(tempdir.path(), backend.clone());
        writer.flush(builder.finish()).await.unwrap();

        DiskRowset::open(
            tempdir.path().to_path_buf(),
            columns.into(),
            Cache::new(2333),
            0,
            backend,
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
                DataTypeKind::Int32.nullable().to_column("v1".to_string())
            } else {
                DataTypeKind::Int32.not_null().to_column("v1".to_string())
            },
        )];
        let mut column_options = ColumnBuilderOptions::default_for_test();
        column_options.encode_type = EncodeType::RunLength;
        let mut builder = RowsetBuilder::new(columns.clone().into(), column_options);

        for _ in 0..100 {
            builder.append(
                [ArrayImpl::new_int32(
                    [1, 1, 2, 2, 2].into_iter().cycle().take(len).collect(),
                )]
                .into_iter()
                .collect(),
            )
        }

        let backend = IOBackend::in_memory();

        let writer = RowsetWriter::new(tempdir.path(), backend.clone());
        writer.flush(builder.finish()).await.unwrap();

        DiskRowset::open(
            tempdir.path().to_path_buf(),
            columns.into(),
            Cache::new(2333),
            0,
            backend,
        )
        .await
        .unwrap()
    }

    pub async fn helper_build_rowset_with_first_key_recorded(tempdir: &TempDir) -> DiskRowset {
        let columns = vec![
            ColumnCatalog::new(
                0,
                DataTypeKind::Int32
                    .not_null()
                    .to_column_primary_key("v1".to_string()),
            ),
            ColumnCatalog::new(
                1,
                DataTypeKind::Int32.not_null().to_column("v2".to_string()),
            ),
            ColumnCatalog::new(
                2,
                DataTypeKind::Int32.not_null().to_column("v3".to_string()),
            ),
        ];

        let mut builder = RowsetBuilder::new(
            columns.clone().into(),
            ColumnBuilderOptions::record_first_key_test(),
        );
        let mut key = 0;
        for _ in 0..10 {
            let mut array0 = vec![];
            let mut array1 = vec![];
            let mut array2 = vec![];
            for _ in 0..28 {
                array0.push(key);
                array1.push(key + 1);
                array2.push(key + 2);
                key += 1;
            }
            builder.append(
                [
                    ArrayImpl::new_int32(array0.clone().into_iter().collect()),
                    ArrayImpl::new_int32(array1.clone().into_iter().collect()),
                    ArrayImpl::new_int32(array2.clone().into_iter().collect()),
                ]
                .into_iter()
                .collect(),
            );
        }

        let backend = IOBackend::in_memory();

        let writer = RowsetWriter::new(tempdir.path(), backend.clone());
        writer.flush(builder.finish()).await.unwrap();

        DiskRowset::open(
            tempdir.path().to_path_buf(),
            columns.into(),
            Cache::new(2333),
            0,
            backend,
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

    #[tokio::test]
    async fn test_get_start_id() {
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = helper_build_rowset_with_first_key_recorded(&tempdir).await;
        let start_keys = vec![DataValue::Int32(222)];

        {
            let start_rid = match rowset.start_rowid(start_keys.borrow()).await {
                ColumnSeekPosition::RowId(x) => x,
                _ => panic!("Unable to reach the branch"),
            };
            assert_eq!(start_rid, 196_u32);
        }
        {
            let start_keys = vec![DataValue::Int32(10000)];
            let start_rid = match rowset.start_rowid(start_keys.borrow()).await {
                ColumnSeekPosition::RowId(x) => x,
                _ => panic!("Unable to reach the branch"),
            };
            assert_eq!(start_rid, 252_u32);
        }
    }
}
