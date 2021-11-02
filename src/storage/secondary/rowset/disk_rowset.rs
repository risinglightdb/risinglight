use moka::future::Cache;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;

use crate::catalog::ColumnCatalog;
use crate::storage::secondary::rowset::rowset_builder::{
    path_of_data_column, path_of_index_column,
};
use crate::storage::StorageResult;

use super::column::Column;
use super::{Block, BlockCacheKey, ColumnIndex};

/// Represents a column in Secondary.
///
/// [`DiskRowset`] contains all necessary information, e.g. column info, rowset location.
#[allow(dead_code)]
pub struct DiskRowset {
    directory: PathBuf,
    column_infos: Arc<[ColumnCatalog]>,
    columns: Vec<Column>,
    block_cache: Cache<BlockCacheKey, Block>,
    base_block_key: BlockCacheKey,
}

impl DiskRowset {
    pub async fn open(
        directory: PathBuf,
        column_infos: Arc<[ColumnCatalog]>,
        block_cache: Cache<BlockCacheKey, Block>,
        rowset_id: usize,
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
                Arc::new(file.into_std().await),
                block_cache.clone(),
                BlockCacheKey::default().rowset(rowset_id).column(id),
            );
            columns.push(column);
        }

        Ok(Self {
            column_infos,
            columns,
            directory,
            block_cache,
            base_block_key: BlockCacheKey::default().rowset(rowset_id),
        })
    }

    #[allow(dead_code)]
    pub fn column(&self, storage_column_id: usize) -> Column {
        self.columns[storage_column_id].clone()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::array::I32Array;
    use crate::storage::secondary::rowset::rowset_builder::RowsetBuilder;
    use crate::storage::secondary::ColumnBuilderOptions;
    use crate::types::{DataTypeExt, DataTypeKind};

    use super::*;

    pub async fn helper_build_rowset(tempdir: &TempDir) -> DiskRowset {
        let columns = vec![ColumnCatalog::new(
            0,
            "v1".to_string(),
            DataTypeKind::Int.nullable().to_column(),
        )];
        let mut builder = RowsetBuilder::new(
            columns.clone().into(),
            tempdir.path(),
            ColumnBuilderOptions { target_size: 4096 },
        );

        for _ in 0..1000 {
            builder.append(
                [
                    I32Array::from_iter([1, 2, 3].iter().cycle().cloned().take(1000).map(Some))
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
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_get_block() {
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = helper_build_rowset(&tempdir).await;
        let column = rowset.column(0);
        column.get_block(0).await;
    }
}
