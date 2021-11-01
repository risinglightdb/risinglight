use moka::future::Cache;
use risinglight_proto::rowset::BlockIndex;
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

        for column_info in column_infos.iter() {
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
                BlockCacheKey::default()
                    .rowset(rowset_id)
                    .column(column_info.id() as usize),
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
}
