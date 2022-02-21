// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::{Path, PathBuf};

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::catalog::ColumnCatalog;
use crate::storage::secondary::rowset::EncodedRowset;
use crate::storage::StorageResult;

pub fn path_of_data_column(base: impl AsRef<Path>, column_info: &ColumnCatalog) -> PathBuf {
    path_of_column(base, column_info, ".col")
}

pub fn path_of_index_column(base: impl AsRef<Path>, column_info: &ColumnCatalog) -> PathBuf {
    path_of_column(base, column_info, ".idx")
}

pub fn path_of_column(
    base: impl AsRef<Path>,
    column_info: &ColumnCatalog,
    suffix: &str,
) -> PathBuf {
    base.as_ref()
        .join(format!("{}{}", column_info.id(), suffix))
}

/// Rowset writer.
pub struct RowsetWriter {
    /// Directory of the rowset.
    directory: PathBuf,
}

impl RowsetWriter {
    pub fn new(directory: impl AsRef<Path>) -> Self {
        Self {
            directory: directory.as_ref().to_path_buf(),
        }
    }

    pub async fn create_dir(&self) -> StorageResult<()> {
        tokio::fs::create_dir(&self.directory)
            .await
            .map_err(|err| err.into())
    }

    async fn pipe_to_file(path: impl AsRef<Path>, data: Vec<u8>) -> StorageResult<()> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.as_ref())
            .await?;

        let mut writer = BufWriter::new(file);
        writer.write_all(&data).await?;
        writer.flush().await?;

        let file = writer.into_inner();
        file.sync_data().await?;

        Ok(())
    }

    async fn sync_dir(path: &impl AsRef<Path>) -> StorageResult<()> {
        File::open(path.as_ref()).await?.sync_data().await?;
        Ok(())
    }

    /// Flush rows in an encoded rowset. Create files if not exists.
    /// Panics if the rowset is empty.
    pub async fn flush(self, rowset: EncodedRowset) -> StorageResult<()> {
        // Panic on empty flush.
        if rowset.is_empty() {
            panic!("empty rowset")
        }

        for (column_info, column) in rowset.columns_info.iter().zip(rowset.columns) {
            Self::pipe_to_file(
                path_of_data_column(&self.directory, column_info),
                column.data,
            )
            .await?;
            Self::pipe_to_file(
                path_of_index_column(&self.directory, column_info),
                column.index,
            )
            .await?;
        }

        Self::sync_dir(&self.directory).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ArrayImpl;
    use crate::storage::secondary::rowset::RowsetBuilder;
    use crate::storage::secondary::ColumnBuilderOptions;
    use crate::types::{DataTypeExt, DataTypeKind};

    #[tokio::test]
    async fn test_rowset_flush() {
        let tempdir = tempfile::tempdir().unwrap();

        let mut builder = RowsetBuilder::new(
            vec![ColumnCatalog::new(
                0,
                DataTypeKind::Int(None)
                    .nullable()
                    .to_column("v1".to_string()),
            )]
            .into(),
            ColumnBuilderOptions::default_for_test(),
        );

        for _ in 0..1000 {
            builder.append(
                [ArrayImpl::Int32(
                    [1, 2, 3].into_iter().cycle().take(1000).collect(),
                )]
                .into_iter()
                .collect(),
            )
        }

        let writer = RowsetWriter::new(tempdir.path());
        writer.flush(builder.finish()).await.unwrap();
    }
}
