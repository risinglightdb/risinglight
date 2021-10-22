use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::array::DataChunk;
use crate::catalog::ColumnCatalog;
use crate::storage::secondary::ColumnBuilderOptions;
use crate::storage::StorageResult;

use super::primitive_column_builder::BoolColumnBuilder;
use super::{ColumnBuilder, ColumnBuilderImpl};

use itertools::Itertools;

/// Builds a Rowset from [`DataChunk`].
pub struct RowsetBuilder {
    /// Column information
    columns: Arc<[ColumnCatalog]>,

    /// Column data builders
    builders: Vec<ColumnBuilderImpl>,

    /// Nullable column bitmap builders
    bitmap_builders: Vec<Option<BoolColumnBuilder>>,

    /// Output directory of the rowset
    directory: PathBuf,
}

impl RowsetBuilder {
    pub fn new(
        columns: Arc<[ColumnCatalog]>,
        directory: impl AsRef<Path>,
        column_options: ColumnBuilderOptions,
    ) -> Self {
        Self {
            builders: columns
                .iter()
                .map(|column| {
                    ColumnBuilderImpl::new_from_datatype(&column.datatype(), column_options.clone())
                })
                .collect_vec(),
            bitmap_builders: columns
                .iter()
                .map(|column| {
                    if column.is_nullable() {
                        Some(BoolColumnBuilder::new(column_options.clone()))
                    } else {
                        None
                    }
                })
                .collect_vec(),
            directory: directory.as_ref().to_path_buf(),
            columns,
        }
    }

    pub fn append(&mut self, chunk: DataChunk) {
        for idx in 0..chunk.column_count() {
            self.builders[idx].append(chunk.array_at(idx));
            if let Some(_bitmap_builder) = &mut self.bitmap_builders[idx] {
                // TODO(chi): add Bitmap encoder and do append
                todo!();
                // bitmap_builder.append(chunk.array_at(idx).get_valid_bitmap())
            }
        }
    }

    fn _path_of_bitmap_index_column(
        base: impl AsRef<Path>,
        column_info: &ColumnCatalog,
    ) -> PathBuf {
        Self::path_of_column(base, column_info, "b.idx")
    }

    fn path_of_bitmap_column(base: impl AsRef<Path>, column_info: &ColumnCatalog) -> PathBuf {
        Self::path_of_column(base, column_info, "b.col")
    }

    fn path_of_data_column(base: impl AsRef<Path>, column_info: &ColumnCatalog) -> PathBuf {
        Self::path_of_column(base, column_info, ".col")
    }

    fn _path_of_index_column(base: impl AsRef<Path>, column_info: &ColumnCatalog) -> PathBuf {
        Self::path_of_column(base, column_info, ".idx")
    }

    fn path_of_column(
        base: impl AsRef<Path>,
        column_info: &ColumnCatalog,
        suffix: &str,
    ) -> PathBuf {
        base.as_ref()
            .join(format!("{}{}", column_info.id(), suffix))
    }

    async fn pipe_to_file(path: impl AsRef<Path>, data: Vec<u8>) -> StorageResult<()> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.as_ref())
            .await?;

        let mut writer = BufWriter::new(file);
        writer.write(&data).await?;
        writer.flush().await?;

        let file = writer.into_inner();
        file.sync_all().await?;

        Ok(())
    }

    pub async fn finish_and_flush(self) -> StorageResult<()> {
        for ((column_info, builder), bitmap_builder) in self
            .columns
            .iter()
            .zip(self.builders)
            .zip(self.bitmap_builders)
        {
            let (_index, data) = builder.finish();

            Self::pipe_to_file(
                Self::path_of_data_column(&self.directory, column_info),
                data,
            )
            .await?;

            // TODO(chi): flush index

            if let Some(bitmap_builder) = bitmap_builder {
                let (_index, data) = bitmap_builder.finish();

                // TODO(chi): flush index

                Self::pipe_to_file(
                    Self::path_of_bitmap_column(&self.directory, column_info),
                    data,
                )
                .await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::array::I32Array;
    use crate::types::{DataTypeExt, DataTypeKind};

    use super::*;

    #[tokio::test]
    async fn test_rowset_flush() {
        let tempdir = tempfile::tempdir().unwrap();

        let mut builder = RowsetBuilder::new(
            vec![ColumnCatalog::new(
                0,
                "v1".to_string(),
                DataTypeKind::Int.not_null().to_column(),
            )]
            .into(),
            tempdir.path(),
            ColumnBuilderOptions { target_size: 4096 },
        );

        for _ in 0..1000 {
            builder.append(
                DataChunk::builder()
                    .arrays(
                        vec![I32Array::from_iter(
                            [1, 2, 3].iter().cycle().cloned().take(1000).map(Some),
                        )
                        .into()]
                        .into(),
                    )
                    .build(),
            )
        }

        builder.finish_and_flush().await.unwrap();
    }
}
