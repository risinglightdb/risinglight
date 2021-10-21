use tokio::fs::File;
use std::path::{Path, PathBuf};

use crate::array::DataChunk;
use crate::catalog::{ColumnCatalog, ColumnDesc};
use crate::storage::StorageResult;

use super::primitive_column_builder::ColumnBuilderOptions;
use super::{ColumnBuilder, ColumnBuilderImpl};

use itertools::Itertools;

/// Builds a Rowset from [`DataChunk`].
pub struct RowsetBuilder {
    /// Column information
    columns: Vec<ColumnCatalog>,

    /// Column data builders
    builders: Vec<ColumnBuilderImpl>,

    /// Nullable column bitmap builders
    bitmap_builders: Vec<Option<ColumnBuilderImpl>>,

    /// Output directory of the rowset
    directory: PathBuf,
}

impl RowsetBuilder {
    pub fn new(
        columns: &[ColumnCatalog],
        directory: PathBuf,
        column_options: ColumnBuilderOptions,
    ) -> Self {
        Self {
            columns: columns.to_vec(),
            builders: columns
                .iter()
                .map(|column| {
                    ColumnBuilderImpl::new_from_datatype(&column.datatype(), column_options.clone())
                })
                .collect_vec(),
            bitmap_builders: columns.iter().map(|column| None).collect_vec(), // TODO(chi): add bitmap builder
            directory,
        }
    }

    pub fn append(&mut self, chunk: DataChunk) {
        for idx in 0..chunk.column_count() {
            self.builders[idx].append(chunk.array_at(idx));
            if let Some(_bitmap_builder) = &mut self.bitmap_builders[idx] {
                todo!()
            }
        }
    }

    fn _path_of_bitmap_index_column(base: impl AsRef<Path>, column_info: &ColumnCatalog) -> PathBuf {
        Self::path_of_column(base, column_info, "b.idx")
    }

    fn _path_of_bitmap_column(base: impl AsRef<Path>, column_info: &ColumnCatalog) -> PathBuf {
        Self::path_of_column(base, column_info, "b.col")
    }

    fn path_of_data_column(base: impl AsRef<Path>, column_info: &ColumnCatalog) -> PathBuf {
        Self::path_of_column(base, column_info, ".col")
    }

    fn _path_of_index_column(base: impl AsRef<Path>, column_info: &ColumnCatalog) -> PathBuf {
        Self::path_of_column(base, column_info, ".idx")
    }

    fn path_of_column(base: impl AsRef<Path>, column_info: &ColumnCatalog, suffix: &str) -> PathBuf {
        base.as_ref().join(format!("{}{}", column_info.id(), suffix))
    }

    pub fn finish(self) -> StorageResult<()> {
        for ((column_info, builder), bitmap_builder) in self
            .columns
            .into_iter()
            .zip(self.builders)
            .zip(self.bitmap_builders)
        {
            let (index, data) = builder.finish();

            File::with_options().write(true).create_new(true).open(Self::path_of_data_column(self.directory, column_info))?;

            if let Some(_bitmap_builder) = bitmap_builder {
                todo!()
            }
        }

        Ok(())
    }
}
