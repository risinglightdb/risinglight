// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use itertools::Itertools;

use super::super::{ColumnBuilderImpl, IndexBuilder};
use crate::array::DataChunk;
use crate::catalog::ColumnCatalog;
use crate::storage::secondary::rowset::{EncodedColumn, EncodedRowset};
use crate::storage::secondary::ColumnBuilderOptions;

/// Builds a Rowset from [`DataChunk`].
pub struct RowsetBuilder {
    /// Column information
    columns: Arc<[ColumnCatalog]>,

    /// Column data builders
    builders: Vec<ColumnBuilderImpl>,

    /// Count of rows in this rowset
    row_cnt: u32,

    /// Column builder options
    column_options: ColumnBuilderOptions,
}

impl RowsetBuilder {
    pub fn new(columns: Arc<[ColumnCatalog]>, column_options: ColumnBuilderOptions) -> Self {
        Self {
            builders: columns
                .iter()
                .map(|column| {
                    ColumnBuilderImpl::new_from_datatype(&column.datatype(), column_options.clone())
                })
                .collect_vec(),
            columns,
            row_cnt: 0,
            column_options,
        }
    }

    pub fn append(&mut self, chunk: DataChunk) {
        self.row_cnt += chunk.cardinality() as u32;

        for idx in 0..chunk.column_count() {
            self.builders[idx].append(chunk.array_at(idx));
        }
    }

    pub fn finish(self) -> EncodedRowset {
        let checksum_type = self.column_options.checksum_type;
        EncodedRowset {
            size: self.row_cnt as usize,
            columns_info: self.columns.clone(),
            columns: self
                .builders
                .into_iter()
                .map(|builder| {
                    let (block_indices, data) = builder.finish();

                    let mut index_builder = IndexBuilder::new(checksum_type, block_indices.len());
                    for index in block_indices {
                        index_builder.append(index);
                    }

                    EncodedColumn {
                        index: index_builder.finish(),
                        data,
                    }
                })
                .collect_vec(),
        }
    }
}
