use risinglight_proto::rowset::BlockIndex;

use crate::array::ArrayImpl;
use crate::storage::secondary::ColumnBuilder;
use crate::types::{DataType, DataTypeKind};

use super::primitive_column_builder::{ColumnBuilderOptions, I32ColumnBuilder};

/// [`ColumnBuilder`] of all types
pub enum ColumnBuilderImpl {
    Int32(I32ColumnBuilder),
}

impl ColumnBuilderImpl {
    pub fn new_from_datatype(datatype: &DataType, options: ColumnBuilderOptions) -> Self {
        match datatype.kind() {
            DataTypeKind::Int => Self::Int32(I32ColumnBuilder::new(options)),
            _ => todo!(),
        }
    }

    pub fn append(&mut self, array: &ArrayImpl) {
        match (self, array) {
            (Self::Int32(builder), ArrayImpl::Int32(array)) => builder.append(array),
            _ => todo!(),
        }
    }

    pub fn finish(self) -> (Vec<BlockIndex>, Vec<u8>) {
        match self {
            Self::Int32(builder) => builder.finish(),
        }
    }
}
