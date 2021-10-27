use risinglight_proto::rowset::BlockIndex;

use crate::array::ArrayImpl;
use crate::storage::secondary::rowset::primitive_column_builder::BoolColumnBuilder;
use crate::storage::secondary::{ColumnBuilder, ColumnBuilderOptions};
use crate::types::{DataType, DataTypeKind};

use super::primitive_column_builder::{F64ColumnBuilder, I32ColumnBuilder};

/// [`ColumnBuilder`] of all types
pub enum ColumnBuilderImpl {
    Int32(I32ColumnBuilder),
    Float64(F64ColumnBuilder),
    Bool(BoolColumnBuilder),
}

impl ColumnBuilderImpl {
    pub fn new_from_datatype(datatype: &DataType, options: ColumnBuilderOptions) -> Self {
        match datatype.kind() {
            DataTypeKind::Int => Self::Int32(I32ColumnBuilder::new(options)),
            DataTypeKind::Boolean => Self::Bool(BoolColumnBuilder::new(options)),
            DataTypeKind::Float(_) => Self::Float64(F64ColumnBuilder::new(options)),
            _ => todo!(),
        }
    }

    pub fn append(&mut self, array: &ArrayImpl) {
        match (self, array) {
            (Self::Int32(builder), ArrayImpl::Int32(array)) => builder.append(array),
            (Self::Bool(builder), ArrayImpl::Bool(array)) => builder.append(array),
            (Self::Float64(builder), ArrayImpl::Float64(array)) => builder.append(array),
            _ => todo!(),
        }
    }

    pub fn finish(self) -> (Vec<BlockIndex>, Vec<u8>) {
        match self {
            Self::Int32(builder) => builder.finish(),
            Self::Bool(builder) => builder.finish(),
            Self::Float64(builder) => builder.finish(),
        }
    }
}
