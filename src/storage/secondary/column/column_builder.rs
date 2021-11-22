use risinglight_proto::rowset::BlockIndex;

use super::super::ColumnBuilderOptions;
use super::char_column_builder::CharColumnBuilder;
use super::{BoolColumnBuilder, ColumnBuilder};
use crate::array::ArrayImpl;
use crate::types::{DataType, DataTypeKind};

use super::primitive_column_builder::{F64ColumnBuilder, I32ColumnBuilder};

/// [`ColumnBuilder`] of all types
pub enum ColumnBuilderImpl {
    Int32(I32ColumnBuilder),
    Float64(F64ColumnBuilder),
    Bool(BoolColumnBuilder),
    UTF8(CharColumnBuilder),
}

impl ColumnBuilderImpl {
    pub fn new_from_datatype(datatype: &DataType, options: ColumnBuilderOptions) -> Self {
        match datatype.kind() {
            DataTypeKind::Int(_) => {
                Self::Int32(I32ColumnBuilder::new(datatype.is_nullable(), options))
            }
            DataTypeKind::Boolean => {
                Self::Bool(BoolColumnBuilder::new(datatype.is_nullable(), options))
            }
            DataTypeKind::Float(_) | DataTypeKind::Double => {
                Self::Float64(F64ColumnBuilder::new(datatype.is_nullable(), options))
            }
            DataTypeKind::Char(char_width) => Self::UTF8(CharColumnBuilder::new(
                datatype.is_nullable(),
                char_width,
                options,
            )),
            DataTypeKind::Varchar(_) => {
                // TODO: why varchar have char_width???
                Self::UTF8(CharColumnBuilder::new(
                    datatype.is_nullable(),
                    None,
                    options,
                ))
            }
            other_datatype => todo!("column builder for {:?} is not implemented", other_datatype),
        }
    }

    pub fn append(&mut self, array: &ArrayImpl) {
        match (self, array) {
            (Self::Int32(builder), ArrayImpl::Int32(array)) => builder.append(array),
            (Self::Bool(builder), ArrayImpl::Bool(array)) => builder.append(array),
            (Self::Float64(builder), ArrayImpl::Float64(array)) => builder.append(array),
            (Self::UTF8(builder), ArrayImpl::UTF8(array)) => builder.append(array),
            _ => todo!(),
        }
    }

    pub fn finish(self) -> (Vec<BlockIndex>, Vec<u8>) {
        match self {
            Self::Int32(builder) => builder.finish(),
            Self::Bool(builder) => builder.finish(),
            Self::Float64(builder) => builder.finish(),
            Self::UTF8(builder) => builder.finish(),
        }
    }
}
