// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::BlockIndex;

use super::super::ColumnBuilderOptions;
use super::blob_column_builder::BlobColumnBuilder;
use super::char_column_builder::CharColumnBuilder;
use super::primitive_column_builder::{
    DateColumnBuilder, DecimalColumnBuilder, F64ColumnBuilder, I32ColumnBuilder, I64ColumnBuilder,
};
use super::{BoolColumnBuilder, ColumnBuilder};
use crate::array::ArrayImpl;
use crate::storage::secondary::column::IntervalColumnBuilder;
use crate::types::{DataType, DataTypeKind};

/// [`ColumnBuilder`] of all types
pub enum ColumnBuilderImpl {
    Int32(I32ColumnBuilder),
    Int64(I64ColumnBuilder),
    Float64(F64ColumnBuilder),
    Bool(BoolColumnBuilder),
    Utf8(CharColumnBuilder),
    Decimal(DecimalColumnBuilder),
    Date(DateColumnBuilder),
    Interval(IntervalColumnBuilder),
    Blob(BlobColumnBuilder),
}

impl ColumnBuilderImpl {
    pub fn new_from_datatype(datatype: &DataType, options: ColumnBuilderOptions) -> Self {
        match datatype.kind() {
            DataTypeKind::Int(_) => {
                Self::Int32(I32ColumnBuilder::new(datatype.is_nullable(), options))
            }
            DataTypeKind::BigInt(_) => {
                Self::Int64(I64ColumnBuilder::new(datatype.is_nullable(), options))
            }
            DataTypeKind::Boolean => {
                Self::Bool(BoolColumnBuilder::new(datatype.is_nullable(), options))
            }
            DataTypeKind::Float(_) | DataTypeKind::Double => {
                Self::Float64(F64ColumnBuilder::new(datatype.is_nullable(), options))
            }
            DataTypeKind::Char(char_width) => Self::Utf8(CharColumnBuilder::new(
                datatype.is_nullable(),
                char_width,
                options,
            )),
            DataTypeKind::Varchar(_) => {
                // TODO: why varchar have char_width???
                Self::Utf8(CharColumnBuilder::new(
                    datatype.is_nullable(),
                    None,
                    options,
                ))
            }
            DataTypeKind::Decimal(_, _) => {
                Self::Decimal(DecimalColumnBuilder::new(datatype.is_nullable(), options))
            }
            DataTypeKind::Date => {
                Self::Date(DateColumnBuilder::new(datatype.is_nullable(), options))
            }
            DataTypeKind::Interval => {
                Self::Interval(IntervalColumnBuilder::new(datatype.is_nullable(), options))
            }
            DataTypeKind::Bytea => Self::Blob(BlobColumnBuilder::new(options)),
            other_datatype => todo!("column builder for {:?} is not implemented", other_datatype),
        }
    }

    pub fn append(&mut self, array: &ArrayImpl) {
        match (self, array) {
            (Self::Int32(builder), ArrayImpl::Int32(array)) => builder.append(array),
            (Self::Int64(builder), ArrayImpl::Int64(array)) => builder.append(array),
            (Self::Bool(builder), ArrayImpl::Bool(array)) => builder.append(array),
            (Self::Float64(builder), ArrayImpl::Float64(array)) => builder.append(array),
            (Self::Utf8(builder), ArrayImpl::Utf8(array)) => builder.append(array),
            (Self::Decimal(builder), ArrayImpl::Decimal(array)) => builder.append(array),
            (Self::Date(builder), ArrayImpl::Date(array)) => builder.append(array),
            (Self::Interval(builder), ArrayImpl::Interval(array)) => builder.append(array),
            (Self::Blob(builder), ArrayImpl::Blob(array)) => builder.append(array),
            _ => todo!(),
        }
    }

    pub fn finish(self) -> (Vec<BlockIndex>, Vec<u8>) {
        match self {
            Self::Int32(builder) => builder.finish(),
            Self::Int64(builder) => builder.finish(),
            Self::Bool(builder) => builder.finish(),
            Self::Float64(builder) => builder.finish(),
            Self::Utf8(builder) => builder.finish(),
            Self::Decimal(builder) => builder.finish(),
            Self::Date(builder) => builder.finish(),
            Self::Interval(builder) => builder.finish(),
            Self::Blob(builder) => builder.finish(),
        }
    }
}
