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
        use DataTypeKind::*;
        match datatype.kind() {
            Null => panic!("column type should not be null"),
            Int32 => Self::Int32(I32ColumnBuilder::new(datatype.nullable, options)),
            Int64 => Self::Int64(I64ColumnBuilder::new(datatype.nullable, options)),
            Bool => Self::Bool(BoolColumnBuilder::new(datatype.nullable, options)),
            Float64 => Self::Float64(F64ColumnBuilder::new(datatype.nullable, options)),
            String => Self::Utf8(CharColumnBuilder::new(datatype.nullable, None, options)),
            Decimal(_, _) => Self::Decimal(DecimalColumnBuilder::new(datatype.nullable, options)),
            Date => Self::Date(DateColumnBuilder::new(datatype.nullable, options)),
            Interval => Self::Interval(IntervalColumnBuilder::new(datatype.nullable, options)),
            Blob => Self::Blob(BlobColumnBuilder::new(datatype.nullable, options)),
            Struct(_) => todo!("struct column builder"),
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
