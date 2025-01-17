// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::BlockIndex;

use super::super::ColumnBuilderOptions;
use super::blob_column_builder::BlobColumnBuilder;
use super::char_column_builder::CharColumnBuilder;
use super::primitive_column_builder::{
    DateColumnBuilder, DecimalColumnBuilder, F64ColumnBuilder, I16ColumnBuilder, I32ColumnBuilder,
    I64ColumnBuilder,
};
use super::{BoolColumnBuilder, ColumnBuilder};
use crate::array::ArrayImpl;
use crate::storage::secondary::column::{
    IntervalColumnBuilder, TimestampColumnBuilder, TimestampTzColumnBuilder,
};
use crate::storage::secondary::VectorColumnBuilder;
use crate::types::DataType;

/// [`ColumnBuilder`] of all types
pub enum ColumnBuilderImpl {
    Int16(I16ColumnBuilder),
    Int32(I32ColumnBuilder),
    Int64(I64ColumnBuilder),
    Float64(F64ColumnBuilder),
    Bool(BoolColumnBuilder),
    String(CharColumnBuilder),
    Decimal(DecimalColumnBuilder),
    Date(DateColumnBuilder),
    Timestamp(TimestampColumnBuilder),
    TimestampTz(TimestampTzColumnBuilder),
    Interval(IntervalColumnBuilder),
    Blob(BlobColumnBuilder),
    Vector(VectorColumnBuilder),
}

impl ColumnBuilderImpl {
    pub fn new_from_datatype(
        datatype: &DataType,
        nullable: bool,
        options: ColumnBuilderOptions,
    ) -> Self {
        use DataType::*;
        match datatype {
            Null => panic!("column type should not be null"),
            Int16 => Self::Int16(I16ColumnBuilder::new(nullable, options)),
            Int32 => Self::Int32(I32ColumnBuilder::new(nullable, options)),
            Int64 => Self::Int64(I64ColumnBuilder::new(nullable, options)),
            Bool => Self::Bool(BoolColumnBuilder::new(nullable, options)),
            Float64 => Self::Float64(F64ColumnBuilder::new(nullable, options)),
            String => Self::String(CharColumnBuilder::new(nullable, None, options)),
            Decimal(_, _) => Self::Decimal(DecimalColumnBuilder::new(nullable, options)),
            Date => Self::Date(DateColumnBuilder::new(nullable, options)),
            Timestamp => Self::Timestamp(TimestampColumnBuilder::new(nullable, options)),
            TimestampTz => Self::TimestampTz(TimestampTzColumnBuilder::new(nullable, options)),
            Interval => Self::Interval(IntervalColumnBuilder::new(nullable, options)),
            Blob => Self::Blob(BlobColumnBuilder::new(nullable, options)),
            Vector(_) => Self::Vector(VectorColumnBuilder::new(nullable, options)),
            Struct(_) => todo!("struct column builder"),
        }
    }

    pub fn append(&mut self, array: &ArrayImpl) {
        match (self, array) {
            (Self::Int16(builder), ArrayImpl::Int16(array)) => builder.append(array),
            (Self::Int32(builder), ArrayImpl::Int32(array)) => builder.append(array),
            (Self::Int64(builder), ArrayImpl::Int64(array)) => builder.append(array),
            (Self::Bool(builder), ArrayImpl::Bool(array)) => builder.append(array),
            (Self::Float64(builder), ArrayImpl::Float64(array)) => builder.append(array),
            (Self::String(builder), ArrayImpl::String(array)) => builder.append(array),
            (Self::Decimal(builder), ArrayImpl::Decimal(array)) => builder.append(array),
            (Self::Date(builder), ArrayImpl::Date(array)) => builder.append(array),
            (Self::Timestamp(builder), ArrayImpl::Timestamp(array)) => builder.append(array),
            (Self::TimestampTz(builder), ArrayImpl::TimestampTz(array)) => builder.append(array),
            (Self::Interval(builder), ArrayImpl::Interval(array)) => builder.append(array),
            (Self::Blob(builder), ArrayImpl::Blob(array)) => builder.append(array),
            (Self::Vector(builder), ArrayImpl::Vector(array)) => builder.append(array),
            _ => todo!(),
        }
    }

    pub fn finish(self) -> (Vec<BlockIndex>, Vec<u8>) {
        match self {
            Self::Int16(builder) => builder.finish(),
            Self::Int32(builder) => builder.finish(),
            Self::Int64(builder) => builder.finish(),
            Self::Bool(builder) => builder.finish(),
            Self::Float64(builder) => builder.finish(),
            Self::String(builder) => builder.finish(),
            Self::Decimal(builder) => builder.finish(),
            Self::Date(builder) => builder.finish(),
            Self::Timestamp(builder) => builder.finish(),
            Self::TimestampTz(builder) => builder.finish(),
            Self::Interval(builder) => builder.finish(),
            Self::Blob(builder) => builder.finish(),
            Self::Vector(builder) => builder.finish(),
        }
    }
}
