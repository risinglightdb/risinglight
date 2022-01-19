// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bytes::{Buf, BufMut};
use rust_decimal::Decimal;

use crate::array::{Array, BoolArray, DateArray, DecimalArray, F64Array, I32Array, IntervalArray};
use crate::types::{Date, Interval};

/// Encode a primitive value into fixed-width buffer
pub trait PrimitiveFixedWidthEncode: Copy + Clone + 'static + Send + Sync {
    /// Width of each element
    const WIDTH: usize;
    const DEAFULT_VALUE: &'static Self;

    type ArrayType: Array<Item = Self>;

    /// Encode current primitive data to the end of an `Vec<u8>`.
    fn encode(&self, buffer: &mut impl BufMut);

    /// Decode a data from a bytes array.
    fn decode(buffer: &mut impl Buf) -> Self;
}

impl PrimitiveFixedWidthEncode for bool {
    const WIDTH: usize = std::mem::size_of::<u8>();
    const DEAFULT_VALUE: &'static bool = &false;
    type ArrayType = BoolArray;

    fn encode(&self, buffer: &mut impl BufMut) {
        buffer.put_u8(*self as u8)
    }

    fn decode(buffer: &mut impl Buf) -> Self {
        buffer.get_u8() != 0
    }
}

impl PrimitiveFixedWidthEncode for i32 {
    const WIDTH: usize = std::mem::size_of::<i32>();
    const DEAFULT_VALUE: &'static i32 = &0;

    type ArrayType = I32Array;

    fn encode(&self, buffer: &mut impl BufMut) {
        buffer.put_i32_le(*self);
    }

    fn decode(buffer: &mut impl Buf) -> Self {
        buffer.get_i32_le()
    }
}

impl PrimitiveFixedWidthEncode for f64 {
    const WIDTH: usize = std::mem::size_of::<f64>();
    const DEAFULT_VALUE: &'static f64 = &0.0;

    type ArrayType = F64Array;

    fn encode(&self, buffer: &mut impl BufMut) {
        buffer.put_f64_le(*self);
    }

    fn decode(buffer: &mut impl Buf) -> Self {
        buffer.get_f64_le()
    }
}

impl PrimitiveFixedWidthEncode for Decimal {
    const WIDTH: usize = std::mem::size_of::<Decimal>();
    const DEAFULT_VALUE: &'static Self = &Decimal::from_parts(0, 0, 0, false, 0);

    type ArrayType = DecimalArray;

    fn encode(&self, buffer: &mut impl BufMut) {
        buffer.put_u128_le(u128::from_le_bytes(self.serialize()))
    }

    fn decode(buffer: &mut impl Buf) -> Self {
        Decimal::deserialize(buffer.get_u128_le().to_le_bytes())
    }
}

impl PrimitiveFixedWidthEncode for Date {
    const WIDTH: usize = std::mem::size_of::<i32>();
    const DEAFULT_VALUE: &'static Self = &Date::new(0);

    type ArrayType = DateArray;

    fn encode(&self, buffer: &mut impl BufMut) {
        buffer.put_i32(self.get_inner());
    }

    fn decode(buffer: &mut impl Buf) -> Self {
        Date::new(buffer.get_i32())
    }
}

impl PrimitiveFixedWidthEncode for Interval {
    const WIDTH: usize = std::mem::size_of::<i32>() + std::mem::size_of::<i32>();
    const DEAFULT_VALUE: &'static Self = &Interval::new(0, 0);

    type ArrayType = IntervalArray;

    fn encode(&self, buffer: &mut impl BufMut) {
        buffer.put_i32(self.get_years());
        buffer.put_i32(self.get_days());
    }

    fn decode(buffer: &mut impl Buf) -> Self {
        let years = buffer.get_i32();
        let days = buffer.get_i32();
        Interval::new(years, days)
    }
}
