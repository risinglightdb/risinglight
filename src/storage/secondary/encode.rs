use bytes::{Buf, BufMut};

use crate::array::{Array, BoolArray, F64Array, I32Array};

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
