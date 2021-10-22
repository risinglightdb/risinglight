use crate::array::{Array, BoolArray, F64Array, I32Array};

/// Encode a primitive value into fixed-width buffer
pub trait PrimitiveFixedWidthEncode: Copy + Clone + 'static + Send + Sync {
    /// Width of each element
    const WIDTH: usize;
    const DEAFULT_VALUE: &'static Self;

    type ArrayType: Array<Item = Self>;
    fn encode(&self, buffer: &mut Vec<u8>);
}

impl PrimitiveFixedWidthEncode for bool {
    const WIDTH: usize = std::mem::size_of::<u8>();
    const DEAFULT_VALUE: &'static bool = &false;
    type ArrayType = BoolArray;

    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend((*self as u8).to_le_bytes());
    }
}

impl PrimitiveFixedWidthEncode for i32 {
    const WIDTH: usize = std::mem::size_of::<i32>();
    const DEAFULT_VALUE: &'static i32 = &0;

    type ArrayType = I32Array;

    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(self.to_le_bytes());
    }
}

impl PrimitiveFixedWidthEncode for f64 {
    const WIDTH: usize = std::mem::size_of::<f64>();
    const DEAFULT_VALUE: &'static f64 = &0.0;

    type ArrayType = F64Array;

    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(self.to_le_bytes());
    }
}
