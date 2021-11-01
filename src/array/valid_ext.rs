use bitvec::vec::BitVec;

use super::{Array, ArrayImpl};

pub trait ArrayValidExt: Array {
    fn get_valid_bitmap(&self) -> &BitVec;
}

impl ArrayImpl {
    pub fn get_valid_bitmap(&self) -> &BitVec {
        match self {
            Self::Bool(a) => a.get_valid_bitmap(),
            Self::Int32(a) => a.get_valid_bitmap(),
            Self::Int64(a) => a.get_valid_bitmap(),
            Self::Float64(a) => a.get_valid_bitmap(),
            Self::UTF8(a) => a.get_valid_bitmap(),
        }
    }
}
