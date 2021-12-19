//! Provide utilities to access the internal states of the [`Array`].

use bitvec::vec::BitVec;

use super::{Array, ArrayImpl};

pub trait ArrayValidExt: Array {
    fn get_valid_bitmap(&self) -> &BitVec;
}

pub trait ArrayImplValidExt {
    fn get_valid_bitmap(&self) -> &BitVec;
}

impl ArrayImplValidExt for ArrayImpl {
    fn get_valid_bitmap(&self) -> &BitVec {
        match self {
            Self::Bool(a) => a.get_valid_bitmap(),
            Self::Int32(a) => a.get_valid_bitmap(),
            Self::Int64(a) => a.get_valid_bitmap(),
            Self::Float64(a) => a.get_valid_bitmap(),
            Self::Utf8(a) => a.get_valid_bitmap(),
            Self::Decimal(a) => a.get_valid_bitmap(),
        }
    }
}

pub trait ArrayEstimateExt: Array {
    /// Get estimated size of the array in memory
    fn get_estimated_size(&self) -> usize;
}

pub trait ArrayImplEstimateExt {
    /// Get estimated size of the array in memory
    fn get_estimated_size(&self) -> usize;
}

impl ArrayImplEstimateExt for ArrayImpl {
    fn get_estimated_size(&self) -> usize {
        match self {
            Self::Bool(a) => a.get_estimated_size(),
            Self::Int32(a) => a.get_estimated_size(),
            Self::Int64(a) => a.get_estimated_size(),
            Self::Float64(a) => a.get_estimated_size(),
            Self::Utf8(a) => a.get_estimated_size(),
            Self::Decimal(a) => a.get_estimated_size(),
        }
    }
}
