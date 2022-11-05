// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! Provide utilities to access the internal states of the [`Array`].

use std::iter::TrustedLen;

use bitvec::vec::BitVec;

use super::{Array, ArrayImpl};
use crate::for_all_variants;

pub trait ArrayValidExt: Array {
    fn get_valid_bitmap(&self) -> &BitVec;
}

pub trait ArrayImplValidExt {
    fn get_valid_bitmap(&self) -> &BitVec;
}

pub trait ArrayEstimateExt: Array {
    /// Get estimated size of the array in memory
    fn get_estimated_size(&self) -> usize;
}

pub trait ArrayImplEstimateExt {
    /// Get estimated size of the array in memory
    fn get_estimated_size(&self) -> usize;
}

pub trait ArrayFromDataExt: Array {
    fn from_data(
        data_iter: impl Iterator<Item = <Self::Item as ToOwned>::Owned> + TrustedLen,
        valid: BitVec,
    ) -> Self;
}

/// Implement dispatch functions for `ArrayImplValidExt` and `ArrayImplEstimateExt`
macro_rules! impl_array_impl_internal_ext {
    ([], $( { $Abc:ident, $Type:ty, $abc:ident, $AbcArray:ty, $AbcArrayBuilder:ty, $Value:ident, $Pattern:pat } ),*) => {
        impl ArrayImplValidExt for ArrayImpl {
            fn get_valid_bitmap(&self) -> &BitVec {
                match self {
                    $(
                        Self::$Abc(a) => a.get_valid_bitmap(),
                    )*
                }
            }
        }

        impl ArrayImplEstimateExt for ArrayImpl {
            fn get_estimated_size(&self) -> usize {
                match self {
                    $(
                        Self::$Abc(a) => a.get_estimated_size(),
                    )*
                }
            }
        }
    }
}

for_all_variants! { impl_array_impl_internal_ext }
