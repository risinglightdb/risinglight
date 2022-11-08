// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! Utilities to shuffle [`Array`] content.

use itertools::Itertools;
use smallvec::SmallVec;

use super::*;
use crate::for_all_variants;

/// Transform an [`Array`] to `Vec<Option<Item>>`.
pub trait ArrayToVecExt: Array {
    /// Transform an [`Array`] to `Vec<Option<Item>>`.
    ///
    /// ```
    /// use risinglight::array::*;
    ///
    /// let array = I32Array::from_iter([1, 3, 5, 7, 9].map(Some));
    /// assert_eq!(
    ///     array.to_vec(),
    ///     vec![Some(1), Some(3), Some(5), Some(7), Some(9)]
    /// );
    /// ```
    fn to_vec(&self) -> Vec<Option<<Self::Item as ToOwned>::Owned>> {
        self.iter().map(|x| x.map(|x| x.to_owned())).collect_vec()
    }
}

impl<T: Array> ArrayToVecExt for T {}

/// Append scattered array values into builder
pub trait ArrayBuilderPickExt: ArrayBuilder {
    /// Pick rows accroding to `logical_rows` from array to the current builder.
    ///
    /// For example, the `array` contains `[1, 3, 5, 7, 9]`, and `logical_rows` is
    /// `[4, 2, 0]`, then we will append `[9, 5, 1]` to the builder.
    ///
    /// ```
    /// use risinglight::array::*;
    ///
    /// let mut builder = I32ArrayBuilder::with_capacity(10);
    /// let array = I32Array::from_iter([1, 3, 5, 7, 9].map(Some));
    /// builder.pick_from(&array, &[4, 2, 0]);
    /// assert_eq!(builder.finish().to_vec(), vec![Some(9), Some(5), Some(1)]);
    /// ```
    fn pick_from(&mut self, array: &Self::Array, logical_rows: &[usize]) {
        for idx in logical_rows {
            self.push(array.get(*idx));
        }
    }

    /// Pick rows accroding to `logical_rows` from arrays to the current builder.
    fn pick_from_multiple(&mut self, arrays: &[&Self::Array], logical_rows: &[(usize, usize)]) {
        for (idx, row) in logical_rows {
            self.push(arrays[*idx].get(*row));
        }
    }
}

impl<T: ArrayBuilder> ArrayBuilderPickExt for T {}

pub trait ArrayImplBuilderPickExt {
    fn pick_from(&mut self, array: &ArrayImpl, logical_rows: &[usize]);

    fn pick_from_multiple(&mut self, arrays: &[ArrayImpl], logical_rows: &[(usize, usize)]);
}

/// Get sorted indices from the current [`Array`]
pub trait ArraySortExt: Array
where
    <Self as Array>::Item: PartialOrd,
{
    /// Get indices of original items in a sorted array, which can be directly used in
    /// [`ArrayBuilderPickExt`].
    ///
    /// For example, `[1, 7, 3, 9, 5]` will have a sorted indices of `[0, 2, 4, 1, 3]`.
    ///
    /// Note that `None` is the smallest item, and will be put before any other items.
    ///
    /// ```
    /// use risinglight::array::*;
    ///
    /// let array = I32Array::from_iter([Some(1), Some(7), Some(3), Some(9), Some(5), None, None]);
    /// let indices = array.get_sorted_indices();
    /// assert_eq!(indices[2..], [0, 2, 4, 1, 3]);
    ///
    /// let mut builder = I32ArrayBuilder::with_capacity(10);
    /// builder.pick_from(&array, &indices);
    /// assert_eq!(
    ///     builder.finish().to_vec(),
    ///     [None, None, Some(1), Some(3), Some(5), Some(7), Some(9)]
    /// );
    /// ```
    fn get_sorted_indices(&self) -> Vec<usize> {
        let mut sort_keys = (0..self.len())
            .map(|x| self.get(x))
            .enumerate()
            .collect_vec();

        sort_keys.sort_unstable_by(|a, b| {
            use std::cmp::Ordering::*;
            let a = a.1;
            let b = b.1;
            match (a, b) {
                (None, None) => Equal,
                (None, _) => Less,
                (_, None) => Greater,
                // TODO: handle panic when doing `partial_cmp`.
                (a, b) => a.partial_cmp(&b).unwrap(),
            }
        });

        sort_keys.into_iter().map(|x| x.0).collect_vec()
    }
}

impl<T: Array> ArraySortExt for T where T::Item: PartialOrd {}

pub trait ArrayImplSortExt {
    fn get_sorted_indices(&self) -> Vec<usize>;
}

/// Implement dispatch functions for `ArrayImplBuilderPickExt` and `ArrayImplSortExt`
macro_rules! impl_array_impl_shuffle_ext {
    ([], $( { $Abc:ident, $Type:ty, $abc:ident, $AbcArray:ty, $AbcArrayBuilder:ty, $Value:ident, $Pattern:pat } ),*) => {
        impl ArrayImplBuilderPickExt for ArrayBuilderImpl {
            fn pick_from(&mut self, array: &ArrayImpl, logical_rows: &[usize]) {
                match (self, array) {
                    $(
                        (Self::$Abc(builder), ArrayImpl::$Abc(arr)) => builder.pick_from(arr, logical_rows),
                    )*
                    _ => panic!("failed to push value: type mismatch"),
                }
            }

            fn pick_from_multiple(
                &mut self,
                arrays: &[ArrayImpl],
                logical_rows: &[(usize, usize)],
            ) {
                match self {
                    $(
                        Self::$Abc(builder) => {
                            let typed_arrays = arrays
                                .iter()
                                .map(|x| x.try_into().unwrap())
                                .collect::<SmallVec<[_; 8]>>();
                            builder.pick_from_multiple(&typed_arrays, logical_rows);
                        }
                    )*
                }
            }
        }

        impl ArrayImplSortExt for ArrayImpl {
            fn get_sorted_indices(&self) -> Vec<usize> {
                match self {
                    $(
                        Self::$Abc(a) => a.get_sorted_indices(),
                    )*
                }
            }
        }
    }
}

for_all_variants! { impl_array_impl_shuffle_ext }
