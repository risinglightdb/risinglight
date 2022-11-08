// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bitvec::prelude::BitVec;
use smallvec::SmallVec;

use crate::array::{ArrayImpl, DataChunk};

pub type PackedVec<T> = SmallVec<[T; 16]>;

/// Similar to [`DataChunk`], in the storage system, we use [`StorageChunk`]
/// to represent a set of columns. [`StorageChunk`] contains pointers to
/// array, and a visibility map. [`StorageChunk`] generally corresponds to
/// a batch read from a `RowSet`. All constructed [`StorageChunk`] has at
/// least one element.
#[derive(Clone)]
pub struct StorageChunk {
    /// If a row is visible in this chunk. Data come from the delete map.
    visibility: Option<BitVec>,

    /// Plain array from the blocks.
    arrays: PackedVec<ArrayImpl>,

    /// Number of accessible rows.
    cardinality: usize,
}

impl StorageChunk {
    /// Construct a [`StorageChunk`] from `visibility` and `arrays`. If there are no element in the
    /// chunk, the function will return `None`.
    pub fn construct(
        visibility: Option<BitVec>,
        arrays: SmallVec<[ArrayImpl; 16]>,
    ) -> Option<Self> {
        assert!(!arrays.is_empty());
        let first_length = arrays[0].len();
        for array in &arrays {
            assert_eq!(first_length, array.len());
        }
        let cardinality = if let Some(ref visibility) = visibility {
            assert_eq!(visibility.len(), first_length);
            visibility.count_ones()
        } else {
            first_length
        };

        if cardinality > 0 {
            Some(Self {
                visibility,
                arrays,
                cardinality,
            })
        } else {
            None
        }
    }

    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    pub fn row_count(&self) -> usize {
        self.array_at(0).len()
    }

    pub fn column_count(&self) -> usize {
        self.arrays.len()
    }

    pub fn arrays(&self) -> &[ArrayImpl] {
        &self.arrays
    }

    pub fn array_at(&self, idx: usize) -> &ArrayImpl {
        &self.arrays[idx]
    }

    pub fn visibility(&self) -> &Option<BitVec> {
        &self.visibility
    }

    pub fn to_data_chunk(self) -> DataChunk {
        if self.arrays.is_empty() {
            return DataChunk::no_column(self.cardinality);
        }
        match self.visibility {
            Some(visibility) => DataChunk::from_iter(
                self.arrays
                    .iter()
                    .map(|a| a.filter(visibility.iter().map(|x| *x))),
            ),
            None => DataChunk::from_iter(self.arrays),
        }
    }
}
