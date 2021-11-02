use std::sync::Arc;

use bitvec::prelude::BitVec;

use crate::array::ArrayImpl;

/// Similar to [`DataChunk`], in the storage system, we use [`StorageChunk`]
/// to represent a set of columns. [`StorageChunk`] contains pointers to
/// array, and a visibility map. [`StorageChunk`] generally corresponds to
/// a batch read from a [`RowSet`].
pub struct StorageChunk {
    /// If a row is visible in this chunk. Data come from the delete map.
    visibility: Option<BitVec>,

    /// Plain array from the blocks.
    arrays: Vec<Arc<ArrayImpl>>,

    /// Number of accessible rows.
    cardinality: usize,
}

impl StorageChunk {
    pub fn new(visibility: Option<BitVec>, arrays: Vec<Arc<ArrayImpl>>) -> Self {
        assert!(!arrays.is_empty());
        let first_length = arrays[0].len();
        for array in &arrays {
            assert_eq!(first_length, array.len());
        }
        let cardinality;
        if let Some(ref visibility) = visibility {
            assert_eq!(visibility.len(), first_length);
            cardinality = visibility.count_ones();
        } else {
            cardinality = first_length;
        }
        Self {
            visibility,
            arrays,
            cardinality,
        }
    }

    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    pub fn column_count(&self) -> usize {
        self.arrays.len()
    }

    pub fn array_at(&self, idx: usize) -> &Arc<ArrayImpl> {
        &self.arrays[idx]
    }

    pub fn visibility(&self) -> &Option<BitVec> {
        &self.visibility
    }
}
