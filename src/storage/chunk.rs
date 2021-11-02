use std::sync::Arc;

use bitvec::prelude::BitVec;

use crate::array::ArrayImpl;

/// Similar to [`DataChunk`], in the storage system, we use [`StorageChunk`]
/// to represent a set of columns. [`StorageChunk`] contains pointers to
/// array, and a visibility map. [`StorageChunk`] generally corresponds to
/// a batch read from a [`RowSet`]. In Secondary, unaligned block read
/// will produce arrays of different starting RowIds from each column,
/// and delete map will be applied to the columns with the `visibility`
/// bitmap.
pub struct StorageChunk {
    /// If a row is visible in this chunk. Data come from the delete map.
    visibility: Option<BitVec>,

    /// Indicates from which row should we begin to read the item. Columns
    /// are not aligned on block boundary, so every time we read from the
    /// column, we might not get the exact array beginning from a RowId.
    /// e.g. We request data from a column beginning at RowId 100, and
    /// it might return an array beginning at RowId 90. Then the resulting
    /// array will be stored in `arrays`, and we will store an offset of
    /// `10` in this vector.
    offsets: Vec<usize>,

    /// Plain array from the blocks.
    arrays: Vec<Arc<ArrayImpl>>,

    /// Number of accessible rows.
    cardinality: usize,
}

pub struct StorageArraySlice {
    array: Arc<ArrayImpl>,
    offset: usize,
}

impl StorageChunk {
    pub fn new(
        visibility: Option<BitVec>,
        offsets: Vec<usize>,
        arrays: Vec<Arc<ArrayImpl>>,
    ) -> Self {
        assert!(!arrays.is_empty());
        let first_length = arrays[0].len() - offsets[0];
        for (array, offset) in arrays.iter().zip(offsets.iter()) {
            assert_eq!(first_length, array.len() - *offset);
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
            offsets,
            cardinality,
        }
    }

    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    pub fn column_count(&self) -> usize {
        self.arrays.len()
    }

    pub fn array_slice_at(&self, idx: usize) -> StorageArraySlice {
        StorageArraySlice {
            array: self.arrays[idx].clone(),
            offset: self.offsets[idx],
        }
    }

    pub fn visibility(&self) -> &Option<BitVec> {
        &self.visibility
    }
}
