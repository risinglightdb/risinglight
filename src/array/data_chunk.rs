use super::*;

use crate::types::DataValue;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::fmt;
use std::sync::Arc;

/// A collection of arrays.
///
/// A chunk is a horizontal subset of a query result.
#[derive(Default, Serialize, Deserialize, PartialEq)]
pub struct DataChunk {
    arrays: SmallVec<[ArrayImpl; 16]>,
}

impl FromIterator<ArrayImpl> for DataChunk {
    fn from_iter<I: IntoIterator<Item = ArrayImpl>>(iter: I) -> Self {
        let arrays: SmallVec<[ArrayImpl; 16]> = iter.into_iter().collect();
        assert!(!arrays.is_empty());
        let cardinality = arrays[0].len();
        assert!(
            arrays.iter().map(|a| a.len()).all(|l| l == cardinality),
            "all arrays must have the same length"
        );
        DataChunk { arrays }
    }
}

impl DataChunk {
    /// Return a DataChunk with 1 element in 1 array.
    pub fn single() -> Self {
        DataChunk {
            arrays: [ArrayImpl::Int32((0..=0).collect())].into_iter().collect(),
        }
    }

    /// Return the number of rows in the chunk.
    pub fn cardinality(&self) -> usize {
        self.arrays[0].len()
    }

    pub fn get_row_by_idx(&self, idx: usize) -> Vec<DataValue> {
        self.arrays
            .iter()
            .map(|arr| arr.get_data_value_by_idx(idx))
            .collect()
    }

    /// Get the reference of array by index.
    pub fn array_at(&self, idx: usize) -> &ArrayImpl {
        &self.arrays[idx]
    }

    /// Filter elements and create a new chunk.
    pub fn filter(&self, visibility: impl Iterator<Item = bool> + Clone) -> Self {
        let arrays = self
            .arrays
            .iter()
            .map(|a| a.filter(visibility.clone()))
            .collect();
        DataChunk { arrays }
    }

    pub fn column_count(&self) -> usize {
        self.arrays.len()
    }
}

pub type DataChunkRef = Arc<DataChunk>;

/// Print the chunk as a pretty table.
impl fmt::Display for DataChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use prettytable::{format, Table};
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        for i in 0..self.cardinality() {
            let row = self.arrays.iter().map(|a| a.get_to_string(i)).collect();
            table.add_row(row);
        }
        write!(f, "{}", table)
    }
}
