use super::*;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::fmt;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// A collection of arrays.
///
/// A chunk is a horizontal subset of a query result.
#[derive(TypedBuilder, Default, Serialize, Deserialize, PartialEq)]
pub struct DataChunk {
    #[builder(default)]
    arrays: SmallVec<[ArrayImpl; 16]>,
    #[builder(default)]
    cardinality: usize,
}

impl DataChunk {
    /// Return the number of rows in the chunk.
    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    /// Get the reference of array by index.
    pub fn array_at(&self, idx: usize) -> &ArrayImpl {
        &self.arrays[idx]
    }

    /// Filter elements and create a new chunk.
    pub fn filter(&self, visibility: impl Iterator<Item = bool> + Clone) -> Self {
        let cardinality = visibility.clone().filter(|v| *v).count();
        let arrays = self
            .arrays
            .iter()
            .map(|a| a.filter(visibility.clone()))
            .collect();
        DataChunk {
            arrays,
            cardinality,
        }
    }
}

pub type DataChunkRef = Arc<DataChunk>;

// Print the chunk as a pretty table.
impl fmt::Display for DataChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use prettytable::{format, Table};
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        for i in 0..self.cardinality {
            let row = self.arrays.iter().map(|a| a.get_to_string(i)).collect();
            table.add_row(row);
        }
        write!(f, "{}", table)
    }
}
