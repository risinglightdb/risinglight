use std::fmt;
use std::sync::Arc;

use itertools::Itertools;

use super::*;

/// A collection of arrays.
///
/// A chunk is a horizontal subset of a query result.
#[derive(PartialEq, Clone)]
pub struct DataChunk {
    arrays: Arc<[ArrayImpl]>,
}

/// Create [`DataChunk`] from a list of column arrays.
impl FromIterator<ArrayImpl> for DataChunk {
    fn from_iter<I: IntoIterator<Item = ArrayImpl>>(iter: I) -> Self {
        let arrays = iter.into_iter().collect::<Arc<[ArrayImpl]>>();
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
    /// Return a [`DataChunk`] with 1 `item` in 1 array.
    pub fn single(item: i32) -> Self {
        DataChunk {
            arrays: [ArrayImpl::Int32([item].into_iter().collect())]
                .into_iter()
                .collect(),
        }
    }

    /// Return the number of rows in the chunk.
    pub fn cardinality(&self) -> usize {
        self.arrays[0].len()
    }

    /// Get all arrays.
    pub fn arrays(&self) -> &[ArrayImpl] {
        &self.arrays
    }

    /// Concatenate multiple chunks into one.
    pub fn concat(chunks: &[DataChunk]) -> Self {
        assert!(!chunks.is_empty(), "must concat at least one chunk");
        let mut builders = chunks[0]
            .arrays()
            .iter()
            .map(ArrayBuilderImpl::from_type_of_array)
            .collect_vec();
        for chunk in chunks {
            for (array, builder) in chunk.arrays.iter().zip(builders.iter_mut()) {
                builder.append(array);
            }
        }
        builders.into_iter().map(|b| b.finish()).collect()
    }
}

/// Print the chunk as a pretty table.
impl fmt::Display for DataChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use prettytable::{format, Table};
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        for i in 0..self.cardinality() {
            let row = self.arrays.iter().map(|a| a.get(i).to_string()).collect();
            table.add_row(row);
        }
        write!(f, "{}", table)
    }
}

impl fmt::Debug for DataChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}
