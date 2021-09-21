use super::*;

use bitvec::vec::BitVec;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::fmt;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// `DataChunk` is a collection of arrays with visibility mask.
#[derive(TypedBuilder, Default, Serialize, Deserialize, PartialEq)]
pub struct DataChunk {
    #[builder(default)]
    arrays: SmallVec<[ArrayImpl; 16]>,
    #[builder(default)]
    dimension: usize,
    #[builder(default)]
    cardinality: usize,
    #[builder(default, setter(strip_option))]
    visibility: Option<BitVec>,
}

impl DataChunk {
    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    pub fn visibility(&self) -> &Option<BitVec> {
        &self.visibility
    }

    pub fn set_visibility(&mut self, visibility: BitVec) {
        self.visibility = Some(visibility);
    }

    pub fn array_at(&self, idx: usize) -> &ArrayImpl {
        &self.arrays[idx]
    }
}

pub type DataChunkRef = Arc<DataChunk>;

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
