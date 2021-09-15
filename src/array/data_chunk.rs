use super::*;

use bitvec::vec::BitVec;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// `DataChunk` is a collection of arrays with visibility mask.
#[derive(TypedBuilder, Default, Serialize, Deserialize)]
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
