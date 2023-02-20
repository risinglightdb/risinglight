// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Display};

use super::DataChunk;

/// A collection of arrays.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamChunk {
    ops: Vec<Op>,
    data: DataChunk,
}

/// Operations for each line in [`DataChunk`].
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub enum Op {
    Insert,
    Delete,
}

impl Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Op::Insert => write!(f, "+"),
            Op::Delete => write!(f, "-"),
        }
    }
}

impl StreamChunk {
    /// Create a new stream chunk.
    pub fn new(ops: Vec<Op>, data: DataChunk) -> Self {
        assert_eq!(ops.len(), data.cardinality());
        StreamChunk { ops, data }
    }

    /// Get cardinality of chunk.
    pub fn cardinality(&self) -> usize {
        self.data.cardinality()
    }
}

/// Print the stream chunk as a pretty table.
impl Display for StreamChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use comfy_table::Table;
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");
        for i in 0..self.cardinality() {
            let op = self.ops[i].to_string();
            let row = self.data.arrays().iter().map(|a| a.get_to_string(i));
            table.add_row(std::iter::once(op).chain(row));
        }
        write!(f, "{}", table)
    }
}

impl Debug for StreamChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}
