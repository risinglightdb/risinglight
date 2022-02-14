use std::iter::IntoIterator;

use itertools::Itertools;

use super::{ArrayBuilderImpl, DataChunk};
use crate::types::{DataType, DataValue};

pub struct DataChunkBuilder {
    array_builders: Vec<ArrayBuilderImpl>,
    size: usize,
    capacity: usize,
}

impl DataChunkBuilder {
    pub fn new(data_types: impl IntoIterator<Item = DataType>, capacity: usize) -> Self {
        assert_ne!(capacity, 0);
        let array_builders = data_types
            .into_iter()
            .map(|ty| ArrayBuilderImpl::with_capacity(capacity, &ty))
            .collect();
        DataChunkBuilder {
            array_builders,
            size: 0,
            capacity,
        }
    }

    /// Push a row in the Iterator.
    ///
    /// The row is accepted as an iterator of DataValue, and it's required that the size of row
    /// should be the same as the number of columns.
    ///
    /// Either a [`DataChunkBuilder`] or a [`DataChunk`] will be returned, depends on whether `size
    /// == capacity`
    pub fn push_row(mut self, row: impl IntoIterator<Item = DataValue>) -> Result<Self, DataChunk> {
        self.array_builders
            .iter_mut()
            .zip_eq(row)
            .for_each(|(builder, v)| builder.push(&v));
        self.size += 1;
        if self.size == self.capacity {
            Err(self.finish().unwrap())
        } else {
            Ok(self)
        }
    }

    /// Generate a [`DataChunk`] with the remaining rows.
    ///
    /// If there are no remaining rows, `None` will be returned.
    #[must_use]
    pub fn finish(self) -> Option<DataChunk> {
        match self.capacity {
            0 => None,
            _ => Some(self.array_builders.into_iter().collect()),
        }
    }
}
