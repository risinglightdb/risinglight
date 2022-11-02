use std::iter::IntoIterator;

use itertools::Itertools;

use super::{ArrayBuilderImpl, DataChunk};
use crate::types::{ConvertError, DataType, DataValue};

/// A helper struct to build a [`DataChunk`].
pub struct DataChunkBuilder {
    array_builders: Vec<ArrayBuilderImpl>,
    size: usize,
    capacity: usize,
}

impl DataChunkBuilder {
    pub fn new<'a>(data_types: impl IntoIterator<Item = &'a DataType>, capacity: usize) -> Self {
        assert_ne!(capacity, 0);
        let array_builders = data_types
            .into_iter()
            .map(|ty| ArrayBuilderImpl::with_capacity(capacity, ty))
            .collect();
        DataChunkBuilder {
            array_builders,
            size: 0,
            capacity,
        }
    }

    /// Push a row in the Iterator.
    ///
    /// The row is accepted as an iterator of [`DataValue`], and it's required that the size of row
    /// should be the same as the number of columns.
    ///
    /// A [`DataChunk`] will be returned while `size == capacity`, and it should always be handled
    /// correctly.
    #[must_use]
    pub fn push_row(&mut self, row: impl IntoIterator<Item = DataValue>) -> Option<DataChunk> {
        self.array_builders
            .iter_mut()
            .zip_eq(row)
            .for_each(|(builder, v)| builder.push(&v));
        self.size += 1;
        if self.size == self.capacity {
            self.take()
        } else {
            None
        }
    }

    /// Push a row of str in the Iterator.
    ///
    /// The row is accepted as an iterator of [`&str`], and it's required that the size of row
    /// should be the same as the number of columns.
    ///
    /// It will convert [`&str`] into specified type, and a [`ConvertError`] will be returned if
    /// converting fails.
    ///
    /// A [`DataChunk`] will be returned while converting correctly and `size == capacity`.
    pub fn push_str_row<'a>(
        &mut self,
        row: impl IntoIterator<Item = &'a str>,
    ) -> Result<Option<DataChunk>, ConvertError> {
        for (builder, r) in self.array_builders.iter_mut().zip_eq(row) {
            builder.push_str(r)?
        }

        self.size += 1;
        if self.size == self.capacity {
            Ok(self.take())
        } else {
            Ok(None)
        }
    }

    /// Generate a [`DataChunk`] with the remaining rows.
    ///
    /// If there are no remaining rows, `None` will be returned.
    #[must_use]
    pub fn take(&mut self) -> Option<DataChunk> {
        let size = std::mem::take(&mut self.size);
        let capacity = self.capacity;
        match size {
            0 => None,
            _ => Some(
                self.array_builders
                    .iter_mut()
                    .map(|builder| {
                        let chunk = builder.take();
                        builder.reserve(capacity);
                        chunk
                    })
                    .collect(),
            ),
        }
    }
}
