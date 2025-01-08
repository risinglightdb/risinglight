// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::ops::RangeBounds;
use std::sync::Arc;

use super::*;
use crate::types::{DataValue, Row};

/// A collection of arrays.
///
/// A data chunk is a horizontal subset of a query result.
///
/// Note: It's valid for a [`DataChunk`] to have 0 column, but non-zero cardinality.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DataChunk {
    arrays: Arc<[ArrayImpl]>,
    cardinality: usize,
}

impl FromIterator<ArrayImpl> for DataChunk {
    fn from_iter<I: IntoIterator<Item = ArrayImpl>>(iter: I) -> Self {
        let arrays: Arc<[ArrayImpl]> = iter.into_iter().collect();
        let cardinality = arrays.first().map(ArrayImpl::len).unwrap_or(0);
        assert!(
            arrays.iter().map(|a| a.len()).all(|l| l == cardinality),
            "all arrays must have the same length"
        );
        DataChunk {
            arrays,
            cardinality,
        }
    }
}

impl FromIterator<ArrayBuilderImpl> for DataChunk {
    fn from_iter<I: IntoIterator<Item = ArrayBuilderImpl>>(iter: I) -> Self {
        iter.into_iter().map(|b| b.finish()).collect()
    }
}

impl DataChunk {
    /// Return a [`DataChunk`] with 1 element in 1 array.
    pub fn single(item: i32) -> Self {
        DataChunk {
            arrays: [ArrayImpl::new_int32([item].into_iter().collect())]
                .into_iter()
                .collect(),
            cardinality: 1,
        }
    }

    /// Return a no column [`DataChunk`] with `cardinality`.
    pub fn no_column(cardinality: usize) -> Self {
        DataChunk {
            arrays: Arc::new([]),
            cardinality,
        }
    }

    /// Return the number of rows in the chunk.
    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    /// Get reference to a row.
    pub fn row(&self, idx: usize) -> RowRef<'_> {
        debug_assert!(idx < self.cardinality, "index out of range");
        RowRef {
            chunk: self,
            row_idx: idx,
        }
    }

    /// Get an iterator over the rows.
    pub fn rows(&self) -> impl Iterator<Item = RowRef<'_>> {
        (0..self.cardinality).map(|idx| self.row(idx))
    }

    /// Get the reference of array by index.
    pub fn array_at(&self, idx: usize) -> &ArrayImpl {
        &self.arrays[idx]
    }

    /// Get the mutable reference of array by index.
    pub fn array_mut_at(&mut self, idx: usize) -> &mut ArrayImpl {
        &mut Arc::get_mut(&mut self.arrays).unwrap()[idx]
    }

    /// Get all arrays.
    pub fn arrays(&self) -> &[ArrayImpl] {
        &self.arrays
    }

    /// Filter elements and create a new chunk.
    pub fn filter(&self, visibility: &[bool]) -> Self {
        let arrays: Arc<[ArrayImpl]> = self.arrays.iter().map(|a| a.filter(visibility)).collect();
        DataChunk {
            cardinality: match arrays.first() {
                Some(a) => a.len(),
                None => visibility.iter().filter(|b| **b).count(),
            },
            arrays,
        }
    }

    /// Return the number of columns.
    pub fn column_count(&self) -> usize {
        self.arrays.len()
    }

    /// Returns a slice of self that is equivalent to the given subset.
    pub fn slice(&self, range: impl RangeBounds<usize> + Clone) -> Self {
        let begin = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => self.cardinality,
        };
        assert!(begin <= end && end <= self.cardinality, "out of range");
        let arrays = self.arrays.iter().map(|a| a.slice(range.clone())).collect();
        DataChunk {
            arrays,
            cardinality: end - begin,
        }
    }

    /// Get the estimated in-memory size.
    pub fn estimated_size(&self) -> usize {
        self.arrays.iter().map(|a| a.get_estimated_size()).sum()
    }

    pub fn from_rows(rows: &[RowRef<'_>], chunk: &Self) -> Self {
        let mut arrays = vec![];
        for col_idx in 0..chunk.column_count() {
            let mut builder = ArrayBuilderImpl::from_type_of_array(chunk.array_at(col_idx));
            for row in rows {
                builder.push(&row.get(col_idx));
            }
            arrays.push(builder.finish());
        }
        arrays.into_iter().collect()
    }

    /// Concatenate two chunks in rows.
    pub fn row_concat(self, other: Self) -> Self {
        assert_eq!(self.cardinality(), other.cardinality());
        self.arrays
            .iter()
            .chain(other.arrays.iter())
            .cloned()
            .collect()
    }
}

/// Print the data chunk as a pretty table.
impl fmt::Display for DataChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use comfy_table::Table;
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");
        for i in 0..self.cardinality() {
            let row: Vec<_> = self.arrays.iter().map(|a| a.get_to_string(i)).collect();
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

/// A chunk is a wrapper sturct for many data chunks.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Chunk {
    data_chunks: Vec<DataChunk>,
    header: Option<Vec<String>>,
}

impl Chunk {
    /// New a Chunk with some data chunks.
    pub fn new(data_chunks: Vec<DataChunk>) -> Self {
        Chunk {
            data_chunks,
            header: None,
        }
    }

    /// Get all data chunks.
    pub fn data_chunks(&self) -> &[DataChunk] {
        &self.data_chunks
    }

    /// Get first data chunk.
    pub fn get_first_data_chunk(&self) -> &DataChunk {
        &self.data_chunks[0]
    }

    /// Get header of chunk
    pub fn header(&self) -> Option<&[String]> {
        self.header.as_deref()
    }

    /// Set header for current chunk
    pub fn set_header(&mut self, header: Vec<String>) {
        self.header = Some(header);
    }
}

/// Print the chunk as a pretty table.
impl fmt::Display for Chunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use comfy_table::Table;
        let mut table = Table::new();
        if let Some(header) = self.header() {
            table.set_header(header);
        }
        table.load_preset("||--+-++|    ++++++");
        for data_chunk in self.data_chunks() {
            for i in 0..data_chunk.cardinality() {
                let row: Vec<_> = data_chunk
                    .arrays
                    .iter()
                    .map(|a| a.get_to_string(i))
                    .collect();
                table.add_row(row);
            }
        }
        write!(f, "{}", table)
    }
}

impl fmt::Debug for Chunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// Convert a [`Chunk`] to sqllogictest string
pub fn datachunk_to_sqllogictest_string(chunk: &Chunk) -> Vec<Vec<String>> {
    let mut output = vec![];
    for data_chunk in chunk.data_chunks() {
        for row in 0..data_chunk.cardinality() {
            let mut row_vec = vec![];
            for array in data_chunk.arrays() {
                let s = match array.get(row) {
                    DataValue::Null => "NULL".to_string(),
                    DataValue::Bool(v) => v.to_string(),
                    DataValue::Int16(v) => v.to_string(),
                    DataValue::Int32(v) => v.to_string(),
                    DataValue::Int64(v) => v.to_string(),
                    DataValue::Float64(v) => v.to_string(),
                    DataValue::String(s) if s.is_empty() => "(empty)".to_string(),
                    DataValue::String(s) => s.to_string(),
                    DataValue::Blob(s) if s.is_empty() => "(empty)".to_string(),
                    DataValue::Blob(s) => s.to_string(),
                    DataValue::Decimal(v) => v.to_string(),
                    DataValue::Date(v) => v.to_string(),
                    DataValue::Timestamp(v) => v.to_string(),
                    DataValue::TimestampTz(v) => v.to_string(),
                    DataValue::Interval(v) => v.to_string(),
                    DataValue::Vector(v) => v.to_string(),
                };
                row_vec.push(s);
            }
            output.push(row_vec);
        }
    }
    output
}

/// Reference to a row in [`DataChunk`].
pub struct RowRef<'a> {
    chunk: &'a DataChunk,
    row_idx: usize,
}

impl RowRef<'_> {
    /// Get the value at given column index.
    pub fn get(&self, idx: usize) -> DataValue {
        self.chunk.array_at(idx).get(self.row_idx)
    }

    pub fn get_by_indexes(&self, indexes: &[usize]) -> Vec<DataValue> {
        indexes
            .iter()
            .map(|i| self.chunk.array_at(*i).get(self.row_idx))
            .collect()
    }

    /// Get an iterator over the values of the row.
    pub fn values(&self) -> impl Iterator<Item = DataValue> + '_ {
        self.chunk.arrays().iter().map(|a| a.get(self.row_idx))
    }

    pub fn to_owned(&self) -> Row {
        self.values().collect()
    }
}

impl PartialEq<&[DataValue]> for RowRef<'_> {
    fn eq(&self, other: &&[DataValue]) -> bool {
        if other.len() != self.chunk.column_count() {
            return false;
        }
        self.values().zip(other.iter()).all(|(a, b)| &a == b)
    }
}
