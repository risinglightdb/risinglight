// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::ops::RangeBounds;
use std::sync::Arc;

use super::*;
use crate::types::DataValue;

/// A collection of arrays.
///
/// A chunk is a horizontal subset of a query result.
#[derive(Clone, PartialEq)]
pub struct DataChunk {
    pub arrays: Arc<[ArrayImpl]>,
    header: Option<Vec<String>>,
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
            header: None,
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
            arrays: [ArrayImpl::Int32([item].into_iter().collect())]
                .into_iter()
                .collect(),
            header: None,
        }
    }

    /// Return the number of rows in the chunk.
    pub fn cardinality(&self) -> usize {
        self.arrays.first().map(ArrayImpl::len).unwrap_or(0)
    }

    /// Get reference to a row.
    pub fn row(&self, idx: usize) -> RowRef<'_> {
        RowRef {
            chunk: self,
            row_idx: idx,
        }
    }

    /// Get an iterator over the rows.
    pub fn rows(&self) -> impl Iterator<Item = RowRef<'_>> {
        (0..self.cardinality()).map(|idx| self.row(idx))
    }

    /// Get the reference of array by index.
    pub fn array_at(&self, idx: usize) -> &ArrayImpl {
        &self.arrays[idx]
    }

    /// Get all arrays.
    pub fn arrays(&self) -> &[ArrayImpl] {
        &self.arrays
    }

    /// Filter elements and create a new chunk.
    pub fn filter(&self, visibility: impl Iterator<Item = bool> + Clone) -> Self {
        let arrays = self
            .arrays
            .iter()
            .map(|a| a.filter(visibility.clone()))
            .collect();
        DataChunk {
            arrays,
            header: None,
        }
    }

    /// Return the number of columns.
    pub fn column_count(&self) -> usize {
        self.arrays.len()
    }

    /// Returns a slice of self that is equivalent to the given subset.
    pub fn slice(&self, range: impl RangeBounds<usize> + Clone) -> Self {
        let arrays = self.arrays.iter().map(|a| a.slice(range.clone())).collect();
        DataChunk {
            arrays,
            header: None,
        }
    }

    /// Get the estimated in-memory size.
    pub fn estimated_size(&self) -> usize {
        self.arrays.iter().map(|a| a.get_estimated_size()).sum()
    }

    /// This function should only be called in plan nodes. If you want to change column name, you
    /// should change the plan node, instead of directly attaching a header to the chunk.
    pub fn set_header(&mut self, header: Vec<String>) {
        self.header = Some(header);
    }

    pub fn header(&self) -> Option<&[String]> {
        self.header.as_deref()
    }

    pub fn from_rows<'a>(rows: &[RowRef<'a>], chunk: &Self) -> Self {
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
}

/// Print the chunk as a pretty table.
impl fmt::Display for DataChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use comfy_table::Table;
        let mut table = Table::new();
        if let Some(header) = &self.header {
            table.set_header(header);
        }
        table.load_preset("||--+-++|    ++++++");
        for _i in 0..self.cardinality() {
            let row: Vec<_> = self.arrays.iter().map(|a| a.get_to_string(0)).collect();
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

/// Convert a [`DataChunk`] to sqllogictest string
pub fn datachunk_to_sqllogictest_string(chunk: &DataChunk) -> String {
    let mut output = String::new();
    for row in 0..chunk.cardinality() {
        use std::fmt::Write;
        for (col, array) in chunk.arrays().iter().enumerate() {
            if col != 0 {
                write!(output, " ").unwrap();
            }
            match array.get(row) {
                DataValue::Null => write!(output, "NULL"),
                DataValue::Bool(v) => write!(output, "{}", v),
                DataValue::Int32(v) => write!(output, "{}", v),
                DataValue::Int64(v) => write!(output, "{}", v),
                DataValue::Float64(v) => write!(output, "{}", v),
                DataValue::String(s) if s.is_empty() => write!(output, "(empty)"),
                DataValue::String(s) => write!(output, "{}", s),
                DataValue::Blob(s) if s.is_empty() => write!(output, "(empty)"),
                DataValue::Blob(s) => write!(output, "{}", s),
                DataValue::Decimal(v) => write!(output, "{}", v),
                DataValue::Date(v) => write!(output, "{}", v),
                DataValue::Interval(v) => write!(output, "{}", v),
            }
            .unwrap();
        }
        writeln!(output).unwrap();
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
}
