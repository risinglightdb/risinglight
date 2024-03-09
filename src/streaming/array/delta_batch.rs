use arrow::array::{AsArray, BooleanArray, RecordBatch, RecordBatchOptions};
use arrow::buffer::BooleanBuffer;
use arrow::compute::FilterBuilder;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;

/// A [`RecordBatch`] where each row is either an insert or a delete.
#[derive(Debug, Clone, PartialEq)]
pub struct DeltaBatch {
    /// A boolean buffer where `true` means delete and `false` means insert.
    ops: BooleanBuffer,
    data: RecordBatch,
}

impl DeltaBatch {
    /// Creates a new [`DeltaBatch`].
    pub fn new(ops: BooleanBuffer, data: RecordBatch) -> Self {
        assert_eq!(ops.len(), data.num_rows());
        Self { ops, data }
    }

    /// Returns true if the row is an insert.
    pub fn is_insert(&self, row: usize) -> bool {
        !self.ops.value(row)
    }

    /// Returns true if the row is an delete.
    pub fn is_delete(&self, row: usize) -> bool {
        self.ops.value(row)
    }

    /// Returns the schema of the [`DeltaBatch`].
    pub fn schema(&self) -> SchemaRef {
        self.data.schema()
    }

    /// Projects the schema onto the specified columns
    pub fn project(&self, indices: &[usize]) -> Result<Self, ArrowError> {
        Ok(Self {
            data: self.data.project(indices)?,
            ops: self.ops.clone(),
        })
    }

    /// Returns the number of columns.
    pub fn num_columns(&self) -> usize {
        self.data.num_columns()
    }

    /// Returns the number of rows.
    pub fn num_rows(&self) -> usize {
        self.data.num_rows()
    }

    /// Returns the operation column.
    pub fn ops(&self) -> &BooleanBuffer {
        &self.ops
    }

    /// Returns the [`RecordBatch`] of this [`DeltaBatch`].
    pub fn data(&self) -> &RecordBatch {
        &self.data
    }

    /// Returns a new [`DeltaBatch`] with arrays containing only values matching the filter.
    pub fn filter(&self, predicate: &BooleanArray) -> Result<Self, ArrowError> {
        // modified from `arrow::compute::filter_record_batch`
        let filter = FilterBuilder::new(predicate).optimize().build();

        let ops = filter
            .filter(&BooleanArray::new(self.ops.clone(), None))?
            .as_boolean()
            .clone()
            .into_parts()
            .0;
        let filtered_arrays = self
            .data
            .columns()
            .iter()
            .map(|a| filter.filter(a))
            .collect::<Result<Vec<_>, _>>()?;
        let options = RecordBatchOptions::default().with_row_count(Some(filter.count()));
        let data = RecordBatch::try_new_with_options(self.schema(), filtered_arrays, &options)?;
        Ok(Self { ops, data })
    }
}

/// Converts a [`RecordBatch`] into a [`DeltaBatch`] with all rows as inserts.
impl From<RecordBatch> for DeltaBatch {
    fn from(data: RecordBatch) -> Self {
        let ops = BooleanBuffer::new_unset(data.num_rows());
        Self { ops, data }
    }
}
