use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::SchemaRef;

use super::array::DeltaBatchStream;

pub trait Table: Send + Sync {
    /// Returns the schema of the table.
    fn schema(&self) -> SchemaRef;

    /// Observe changes of the table with the given projection.
    fn observe(&self, projection: &[usize]) -> Result<DeltaBatchStream>;
}

pub type TableRef = Arc<dyn Table>;
