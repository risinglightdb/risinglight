use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::{FieldRef, Schema, SchemaRef};
use itertools::Itertools;

pub trait Expression: Send + Sync {
    /// Returns the data type of the expression.
    fn field(&self) -> FieldRef;

    /// Evaluate the expression on the input record batch.
    ///
    /// Normally the output record batch should have exactly one column.
    /// If any error occurs, it will have an additional column to store the error messages.
    fn eval(&self, input: &RecordBatch) -> Result<RecordBatch>;
}

/// A reference-counted reference to an [`Expression`].
pub type ExpressionRef = Arc<dyn Expression>;

/// A list of expressions.
pub struct ExpressionList {
    schema: SchemaRef,
    exprs: Vec<ExpressionRef>,
}

impl ExpressionList {
    /// Create a new expression list.
    pub fn new(exprs: Vec<ExpressionRef>) -> Self {
        let schema = Arc::new(Schema::new(exprs.iter().map(|e| e.field()).collect_vec()));
        Self { schema, exprs }
    }

    /// Returns the output schema of the expression list.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Evaluate the expression list on the input record batch.
    pub fn eval(&self, input: &RecordBatch) -> Result<RecordBatch> {
        let mut columns = Vec::with_capacity(self.exprs.len());
        for expr in &self.exprs {
            let batch = expr.eval(input)?;
            columns.push(batch.column(0).clone());
            // TODO: handle error column
        }
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}
