use crate::array::*;
use crate::binder::BoundExpr;
use crate::executor::ExecuteError;
use crate::types::DataValue;

impl BoundExpr {
    /// Evaluate the given expression as a constant value.
    ///
    /// This method is used in the evaluation of `insert values` and optimizer
    pub fn eval_const(&self) -> Result<DataValue, ExecuteError> {
        match &self {
            Self::Constant(v) => Ok(v.clone()),
            Self::ColumnRef(_) => panic!("can not evaluate on ColumnRef"),
        }
    }

    /// Evaluate the given expression as an array.
    pub fn eval_array(&self, chunk: &DataChunk) -> Result<ArrayImpl, ExecuteError> {
        match &self {
            // NOTE:
            // Currently we assume that the column id is equal to its physical index in the
            // DataChunk. It is true in a simple `SELECT v FROM t` case, where the child plan of the
            // Projection is Get. However, in a more complex case with join or aggregation, this
            // assumption no longer holds. At that time we will convert the ColumnRef into an
            // InputRef, and resolve the physical index from column id.
            Self::ColumnRef(v) => Ok(chunk.arrays()[v.column_ref_id.column_id as usize].clone()),
            Self::Constant(v) => {
                let mut builder = ArrayBuilderImpl::with_capacity(
                    chunk.cardinality(),
                    &self.return_type().unwrap(),
                );
                // TODO: optimize this
                for _ in 0..chunk.cardinality() {
                    builder.push(v);
                }
                Ok(builder.finish())
            }
        }
    }
}
