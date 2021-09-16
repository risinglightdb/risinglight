use crate::array::DataChunk;
use crate::array::{ArrayBuilderImpl, ArrayImpl};

use crate::{parser::*, types::DataValue};

impl Expression {
    /// Evaluate the given expression.
    pub fn eval(&self) -> DataValue {
        match &self.kind {
            ExprKind::Constant(v) => v.clone(),
            _ => todo!("evaluate expression"),
        }
    }

    pub fn eval_array(&self, chunk: &DataChunk) -> ArrayImpl {
        match &self.kind {
            ExprKind::ColumnRef(col_ref) => {
                let mut builder = ArrayBuilderImpl::new(self.return_type.unwrap());
                builder.append(chunk.array_at(col_ref.column_index.unwrap() as usize));
                builder.finish()
            }
            _ => todo!("evaluate expression"),
        }
    }
}
