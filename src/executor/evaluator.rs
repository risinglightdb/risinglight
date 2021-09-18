use crate::{
    array::{ArrayBuilderImpl, ArrayImpl, DataChunk},
    binder::{BoundExpr, BoundExprKind},
    types::DataValue,
};

impl BoundExpr {
    /// Evaluate the given expression.
    pub fn eval(&self) -> DataValue {
        match &self.kind {
            BoundExprKind::Constant(v) => v.clone(),
            _ => todo!("evaluate expression"),
        }
    }

    pub fn eval_array(&self, chunk: &DataChunk) -> ArrayImpl {
        match &self.kind {
            BoundExprKind::ColumnRef(col_ref) => {
                let mut builder = ArrayBuilderImpl::new(self.return_type.clone().unwrap());
                builder.append(chunk.array_at(col_ref.column_index as usize));
                builder.finish()
            }
            _ => todo!("evaluate expression"),
        }
    }
}
