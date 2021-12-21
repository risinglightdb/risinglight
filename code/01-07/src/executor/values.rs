use itertools::Itertools;

use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::binder::BoundExpr;
use crate::types::DataType;

/// The executor of `VALUES`.
pub struct ValuesExecutor {
    pub column_types: Vec<DataType>,
    /// Each row is composed of multiple values, each value is represented by an expression.
    pub values: Vec<Vec<BoundExpr>>,
}

impl Executor for ValuesExecutor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError> {
        let cardinality = self.values.len();
        let mut builders = self
            .column_types
            .iter()
            .map(|ty| ArrayBuilderImpl::with_capacity(cardinality, ty))
            .collect_vec();
        for row in &self.values {
            for (expr, builder) in row.iter().zip(&mut builders) {
                let value = expr.eval_const()?;
                builder.push(&value);
            }
        }
        let chunk = builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<DataChunk>();
        Ok(chunk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ArrayImpl;
    use crate::binder::BoundExpr;
    use crate::types::{DataTypeExt, DataTypeKind, DataValue};

    #[test]
    fn values() {
        let values = [[0, 100], [1, 101], [2, 102], [3, 103]];
        let mut executor = ValuesExecutor {
            column_types: vec![DataTypeKind::Int(None).nullable(); 2],
            values: values
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|&v| BoundExpr::Constant(DataValue::Int32(v)))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
        };
        let output = executor.execute().unwrap();
        let expected = [
            ArrayImpl::Int32((0..4).collect()),
            ArrayImpl::Int32((100..104).collect()),
        ]
        .into_iter()
        .collect::<DataChunk>();
        assert_eq!(output, expected);
    }
}
