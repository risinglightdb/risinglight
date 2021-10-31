use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::binder::BoundExpr;

/// The executor of `values`.
pub struct ValuesExecutor {
    /// Each row is composed of multiple values,
    /// each value is represented by an expression.
    pub values: Vec<Vec<BoundExpr>>,
}

impl ValuesExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            let cardinality = self.values.len();
            assert!(cardinality > 0);

            let mut array_builders = self.values[0]
                .iter()
                .map(|expr| ArrayBuilderImpl::new(expr.return_type.as_ref().unwrap()))
                .collect::<Vec<ArrayBuilderImpl>>();
            for row in &self.values {
                for (expr, builder) in row.iter().zip(&mut array_builders) {
                    let value = expr.eval();
                    builder.push(&value);
                }
            }
            let chunk = array_builders
                .into_iter()
                .map(|builder| builder.finish())
                .collect::<DataChunk>();
            yield chunk;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ArrayImpl;
    use crate::binder::BoundExpr;
    use crate::types::DataValue;

    #[tokio::test]
    async fn values() {
        let values = [[0, 100], [1, 101], [2, 102], [3, 103]];
        let executor = ValuesExecutor {
            values: values
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|&v| BoundExpr::constant(DataValue::Int32(v)))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
        };
        let output = executor.execute().boxed().next().await.unwrap().unwrap();
        let expected = [
            ArrayImpl::Int32((0..4).collect()),
            ArrayImpl::Int32((100..104).collect()),
        ]
        .into_iter()
        .collect::<DataChunk>();
        assert_eq!(output, expected);
    }
}
