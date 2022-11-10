// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::{DataChunk, DataChunkBuilder};
use crate::types::{DataType, DataTypeKind};

/// The executor of `values`.
pub struct ValuesExecutor {
    pub column_types: Vec<DataType>,
    /// Each row is composed of multiple values,
    /// each value is represented by an expression.
    pub values: Vec<Vec<RecExpr>>,
}

impl ValuesExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        type Type = DataTypeKind;
        let mut builder = DataChunkBuilder::new(self.column_types.iter(), PROCESSING_WINDOW_SIZE);
        let dummy = DataChunk::single(0);
        for row in self.values {
            let row_data: Vec<_> = (row.into_iter().zip_eq(&self.column_types))
                .map(|(expr, ty)| Evaluator::new(&expr).eval(&dummy)?.get(0).cast(&ty.kind))
                .try_collect()?;
            if let Some(chunk) = builder.push_row(row_data) {
                yield chunk;
            }
        }
        if let Some(chunk) = builder.take() {
            yield chunk;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ArrayImpl;
    use crate::types::DataTypeKind;

    #[tokio::test]
    async fn values() {
        let values = [[0, 100], [1, 101], [2, 102], [3, 103]];
        let executor = ValuesExecutor {
            column_types: vec![DataTypeKind::Int32.nullable(); 2],
            values: values
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|&v| format!("{v}").parse().unwrap())
                        .collect_vec()
                })
                .collect_vec(),
        };
        let output = executor.execute().next().await.unwrap().unwrap();
        let expected = [
            ArrayImpl::new_int32((0..4).collect()),
            ArrayImpl::new_int32((100..104).collect()),
        ]
        .into_iter()
        .collect::<DataChunk>();
        assert_eq!(output, expected);
    }
}
