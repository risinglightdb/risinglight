// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use itertools::izip;

use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::binder::BoundExpr;
use crate::types::{DataType, DataTypeKind};

/// The executor of `values`.
pub struct ValuesExecutor {
    pub column_types: Vec<DataType>,
    /// Each row is composed of multiple values,
    /// each value is represented by an expression.
    pub values: Vec<Vec<BoundExpr>>,
}

impl ValuesExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        for chunk in self.values.chunks(PROCESSING_WINDOW_SIZE) {
            type Type = DataTypeKind;
            // Create array builders.
            let column_types = &self.column_types;
            let mut builders = column_types
                .iter()
                .map(|ty| ArrayBuilderImpl::with_capacity(chunk.len(), ty))
                .collect_vec();
            // Push value into the builder.
            let dummy = DataChunk::single(0);
            for row in chunk {
                for (expr, column_type, builder) in izip!(row, column_types, &mut builders) {
                    let value = expr.eval(&dummy)?;
                    let size = match column_type.kind {
                        Type::Varchar(size) => size,
                        Type::Char(size) => size,
                        _ => None,
                    };
                    if let Some(width) = size {
                        let item_length = if let DataValue::String(x) = &value.get(0) {
                            x.len() as u64
                        } else if let DataValue::Null = &value.get(0) {
                            0
                        } else {
                            unreachable!()
                        };
                        if item_length > width {
                            return Err(ExecutorError::ExceedLengthLimit {
                                length: item_length,
                                width,
                            });
                        }
                    }
                    builder.push(&value.get(0));
                }
            }
            // Finish build and yield chunk.
            yield builders.into_iter().collect();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ArrayImpl;
    use crate::binder::BoundExpr;
    use crate::types::{DataTypeExt, DataTypeKind, DataValue};

    #[tokio::test]
    async fn values() {
        let values = [[0, 100], [1, 101], [2, 102], [3, 103]];
        let executor = ValuesExecutor {
            column_types: vec![DataTypeKind::Int(None).nullable(); 2],
            values: values
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|&v| BoundExpr::Constant(DataValue::Int32(v)))
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
