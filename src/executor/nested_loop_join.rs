use super::*;
use std::vec::Vec;
use crate::array::{Array, ArrayImpl, DataChunk};
use crate::binder::BoundJoinOperator;
// The executor for nested loop join
pub struct NestedLoopJoinExecutor {
    left_child: BoxedExecutor,
    right_child: BoxedExecutor,
    join_op: BoundJoinOperator,

}

impl NestedLoopJoinExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream!{
            let left_chunks: Vec<DataChunk> = vec![];
            let right_chunks: Vec<DataChunk> = vec![];
            yield Err(ExecutorError::BuildingPlanError);
        }
    }

}