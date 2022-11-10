// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::{ArrayImpl, Utf8Array};
use crate::v1::optimizer::plan_nodes::PhysicalExplain;

/// The executor of `explain` statement.
pub struct ExplainExecutor {
    pub plan: PhysicalExplain,
}

impl ExplainExecutor {
    pub fn execute(self) -> BoxedExecutor {
        let mut explain_result = String::new();
        self.plan.child().explain(0, &mut explain_result).unwrap();
        let chunk = DataChunk::from_iter([ArrayImpl::new_utf8(Utf8Array::from_iter([Some(
            explain_result,
        )]))]);
        async_stream::try_stream! {
            yield chunk;
        }
        .boxed()
    }
}
