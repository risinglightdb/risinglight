// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use futures::{future, stream};

use super::*;
use crate::array::{ArrayImpl, Utf8Array};
use crate::planner::{costs, Explain};

/// The executor of `explain` statement.
pub struct ExplainExecutor {
    pub plan: RecExpr,
    pub catalog: RootCatalogRef,
}

impl ExplainExecutor {
    pub fn execute(self) -> BoxedExecutor {
        let explain = format!(
            "{}",
            Explain::of(&self.plan)
                .with_costs(&costs(&self.plan))
                .with_catalog(&self.catalog)
        );
        let chunk =
            DataChunk::from_iter([ArrayImpl::new_utf8(Utf8Array::from_iter([Some(explain)]))]);

        stream::once(future::ok(chunk)).boxed()
    }
}
