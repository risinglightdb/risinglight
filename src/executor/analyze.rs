// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use pretty_xmlish::PrettyConfig;

use super::*;
use crate::array::{ArrayImpl, StringArray};
use crate::planner::Explain;

/// Run the query and return the query plan with profiling information.
pub struct AnalyzeExecutor {
    pub plan: RecExpr,
    pub catalog: RootCatalogRef,
    pub profiler: Profiler,
}

impl AnalyzeExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        // consume the child stream
        #[for_await]
        for chunk in child {
            _ = chunk?;
        }
        // take profiling information
        let busy_time = self.profiler.busy_time();

        // explain the plan
        let explain_obj = Explain::of(&self.plan)
            .with_times(&busy_time)
            .with_catalog(&self.catalog);
        let explainer = explain_obj.pretty();
        let mut explain = String::with_capacity(4096);
        let mut config = PrettyConfig {
            need_boundaries: false,
            ..PrettyConfig::default()
        };
        config.unicode(&mut explain, &explainer);
        let chunk = DataChunk::from_iter([ArrayImpl::new_string(StringArray::from_iter([Some(
            explain,
        )]))]);

        yield chunk;
    }
}
