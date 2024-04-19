// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicU64, Ordering};

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
        let rows = self.profiler.rows();

        // explain the plan
        let explain_obj = Explain::of(&self.plan)
            .with_rows(&rows)
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

/// A collection of profiling information for each node in the query plan.
#[derive(Default)]
pub struct Profiler {
    spans: HashMap<Id, TimeSpan>,
    rows: HashMap<Id, Counter>,
}

impl Profiler {
    pub fn register(&mut self, id: Id, span: TimeSpan, rows: Counter) {
        self.spans.insert(id, span);
        self.rows.insert(id, rows);
    }

    pub fn busy_time(&self) -> HashMap<Id, Duration> {
        self.spans
            .iter()
            .map(|(&id, span)| (id, span.busy_time()))
            .collect()
    }

    pub fn rows(&self) -> HashMap<Id, u64> {
        self.rows
            .iter()
            .map(|(&id, rows)| (id, rows.get()))
            .collect()
    }
}

#[derive(Default, Clone)]
pub struct Counter {
    count: Arc<AtomicU64>,
}

impl Counter {
    pub fn inc(&self, value: u64) {
        self.count.fetch_add(value, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}
