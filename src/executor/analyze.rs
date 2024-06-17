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
    pub metrics: Metrics,
}

impl AnalyzeExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        // consume the child stream
        #[for_await]
        for chunk in child {
            _ = chunk?;
        }

        // explain the plan
        let get_metadata = |id| {
            vec![
                ("rows", self.metrics.get_rows(id).to_string()),
                ("time", format!("{:?}", self.metrics.get_time(id))),
            ]
        };
        let explain_obj = Explain::of(&self.plan)
            .with_catalog(&self.catalog)
            .with_metadata(&get_metadata);
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

/// A collection of profiling information for a query.
#[derive(Default)]
pub struct Metrics {
    spans: HashMap<Id, TimeSpan>,
    rows: HashMap<Id, Counter>,
}

impl Metrics {
    /// Register metrics for a node.
    pub fn register(&mut self, id: Id, span: TimeSpan, rows: Counter) {
        self.spans.insert(id, span);
        self.rows.insert(id, rows);
    }

    /// Get the running time for a node.
    pub fn get_time(&self, id: Id) -> Duration {
        self.spans.get(&id).map(|span| span.busy_time()).unwrap()
    }

    /// Get the number of rows produced by a node.
    pub fn get_rows(&self, id: Id) -> u64 {
        self.rows.get(&id).map(|rows| rows.get()).unwrap()
    }
}

/// A counter.
#[derive(Default, Clone)]
pub struct Counter {
    count: Arc<AtomicU64>,
}

impl Counter {
    /// Increments the counter.
    pub fn inc(&self, value: u64) {
        self.count.fetch_add(value, Ordering::Relaxed);
    }

    /// Gets the current value of the counter.
    pub fn get(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}
