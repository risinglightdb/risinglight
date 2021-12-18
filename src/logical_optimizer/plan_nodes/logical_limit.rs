use std::fmt;

use super::*;

/// The logical plan of limit operation.
#[derive(Debug, Clone)]
pub struct LogicalLimit {
    pub offset: usize,
    pub limit: usize,
    pub child: PlanRef,
}

impl_plan_node!(LogicalLimit, [child]);

impl fmt::Display for LogicalLimit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalLimit: offset: {}, limit: {}",
            self.offset, self.limit
        )
    }
}
