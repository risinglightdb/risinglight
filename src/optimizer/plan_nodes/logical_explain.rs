use std::fmt;

use super::*;

/// The logical plan of `EXPLAIN`.
#[derive(Debug, Clone)]
pub struct LogicalExplain {
    pub plan: PlanRef,
}

impl_plan_tree_node!(LogicalExplain, [plan]);
impl PlanNode for LogicalExplain {}

impl fmt::Display for LogicalExplain {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Explain:")
    }
}
