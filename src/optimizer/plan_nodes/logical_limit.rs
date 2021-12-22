use std::fmt;

use super::*;

/// The logical plan of limit operation.
#[derive(Debug, Clone)]
pub struct LogicalLimit {
    pub offset: usize,
    pub limit: usize,
    pub child: PlanRef,
}

impl_plan_tree_node!(LogicalLimit, [child]);
impl PlanNode for LogicalLimit {
    fn out_types(&self) -> Vec<DataType> {
        self.child.out_types()
    }
}

impl fmt::Display for LogicalLimit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalLimit: offset: {}, limit: {}",
            self.offset, self.limit
        )
    }
}
