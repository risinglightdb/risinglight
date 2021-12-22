use std::fmt;

use super::*;

/// The physical plan of limit operation.
#[derive(Debug, Clone)]
pub struct PhysicalLimit {
    pub offset: usize,
    pub limit: usize,
    pub child: PlanRef,
}

impl_plan_tree_node!(PhysicalLimit, [child]);
impl PlanNode for PhysicalLimit {
    fn out_types(&self) -> Vec<DataType> {
        self.child.out_types()
    }
}

impl fmt::Display for PhysicalLimit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalLimit: offset: {}, limit: {}",
            self.offset, self.limit
        )
    }
}
