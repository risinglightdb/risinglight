use std::fmt;

use super::*;

/// The physical plan of project operation.
#[derive(Debug, Clone)]
pub struct PhysicalProjection {
    logical: LogicalProjection,
}

impl PhysicalProjection {
    pub fn new(logical: LogicalProjection) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical projection's logical.
    pub fn logical(&self) -> &LogicalProjection {
        &self.logical
    }
}

impl PlanTreeNodeUnary for PhysicalProjection {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logcial().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalProjection);
impl PlanNode for PhysicalProjection {
    fn out_types(&self) -> Vec<DataType> {
        self.logical().out_types()
    }
}

impl fmt::Display for PhysicalProjection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalProjection: exprs {:?}",
            self.project_expressions
        )
    }
}
