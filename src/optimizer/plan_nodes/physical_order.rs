use std::fmt;

use super::*;
use crate::binder::BoundOrderBy;

/// The physical plan of order.
#[derive(Debug, Clone)]
pub struct PhysicalOrder {
    pub comparators: Vec<BoundOrderBy>,
    pub child: PlanRef,
}

impl_plan_tree_node!(PhysicalOrder, [child]);
impl PlanNode for PhysicalOrder {
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        for cmp in &mut self.comparators {
            rewriter.rewrite_expr(&mut cmp.expr);
        }
    }
    fn out_types(&self) -> Vec<DataType> {
        self.child.out_types()
    }
}
impl fmt::Display for PhysicalOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalOrder: {:?}", self.comparators)
    }
}
