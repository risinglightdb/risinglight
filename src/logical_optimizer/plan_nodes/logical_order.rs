use std::fmt;

use super::*;
use crate::binder::BoundOrderBy;

/// The logical plan of order.
#[derive(Debug, Clone)]
pub struct LogicalOrder {
    pub comparators: Vec<BoundOrderBy>,
    pub child: PlanRef,
}

impl_plan_node!(LogicalOrder, [child]
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        for cmp in &mut self.comparators {
            rewriter.rewrite_expr(&mut cmp.expr);
        }
    }
);

impl fmt::Display for LogicalOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalOrder: {:?}", self.comparators)
    }
}
