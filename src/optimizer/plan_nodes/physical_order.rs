// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use super::*;


/// The physical plan of order.
#[derive(Debug, Clone)]
pub struct PhysicalOrder {
    logical: LogicalOrder,
}

impl PhysicalOrder {
    pub fn new(logical: LogicalOrder) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical order's logical.
    pub fn logical(&self) -> &LogicalOrder {
        &self.logical
    }
}

impl PlanTreeNodeUnary for PhysicalOrder {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logical().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalOrder);
impl PlanNode for PhysicalOrder {
    fn out_types(&self) -> Vec<DataType> {
        self.logical().out_types()
    }
}
impl fmt::Display for PhysicalOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalOrder: {:?}", self.logical().comparators())
    }
}
