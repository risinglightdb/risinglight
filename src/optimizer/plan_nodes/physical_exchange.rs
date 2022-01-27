// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The physical plan of exchange.
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalExchange {
    child: PlanRef,
}

impl PhysicalExchange {
    pub fn new(child: PlanRef) -> Self {
        Self { child }
    }
}

impl PlanTreeNodeUnary for PhysicalExchange {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self { child }
    }
}
impl_plan_tree_node_for_unary!(PhysicalExchange);
impl PlanNode for PhysicalExchange {}
impl fmt::Display for PhysicalExchange {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalExchange:")
    }
}
