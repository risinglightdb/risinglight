// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;
use serde::{Serialize};
use super::*;

/// The logical plan of exchange.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalExchange {
    plan: PlanRef,
}

impl LogicalExchange {
    pub fn new(plan: PlanRef) -> Self {
        Self { plan }
    }

    /// Get a reference to the logical explain's plan.
    pub fn plan(&self) -> &dyn PlanNode {
        self.plan.as_ref()
    }
}
impl PlanTreeNodeUnary for LogicalExchange {
    fn child(&self) -> PlanRef {
        self.plan.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(child)
    }
}
impl_plan_tree_node_for_unary!(LogicalExchange);

impl PlanNode for LogicalExchange {}

impl fmt::Display for LogicalExchange {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Exchange:")
    }
}
