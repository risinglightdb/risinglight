// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The physical plan of `DELETE`.
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalDelete {
    logical: LogicalDelete,
}

impl PhysicalDelete {
    pub fn new(logical: LogicalDelete) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical delete's logical.
    pub fn logical(&self) -> &LogicalDelete {
        &self.logical
    }
}

impl PlanTreeNodeUnary for PhysicalDelete {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logical().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalDelete);
impl PlanNode for PhysicalDelete {}
impl fmt::Display for PhysicalDelete {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalDelete: table {}",
            self.logical().table_ref_id().table_id
        )
    }
}
