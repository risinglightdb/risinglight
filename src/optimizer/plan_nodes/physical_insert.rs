// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use itertools::Itertools;
use serde::Serialize;

use super::*;

/// The physical plan of `INSERT`.
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalInsert {
    logical: LogicalInsert,
}

impl PhysicalInsert {
    pub fn new(logical: LogicalInsert) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical insert's logical.
    pub fn logical(&self) -> &LogicalInsert {
        &self.logical
    }
}

impl PlanTreeNodeUnary for PhysicalInsert {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logical().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalInsert);
impl PlanNode for PhysicalInsert {}

impl fmt::Display for PhysicalInsert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalInsert: table {}, columns [{}]",
            self.logical().table_ref_id().table_id,
            self.logical()
                .column_ids()
                .iter()
                .map(ToString::to_string)
                .join(", ")
        )
    }
}
