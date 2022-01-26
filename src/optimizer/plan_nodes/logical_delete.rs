// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;
use serde::{Serialize};
use super::*;
use crate::catalog::TableRefId;

/// The logical plan of `DELETE`.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalDelete {
    table_ref_id: TableRefId,
    child: PlanRef,
}

impl LogicalDelete {
    pub fn new(table_ref_id: TableRefId, child: PlanRef) -> Self {
        Self {
            table_ref_id,
            child,
        }
    }

    /// Get a reference to the logical delete's table ref id.
    pub fn table_ref_id(&self) -> TableRefId {
        self.table_ref_id
    }
}
impl PlanTreeNodeUnary for LogicalDelete {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.table_ref_id(), child)
    }
}
impl_plan_tree_node_for_unary!(LogicalDelete);
impl PlanNode for LogicalDelete {}

impl fmt::Display for LogicalDelete {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalDelete: table {}", self.table_ref_id.table_id)
    }
}
