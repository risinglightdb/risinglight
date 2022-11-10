// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The physical plan of project operation.
#[derive(Debug, Clone, Serialize)]
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
    #[allow(dead_code)]
    fn estimated_cardinality(&self) -> usize {
        self.child().estimated_cardinality()
    }
}

impl PlanTreeNodeUnary for PhysicalProjection {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logical().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalProjection);
impl PlanNode for PhysicalProjection {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.logical().schema()
    }
}

impl fmt::Display for PhysicalProjection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalProjection:")?;
        for expr in self.logical().project_expressions().iter() {
            writeln!(f, "  {}", expr)?
        }
        Ok(())
    }
}
