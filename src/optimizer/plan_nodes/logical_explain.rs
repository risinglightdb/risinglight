// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::types::DataTypeKind;

/// The logical plan of `EXPLAIN`.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalExplain {
    plan: PlanRef,
}

impl LogicalExplain {
    pub fn new(plan: PlanRef) -> Self {
        Self { plan }
    }

    /// Get a reference to the logical explain's plan.
    pub fn plan(&self) -> &dyn PlanNode {
        self.plan.as_ref()
    }
}
impl PlanTreeNodeUnary for LogicalExplain {
    fn child(&self) -> PlanRef {
        self.plan.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(child)
    }
}
impl_plan_tree_node_for_unary!(LogicalExplain);

impl PlanNode for LogicalExplain {
    fn prune_col(&self, _required_cols: BitSet) -> PlanRef {
        let out_types_num = self.plan.out_types().len();
        self.clone_with_child(self.plan.prune_col(BitSet::from_iter(0..out_types_num)))
            .into_plan_ref()
    }
    fn schema(&self) -> Vec<ColumnDesc> {
        vec![ColumnDesc::new(
            DataType::new(DataTypeKind::Int32, false),
            "$explain".to_string(),
            false,
        )]
    }
}

impl fmt::Display for LogicalExplain {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Explain:")
    }
}
