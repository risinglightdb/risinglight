// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::catalog::TableRefId;
use crate::types::DataTypeKind;

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
impl PlanNode for LogicalDelete {
    fn schema(&self) -> Vec<ColumnDesc> {
        vec![ColumnDesc::new(
            DataType::new(DataTypeKind::Int32, false),
            "$delete.row_counts".to_string(),
            false,
        )]
    }

    fn prune_col(&self, _required_cols: BitSet) -> PlanRef {
        let input_cols = (0..self.child().out_types().len()).into_iter().collect();
        self.clone_with_child(self.child.prune_col(input_cols))
            .into_plan_ref()
    }
}

impl fmt::Display for LogicalDelete {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalDelete: table {}", self.table_ref_id.table_id)
    }
}
