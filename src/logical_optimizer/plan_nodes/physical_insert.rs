use std::fmt;

use itertools::Itertools;

use super::{
    impl_plan_tree_node_for_leaf, impl_plan_tree_node_for_unary, Plan, PlanRef, PlanTreeNode,
    UnaryLogicalPlanNode,
};
use crate::binder::BoundExpr;
use crate::catalog::TableRefId;
use crate::types::{ColumnId, DataType};

/// The physical plan of `insert`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub child: PlanRef,
}

impl UnaryLogicalPlanNode for PhysicalInsert {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::PhysicalInsert(PhysicalInsert {
            child,
            table_ref_id: self.table_ref_id,
            column_ids: self.column_ids.clone(),
        })
        .into()
    }
}
impl_plan_tree_node_for_unary! {PhysicalInsert}

/// The physical plan of `values`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalValues {
    pub column_types: Vec<DataType>,
    pub values: Vec<Vec<BoundExpr>>,
}
impl_plan_tree_node_for_leaf! {PhysicalValues}

impl fmt::Display for PhysicalValues {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalValues: {} rows", self.values.len())
    }
}

impl fmt::Display for PhysicalInsert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalInsert: table {}, columns [{}]",
            self.table_ref_id.table_id,
            self.column_ids.iter().map(ToString::to_string).join(", ")
        )
    }
}
