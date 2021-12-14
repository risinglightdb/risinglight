use std::fmt;

use super::{impl_plan_tree_node_for_leaf, Plan, PlanRef, PlanTreeNode};
use crate::binder::BoundExpr;
use crate::types::DataType;

/// The logical plan of `VALUES`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalValues {
    pub column_types: Vec<DataType>,
    pub values: Vec<Vec<BoundExpr>>,
}
impl_plan_tree_node_for_leaf! {LogicalValues}

impl fmt::Display for LogicalValues {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalValues: {} rows", self.values.len())
    }
}
