use std::fmt;

use super::{impl_plan_tree_node_for_leaf, Plan, PlanRef, PlanTreeNode};
use crate::binder::statement::drop::Object;

/// The logical plan of `drop`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalDrop {
    pub object: Object,
}
impl_plan_tree_node_for_leaf! {LogicalDrop}

impl fmt::Display for LogicalDrop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
