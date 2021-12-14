use std::fmt;

use super::{impl_plan_tree_node_for_leaf, Plan, PlanRef, PlanTreeNode};
use crate::binder::Object;

/// The physical plan of `drop`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalDrop {
    pub object: Object,
}

impl fmt::Display for PhysicalDrop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
impl_plan_tree_node_for_leaf! {PhysicalDrop}
