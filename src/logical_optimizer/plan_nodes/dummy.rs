use std::fmt;

use super::{impl_plan_tree_node_for_leaf, Plan, PlanRef, PlanTreeNode};

#[derive(Debug, PartialEq, Clone)]
pub struct Dummy {}
impl fmt::Display for Dummy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Dummy")
    }
}
impl_plan_tree_node_for_leaf! {Dummy}
