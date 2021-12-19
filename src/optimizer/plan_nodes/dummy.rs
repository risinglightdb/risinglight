use std::fmt;

use super::*;

/// A dummy plan.
#[derive(Debug, Clone)]
pub struct Dummy {}

impl_plan_tree_node!(Dummy);
impl PlanNode for Dummy {}
impl fmt::Display for Dummy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Dummy:")
    }
}