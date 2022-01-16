use std::fmt;

use super::*;
use crate::binder::Object;

/// The physical plan of `DROP`.
#[derive(Debug, Clone)]
pub struct PhysicalDrop {
    logical: LogicalDrop,
}

impl PlanTreeNodeLeaf for LogicalCreateTable {}
impl_plan_tree_node_for_leaf!(LogicalCreateTable);
impl PlanNode for PhysicalDrop {}

impl fmt::Display for PhysicalDrop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
