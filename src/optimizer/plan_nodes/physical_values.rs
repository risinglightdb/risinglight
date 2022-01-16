use std::fmt;

use super::*;
use crate::binder::BoundExpr;
use crate::types::DataType;

/// The physical plan of `VALUES`.
#[derive(Debug, Clone)]
pub struct PhysicalValues {
    logical: LogicalValues,
}

impl PhysicalValues {
    pub fn new(logical: LogicalValues) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical values's logical.
    pub fn logical(&self) -> &LogicalValues {
        &self.logical
    }
}

impl PlanTreeNodeLeaf for PhysicalValues {}
impl_plan_tree_node_for_leaf!(PhysicalValues);
impl PlanNode for PhysicalValues {
    fn out_types(&self) -> Vec<DataType> {
        self.logical().out_types()
    }
}
impl fmt::Display for PhysicalValues {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalValues: {} rows", self.logical().values().len())
    }
}
