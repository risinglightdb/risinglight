use std::fmt;

use super::*;
use crate::binder::BoundExpr;
use crate::types::DataType;

/// The physical plan of `VALUES`.
#[derive(Debug, Clone)]
pub struct PhysicalValues {
    pub column_types: Vec<DataType>,
    pub values: Vec<Vec<BoundExpr>>,
}

impl_plan_tree_node!(PhysicalValues);
impl PlanNode for PhysicalValues {}
impl fmt::Display for PhysicalValues {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalValues: {} rows", self.values.len())
    }
}
