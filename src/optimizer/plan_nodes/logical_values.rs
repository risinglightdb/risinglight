use std::alloc::Global;
use std::fmt;

use super::*;
use crate::binder::BoundExpr;
use crate::types::DataType;

/// The logical plan of `VALUES`.
#[derive(Debug, Clone)]
pub struct LogicalValues {
    column_types: Vec<DataType>,
    values: Vec<Vec<BoundExpr>>,
}

impl LogicalValues {
    pub fn new(column_types: Vec<DataType>, values: Vec<Vec<BoundExpr>>) -> Self {
        Self {
            column_types,
            values,
        }
    }

    /// Get a reference to the logical values's column types.
    pub fn column_types(&self) -> &[DataType] {
        self.column_types.as_ref()
    }

    /// Get a reference to the logical values's values.
    pub fn values(&self) -> &[Vec<BoundExpr, Global>] {
        self.values.as_ref()
    }
}
impl PlanTreeNodeLeaf for LogicalValues {}
impl_plan_tree_node_for_leaf!(LogicalValues);

impl PlanNode for LogicalValues {
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        for row in &mut self.values {
            for expr in row {
                rewriter.rewrite_expr(expr);
            }
        }
    }
    fn out_types(&self) -> Vec<DataType> {
        self.column_types.clone()
    }
}

impl fmt::Display for LogicalValues {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalValues: {} rows", self.values.len())
    }
}
