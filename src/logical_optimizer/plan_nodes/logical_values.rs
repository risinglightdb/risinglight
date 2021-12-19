use std::fmt;

use super::*;
use crate::binder::BoundExpr;
use crate::types::DataType;

/// The logical plan of `VALUES`.
#[derive(Debug, Clone)]
pub struct LogicalValues {
    pub column_types: Vec<DataType>,
    pub values: Vec<Vec<BoundExpr>>,
}

impl_plan_node!(LogicalValues, []
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        for row in &mut self.values {
            for expr in row {
                rewriter.rewrite_expr(expr);
            }
        }
    }
);

impl fmt::Display for LogicalValues {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalValues: {} rows", self.values.len())
    }
}
