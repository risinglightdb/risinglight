// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::types::DataType;
use crate::v1::binder::BoundExpr;
use crate::v1::optimizer::logical_plan_rewriter::ExprRewriter;

/// The logical plan of `VALUES`.
#[derive(Debug, Clone, Serialize)]
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
    pub fn values(&self) -> &[Vec<BoundExpr>] {
        self.values.as_ref()
    }
    pub fn clone_with_rewrite_expr(&self, rewriter: &impl ExprRewriter) -> Self {
        let mut values = self.values().to_vec();
        for row in &mut values {
            for expr in row {
                rewriter.rewrite_expr(expr);
            }
        }

        LogicalValues::new(self.column_types().to_vec(), values)
    }
}
impl PlanTreeNodeLeaf for LogicalValues {}
impl_plan_tree_node_for_leaf!(LogicalValues);

impl PlanNode for LogicalValues {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.values[0]
            .iter()
            .map(|expr| {
                let name = "?column?".to_string();
                expr.return_type().to_column(name)
            })
            .collect()
    }

    fn prune_col(&self, required_cols: BitSet) -> PlanRef {
        let types: Vec<_> = required_cols
            .iter()
            .map(|index| self.column_types[index].clone())
            .collect();

        let new_values: Vec<_> = self
            .values
            .iter()
            .map(|row_expr| {
                required_cols
                    .iter()
                    .map(|index| row_expr[index].clone())
                    .collect()
            })
            .collect();

        LogicalValues::new(types, new_values).into_plan_ref()
    }
}

impl fmt::Display for LogicalValues {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalValues: {} rows", self.values.len())
    }
}
