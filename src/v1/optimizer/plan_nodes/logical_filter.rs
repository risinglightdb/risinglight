// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::v1::binder::{BoundExpr, ExprVisitor};
use crate::v1::optimizer::logical_plan_rewriter::ExprRewriter;

/// The logical plan of filter operation.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalFilter {
    expr: BoundExpr,
    child: PlanRef,
}

impl LogicalFilter {
    pub fn new(expr: BoundExpr, child: PlanRef) -> Self {
        Self { expr, child }
    }

    /// Get a reference to the logical filter's expr.
    pub fn expr(&self) -> &BoundExpr {
        &self.expr
    }
    pub fn clone_with_rewrite_expr(
        &self,
        new_child: PlanRef,
        rewriter: &impl ExprRewriter,
    ) -> Self {
        let mut new_expr = self.expr().clone();
        rewriter.rewrite_expr(&mut new_expr);
        LogicalFilter::new(new_expr, new_child)
    }
}
impl PlanTreeNodeUnary for LogicalFilter {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.expr().clone(), child)
    }
}

impl_plan_tree_node_for_unary!(LogicalFilter);
impl PlanNode for LogicalFilter {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.child.schema()
    }

    fn estimated_cardinality(&self) -> usize {
        self.child().estimated_cardinality()
    }

    fn prune_col(&self, required_cols: BitSet) -> PlanRef {
        let mut visitor = CollectRequiredCols(required_cols.clone());
        visitor.visit_expr(&self.expr);
        let input_cols = visitor.0;

        let mut expr = self.expr.clone();
        let mapper = Mapper::new_with_bitset(&input_cols);
        mapper.rewrite_expr(&mut expr);

        let need_prune = required_cols != input_cols;
        let new_filter = Self {
            expr,
            child: self.child.prune_col(input_cols),
        }
        .into_plan_ref();

        if !need_prune {
            return new_filter;
        }

        let input_types = self.out_types();
        let exprs = required_cols
            .iter()
            .map(|old_idx| {
                BoundExpr::InputRef(BoundInputRef {
                    index: mapper[old_idx],
                    return_type: input_types[old_idx].clone(),
                })
            })
            .collect();
        LogicalProjection::new(exprs, new_filter).into_plan_ref()
    }
}

impl fmt::Display for LogicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalFilter: expr {:?}", self.expr)
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::types::{DataTypeKind, DataValue};
    use crate::v1::binder::BoundBinaryOp;

    #[test]
    /// Pruning
    /// ```text
    /// Filter(cond: input_ref(1)<5)
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [1] will result in
    /// ```text
    /// Filter(cond: input_ref(0)<5)
    ///   TableScan(v2)
    /// ```
    fn test_prune_filter() {
        let ty = DataTypeKind::Int32.not_null();
        let col_descs = vec![
            ty.clone().to_column("v1".into()),
            ty.clone().to_column("v2".into()),
            ty.clone().to_column("v3".into()),
        ];
        let table_scan = LogicalTableScan::new(
            crate::catalog::TableRefId {
                database_id: 0,
                schema_id: 0,
                table_id: 0,
            },
            vec![1, 2, 3],
            col_descs.clone(),
            false,
            false,
            None,
        );
        let filter = LogicalFilter::new(
            BoundExpr::BinaryOp(BoundBinaryOp {
                op: BinaryOperator::Lt,
                left_expr: Box::new(BoundExpr::InputRef(BoundInputRef {
                    index: 1,
                    return_type: ty.clone(),
                })),
                right_expr: Box::new(BoundExpr::Constant(DataValue::Int32(5))),
                return_type: ty.clone(),
            }),
            table_scan.into_plan_ref(),
        );

        let mut required_cols = BitSet::new();
        required_cols.insert(1);
        let plan = filter.prune_col(required_cols);
        let plan = plan.as_logical_filter().unwrap();
        assert_eq!(
            plan.expr,
            BoundExpr::BinaryOp(BoundBinaryOp {
                op: BinaryOperator::Lt,
                left_expr: Box::new(BoundExpr::InputRef(BoundInputRef {
                    index: 0,
                    return_type: ty.clone(),
                })),
                right_expr: Box::new(BoundExpr::Constant(DataValue::Int32(5))),
                return_type: ty,
            })
        );
        let child = plan.child.as_logical_table_scan().unwrap();
        assert_eq!(child.column_descs(), &col_descs[1..2]);
        assert_eq!(child.column_ids(), &[2]);
    }

    #[test]
    /// Pruning
    /// ```text
    /// Filter(cond: input_ref(1)<5)
    ///   TableScan(v1, v2, v3, v4)
    /// ```
    /// with required columns [3] will result in
    /// ```text
    /// Project(expr0 = inputref(1))
    ///   Filter(cond: input_ref(0)<5)
    ///     TableScan(v2, v4)
    /// ```
    fn test_prune_filter_with_project() {
        let ty = DataTypeKind::Int32.not_null();
        let col_descs = vec![
            ty.clone().to_column("v1".into()),
            ty.clone().to_column("v2".into()),
            ty.clone().to_column("v3".into()),
            ty.clone().to_column("v4".into()),
        ];
        let table_scan = LogicalTableScan::new(
            crate::catalog::TableRefId {
                database_id: 0,
                schema_id: 0,
                table_id: 0,
            },
            vec![1, 2, 3, 4],
            col_descs,
            false,
            false,
            None,
        );
        let filter = LogicalFilter::new(
            BoundExpr::BinaryOp(BoundBinaryOp {
                op: BinaryOperator::Lt,
                left_expr: Box::new(BoundExpr::InputRef(BoundInputRef {
                    index: 1,
                    return_type: ty.clone(),
                })),
                right_expr: Box::new(BoundExpr::Constant(DataValue::Int32(5))),
                return_type: ty,
            }),
            table_scan.into_plan_ref(),
        );

        let mut required_cols = BitSet::new();
        required_cols.insert(3);
        let plan = filter.prune_col(required_cols);
        let plan = plan.as_logical_projection().unwrap();
        let child = plan.child();
        let child = child.as_logical_filter().unwrap();
        let input_types = child.out_types();
        assert_eq!(plan.project_expressions().len(), 1);
        assert_eq!(
            plan.project_expressions()[0],
            BoundExpr::InputRef(BoundInputRef {
                index: 1,
                return_type: input_types[1].clone(),
            })
        );
        let table_scan = child.child.as_logical_table_scan().unwrap();
        assert_eq!(table_scan.column_ids(), &[2, 4]);
    }
}
