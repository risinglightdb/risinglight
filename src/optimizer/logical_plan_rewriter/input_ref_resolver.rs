// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::binder::*;
use crate::catalog::ColumnRefId;

/// Resolves column references into physical indices into the `DataChunk`.
///
/// This will rewrite all `BoundExpr` expressions to `InputRef`.
#[derive(Default)]
pub struct InputRefResolver {
    /// The output columns of the last visited plan.
    ///
    /// For those plans that don't change columns (e.g. Order, Filter), this variable should
    /// not be touched. For other plans that change columns (e.g. SeqScan, Join, Projection,
    /// Aggregate), this variable should be set before the function returns.
    bindings: Vec<Option<BoundExpr>>,
}

impl ExprRewriter for InputRefResolver {
    fn rewrite_expr(&self, expr: &mut BoundExpr) {
        use BoundExpr::*;
        if let Some(idx) = self
            .bindings
            .iter()
            .position(|col| *col == Some(expr.clone()))
        {
            *expr = InputRef(BoundInputRef {
                index: idx,
                return_type: expr.return_type().unwrap(),
            });
            return;
        }

        match expr {
            AggCall(agg) => {
                for expr in &mut agg.args {
                    self.rewrite_expr(expr);
                }
            }
            // rewrite sub-expressions
            BinaryOp(binary_op) => {
                self.rewrite_expr(&mut *binary_op.left_expr);
                self.rewrite_expr(&mut *binary_op.right_expr);
            }
            UnaryOp(unary_op) => {
                self.rewrite_expr(&mut *unary_op.expr);
            }
            TypeCast(cast) => {
                self.rewrite_expr(&mut *cast.expr);
            }
            IsNull(isnull) => {
                self.rewrite_expr(&mut *isnull.expr);
            }
            ExprWithAlias(expr_with_alias) => {
                self.rewrite_expr(&mut *expr_with_alias.expr);
            }
            _ => {}
        }
    }
}

impl PlanRewriter for InputRefResolver {
    fn rewrite_logical_join(&mut self, join: &LogicalJoin) -> PlanRef {
        let left = self.rewrite(join.left());
        let mut resolver = Self::default();
        let right = resolver.rewrite(join.right());
        self.bindings.append(&mut resolver.bindings);
        Arc::new(join.clone_with_rewrite_expr(left, right, self))
    }

    fn rewrite_logical_table_scan(&mut self, plan: &LogicalTableScan) -> PlanRef {
        self.bindings = plan
            .column_ids()
            .iter()
            .zip(plan.column_descs())
            .map(|(col_id, col_desc)| {
                Some(BoundExpr::ColumnRef(BoundColumnRef {
                    column_ref_id: ColumnRefId::from_table(plan.table_ref_id(), *col_id),
                    is_primary_key: col_desc.is_primary(),
                    desc: col_desc.clone(),
                }))
            })
            .collect();
        Arc::new(plan.clone())
    }

    fn rewrite_internal(&mut self, plan: &Internal) -> PlanRef {
        self.bindings = plan
            .column_ids()
            .iter()
            .zip(plan.column_descs())
            .map(|(col_id, col_desc)| {
                Some(BoundExpr::ColumnRef(BoundColumnRef {
                    column_ref_id: ColumnRefId::from_table(plan.table_ref_id(), *col_id),
                    is_primary_key: col_desc.is_primary(),
                    desc: col_desc.clone(),
                }))
            })
            .collect();
        Arc::new(plan.clone())
    }

    fn rewrite_logical_projection(&mut self, proj: &LogicalProjection) -> PlanRef {
        let new_child = self.rewrite(proj.child());
        let bindings = proj
            .project_expressions()
            .iter()
            .map(|e| Some(e.clone()))
            .collect();
        let ret = Arc::new(proj.clone_with_rewrite_expr(new_child, self));
        self.bindings = bindings;
        ret
    }

    fn rewrite_logical_aggregate(&mut self, agg: &LogicalAggregate) -> PlanRef {
        let new_child = self.rewrite(agg.child());
        let bindings = agg.group_keys().iter().map(|e| Some(e.clone())).collect();
        let ret = Arc::new(agg.clone_with_rewrite_expr(new_child, self));
        self.bindings = bindings;
        ret
    }
    fn rewrite_logical_filter(&mut self, plan: &LogicalFilter) -> PlanRef {
        let child = self.rewrite(plan.child());
        Arc::new(plan.clone_with_rewrite_expr(child, self))
    }
    fn rewrite_logical_order(&mut self, plan: &LogicalOrder) -> PlanRef {
        let child = self.rewrite(plan.child());
        Arc::new(plan.clone_with_rewrite_expr(child, self))
    }
    fn rewrite_logical_values(&mut self, plan: &LogicalValues) -> PlanRef {
        Arc::new(plan.clone_with_rewrite_expr(self))
    }
}
