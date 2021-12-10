use super::super::plan_nodes::{
    logical_aggregate::LogicalAggregate, logical_join::LogicalJoin,
    logical_projection::LogicalProjection, logical_seq_scan::LogicalSeqScan, LogicalPlan,
    LogicalPlanRef, UnaryLogicalPlanNode,
};
use crate::{binder::*, catalog::ColumnRefId, logical_optimizer::plan_rewriter::PlanRewriter};
/// Resolves column references into physical indices into the `DataChunk`.
///
/// This will rewrite all `ColumnRef` expressions to `InputRef`.
#[derive(Default)]
pub struct InputRefResolver {
    /// The output columns of the last visited plan.
    ///
    /// For those plans that don't change columns (e.g. Order, Filter), this variable should
    /// not be touched. For other plans that change columns (e.g. SeqScan, Join, Projection,
    /// Aggregate), this variable should be set before the function returns.
    bindings: Vec<Option<ColumnRefId>>,
}

impl PlanRewriter for InputRefResolver {
    fn rewrite_join(&mut self, plan: &LogicalJoin) -> Option<LogicalPlanRef> {
        use BoundJoinConstraint::*;
        use BoundJoinOperator::*;

        let left_plan = self.rewrite_plan(plan.left_plan.clone());
        let mut resolver = Self::default();
        let right_plan = resolver.rewrite_plan(plan.right_plan.clone());
        self.bindings.append(&mut resolver.bindings);

        Some(
            LogicalPlan::LogicalJoin(LogicalJoin {
                left_plan,
                right_plan,
                join_op: match plan.join_op.clone() {
                    Inner(On(expr)) => Inner(On(self.rewrite_expr(expr))),
                    LeftOuter(On(expr)) => LeftOuter(On(self.rewrite_expr(expr))),
                    RightOuter(On(expr)) => RightOuter(On(self.rewrite_expr(expr))),
                },
            })
            .into(),
        )
    }

    fn rewrite_seqscan(&mut self, plan: &LogicalSeqScan) -> Option<LogicalPlanRef> {
        self.bindings = plan
            .column_ids
            .iter()
            .map(|col_id| Some(ColumnRefId::from_table(plan.table_ref_id, *col_id)))
            .collect();
        None
    }

    fn rewrite_projection(&mut self, plan: &LogicalProjection) -> Option<LogicalPlanRef> {
        let child = self.rewrite_plan(plan.child());
        let mut bindings = vec![];
        let project_expressions = plan
            .project_expressions
            .iter()
            .cloned()
            .map(|expr| {
                bindings.push(match &expr {
                    BoundExpr::ColumnRef(col) => Some(col.column_ref_id),
                    _ => None,
                });
                self.rewrite_expr(expr)
            })
            .collect();
        self.bindings = bindings;
        Some(
            LogicalPlan::LogicalProjection(LogicalProjection {
                project_expressions,
                child,
            })
            .into(),
        )
    }

    fn rewrite_aggregate(&mut self, plan: &LogicalAggregate) -> Option<LogicalPlanRef> {
        let child = self.rewrite_plan(plan.child());

        let agg_calls = plan
            .agg_calls
            .iter()
            .cloned()
            .map(|agg| BoundAggCall {
                kind: agg.kind,
                args: agg
                    .args
                    .into_iter()
                    .map(|expr| self.rewrite_expr(expr))
                    .collect(),
                return_type: agg.return_type,
            })
            .collect();

        let group_keys = plan
            .group_keys
            .iter()
            .cloned()
            .map(|expr| {
                match &expr {
                    BoundExpr::ColumnRef(col) => self.bindings.push(Some(col.column_ref_id)),
                    _ => panic!("{:?} cannot be a group key", expr),
                }
                self.rewrite_expr(expr)
            })
            .collect();
        Some(
            LogicalPlan::LogicalAggregate(LogicalAggregate {
                agg_calls,
                group_keys,
                child,
            })
            .into(),
        )
    }

    /// Transform expr referring to input chunk into `BoundInputRef`
    fn rewrite_expr(&mut self, expr: BoundExpr) -> BoundExpr {
        use BoundExpr::*;
        match expr {
            ColumnRef(column_ref) => InputRef(BoundInputRef {
                index: self
                    .bindings
                    .iter()
                    .position(|col| *col == Some(column_ref.column_ref_id))
                    .expect("column reference not found"),
                return_type: column_ref.desc.datatype().clone(),
            }),
            AggCall(agg) => AggCall(BoundAggCall {
                kind: agg.kind,
                args: agg
                    .args
                    .into_iter()
                    .map(|expr| self.rewrite_expr(expr))
                    .collect(),
                return_type: agg.return_type,
            }),
            // rewrite sub-expressions
            BinaryOp(binary_op) => BinaryOp(BoundBinaryOp {
                op: binary_op.op,
                left_expr: (self.rewrite_expr(*binary_op.left_expr).into()),
                right_expr: (self.rewrite_expr(*binary_op.right_expr).into()),
                return_type: binary_op.return_type,
            }),
            UnaryOp(unary_op) => UnaryOp(BoundUnaryOp {
                op: unary_op.op,
                expr: (self.rewrite_expr(*unary_op.expr).into()),
                return_type: unary_op.return_type,
            }),
            TypeCast(cast) => TypeCast(BoundTypeCast {
                expr: (self.rewrite_expr(*cast.expr).into()),
                ty: cast.ty,
            }),
            IsNull(isnull) => IsNull(BoundIsNull {
                expr: Box::new(self.rewrite_expr(*isnull.expr)),
            }),
            expr => expr,
        }
    }
}
