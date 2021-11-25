use crate::binder::*;
use crate::catalog::ColumnRefId;
use crate::logical_optimizer::plan_node::UnaryLogicalPlanNode;
use crate::logical_optimizer::plan_rewriter::PlanRewriter;
use crate::logical_planner::*;

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

        let relation_plan = self.rewrite_plan(plan.relation_plan.clone());
        // TODO: Make the order of bindings consistent with the output order in executor
        let join_table_plans = plan
            .join_table_plans
            .iter()
            .cloned()
            .map(|plan| {
                let mut resolver = Self::default();
                let table_plan = resolver.rewrite_plan(plan.table_plan.clone());
                self.bindings.append(&mut resolver.bindings);

                LogicalJoinTable {
                    table_plan: (table_plan),
                    join_op: match plan.join_op {
                        Inner(On(expr)) => Inner(On(self.rewrite_expr(expr))),
                    },
                }
            })
            .collect();
        Some(
            LogicalPlan::LogicalJoin(LogicalJoin {
                relation_plan,
                // TODO: implement `rewrite_join` whens `plan.join_table_plans` is not empty
                join_table_plans,
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
        let child = self.rewrite_plan(plan.get_child());
        let mut bindings = vec![];
        let project_expressions = plan
            .project_expressions
            .iter()
            .cloned()
            .map(|expr| {
                bindings.push(match &expr.kind {
                    BoundExprKind::ColumnRef(col) => Some(col.column_ref_id),
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
        let child = self.rewrite_plan(plan.get_child());

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
                match &expr.kind {
                    BoundExprKind::ColumnRef(col) => self.bindings.push(Some(col.column_ref_id)),
                    _ => panic!("{:?} cannot be a group key", expr.kind),
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
        use BoundExprKind::*;
        let new_kind = match expr.kind {
            ColumnRef(column_ref) => InputRef(BoundInputRef {
                index: self
                    .bindings
                    .iter()
                    .position(|col| *col == Some(column_ref.column_ref_id))
                    .expect("column reference not found"),
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
                left_expr: (self.rewrite_expr(*binary_op.left_expr).into()),
                op: binary_op.op,
                right_expr: (self.rewrite_expr(*binary_op.right_expr).into()),
            }),
            UnaryOp(unary_op) => UnaryOp(BoundUnaryOp {
                op: unary_op.op,
                expr: (self.rewrite_expr(*unary_op.expr).into()),
            }),
            TypeCast(cast) => TypeCast(BoundTypeCast {
                expr: (self.rewrite_expr(*cast.expr).into()),
                ty: cast.ty,
            }),
            IsNull(isnull) => IsNull(BoundIsNull {
                expr: Box::new(self.rewrite_expr(*isnull.expr)),
            }),
            kind => kind,
        };
        BoundExpr {
            kind: new_kind,
            return_type: expr.return_type,
        }
    }
}
