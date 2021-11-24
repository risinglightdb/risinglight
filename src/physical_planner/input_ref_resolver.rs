use crate::binder::*;
use crate::catalog::ColumnRefId;
use crate::logical_planner::*;
use crate::optimizer::PlanRewriter;

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
    fn rewrite_join(&mut self, plan: LogicalJoin) -> LogicalPlan {
        use BoundJoinConstraint::*;
        use BoundJoinOperator::*;

        let relation_plan = self.rewrite_plan(plan.relation_plan.as_ref().clone());
        // TODO: Make the order of bindings consistent with the output order in executor
        let join_table_plans = plan
            .join_table_plans
            .into_iter()
            .map(|plan| {
                let mut resolver = Self::default();
                let table_plan = resolver.rewrite_plan(plan.table_plan.as_ref().clone());
                self.bindings.append(&mut resolver.bindings);

                LogicalJoinTable {
                    table_plan: (table_plan.into()),
                    join_op: match plan.join_op {
                        Inner(On(expr)) => Inner(On(self.rewrite_expr(expr))),
                    },
                }
            })
            .collect();

        LogicalPlan::Join(LogicalJoin {
            relation_plan: relation_plan.into(),
            // TODO: implement `rewrite_join` when `plan.join_table_plans` is not empty
            join_table_plans,
        })
    }

    fn rewrite_seqscan(&mut self, plan: LogicalSeqScan) -> LogicalPlan {
        self.bindings = plan
            .column_ids
            .iter()
            .map(|col_id| Some(ColumnRefId::from_table(plan.table_ref_id, *col_id)))
            .collect();
        LogicalPlan::SeqScan(plan)
    }

    fn rewrite_projection(&mut self, plan: LogicalProjection) -> LogicalPlan {
        let child = self.rewrite_plan(plan.child.as_ref().clone());
        let mut bindings = vec![];
        let project_expressions = plan
            .project_expressions
            .into_iter()
            .map(|expr| {
                bindings.push(match &expr.kind {
                    BoundExprKind::ColumnRef(col) => Some(col.column_ref_id),
                    _ => None,
                });
                self.rewrite_expr(expr)
            })
            .collect();
        self.bindings = bindings;
        LogicalPlan::Projection(LogicalProjection {
            project_expressions,
            child: child.into(),
        })
    }

    fn rewrite_aggregate(&mut self, plan: LogicalAggregate) -> LogicalPlan {
        let child = self.rewrite_plan(plan.child.as_ref().clone());

        let agg_calls = plan
            .agg_calls
            .into_iter()
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
            .into_iter()
            .map(|expr| {
                match &expr.kind {
                    BoundExprKind::ColumnRef(col) => self.bindings.push(Some(col.column_ref_id)),
                    _ => panic!("{:?} cannot be a group key", expr.kind),
                }
                self.rewrite_expr(expr)
            })
            .collect();

        LogicalPlan::Aggregate(LogicalAggregate {
            agg_calls,
            group_keys,
            child: child.into(),
        })
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
