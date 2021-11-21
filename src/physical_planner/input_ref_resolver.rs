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
    bindings: Vec<ColumnRefId>,
    /// Aggregations collected from expressions.
    agg_calls: Vec<BoundAggCall>,
}

impl PlanRewriter for InputRefResolver {
    fn rewrite_join(&mut self, plan: LogicalJoin) -> LogicalPlan {
        use BoundJoinConstraint::*;
        use BoundJoinOperator::*;

        let relation_plan = self.rewrite_plan(*plan.relation_plan);
        // TODO: Make the order of bindings consistent with the output order in executor
        let join_table_plans = plan
            .join_table_plans
            .into_iter()
            .map(|plan| {
                let mut resolver = Self::default();
                let table_plan = resolver.rewrite_plan(*plan.table_plan);
                self.bindings.append(&mut resolver.bindings);

                LogicalJoinTable {
                    table_plan: Box::new(table_plan),
                    join_op: match plan.join_op {
                        Inner(On(expr)) => Inner(On(self.rewrite_expr(expr))),
                    },
                }
            })
            .collect();

        LogicalPlan::Join(LogicalJoin {
            relation_plan: Box::new(relation_plan),
            // TODO: implement `rewrite_join` when `plan.join_table_plans` is not empty
            join_table_plans,
        })
    }

    fn rewrite_seqscan(&mut self, plan: LogicalSeqScan) -> LogicalPlan {
        self.bindings = plan
            .column_ids
            .iter()
            .map(|col_id| ColumnRefId::from_table(plan.table_ref_id, *col_id))
            .collect();
        LogicalPlan::SeqScan(plan)
    }

    fn rewrite_projection(&mut self, plan: LogicalProjection) -> LogicalPlan {
        let mut child = self.rewrite_plan(*plan.child);

        self.agg_calls.clear();
        let project_expressions = plan
            .project_expressions
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();

        // Push agg calls into the agg plan
        if !self.agg_calls.is_empty() {
            match &mut child {
                LogicalPlan::Aggregate(agg) => agg.agg_calls.append(&mut self.agg_calls),
                _ => panic!("Logical plan for aggregation is not found"),
            }
            // Re-resolve agg calls here as the arguments in agg calls should be resolved by
            // the bindings from the child plan of the agg plan
            child = self.rewrite_plan(child);
        }
        LogicalPlan::Projection(LogicalProjection {
            project_expressions,
            child: Box::new(child),
        })
    }

    fn rewrite_aggregate(&mut self, plan: LogicalAggregate) -> LogicalPlan {
        let child = self.rewrite_plan(*plan.child);

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

        // Only return the bindings of group keys and let projection resolver decide the agg
        // call bindings
        self.bindings.clear();
        let group_keys = plan
            .group_keys
            .into_iter()
            .map(|expr| {
                match &expr.kind {
                    BoundExprKind::ColumnRef(col) => self.bindings.push(col.column_ref_id),
                    // When hash agg is resolved again, the parent resolver will not use its
                    // bindings
                    BoundExprKind::InputRef(_) => {}
                    _ => panic!("{:?} cannot be a group key", expr.kind),
                }
                self.rewrite_expr(expr)
            })
            .collect();

        LogicalPlan::Aggregate(LogicalAggregate {
            agg_calls,
            group_keys,
            child: Box::new(child),
        })
    }

    /// Transform expr referring to input chunk into `BoundInputRef`
    fn rewrite_expr(&mut self, expr: BoundExpr) -> BoundExpr {
        use BoundExprKind::*;
        let new_kind = match expr.kind {
            ColumnRef(column_ref) => InputRef(BoundInputRef {
                index: {
                    let index = self
                        .bindings
                        .iter()
                        .position(|col| *col == column_ref.column_ref_id)
                        .expect("column reference not found");
                    index
                },
            }),
            AggCall(agg) => {
                // Current agg call is appended at the rightmost of the output chunk. `bindings`
                // here is the index for group keys for further column binding resolving.
                let index = self.bindings.len() + self.agg_calls.len();
                self.agg_calls.push(agg);
                InputRef(BoundInputRef { index })
            }
            // rewrite sub-expressions
            BinaryOp(binary_op) => BinaryOp(BoundBinaryOp {
                left_expr: Box::new(self.rewrite_expr(*binary_op.left_expr)),
                op: binary_op.op,
                right_expr: Box::new(self.rewrite_expr(*binary_op.right_expr)),
            }),
            UnaryOp(unary_op) => UnaryOp(BoundUnaryOp {
                op: unary_op.op,
                expr: Box::new(self.rewrite_expr(*unary_op.expr)),
            }),
            TypeCast(cast) => TypeCast(BoundTypeCast {
                expr: Box::new(self.rewrite_expr(*cast.expr)),
                ty: cast.ty,
            }),
            kind => kind,
        };
        BoundExpr {
            kind: new_kind,
            return_type: expr.return_type,
        }
    }
}
