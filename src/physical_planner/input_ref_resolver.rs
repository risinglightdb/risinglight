use super::*;
use crate::binder::{
    BoundAggCall, BoundBinaryOp, BoundExpr, BoundExprKind, BoundInputRef, BoundJoinConstraint,
    BoundJoinOperator, BoundOrderBy, BoundTypeCast, BoundUnaryOp,
};
use crate::catalog::ColumnRefId;
use crate::logical_planner::{
    LogicalCopyFromFile, LogicalCopyToFile, LogicalCreateTable, LogicalDelete, LogicalDrop,
    LogicalExplain, LogicalFilter, LogicalHashAgg, LogicalInsert, LogicalJoin, LogicalJoinTable,
    LogicalLimit, LogicalOrder, LogicalProjection, LogicalSeqScan, LogicalSimpleAgg, LogicalValues,
};
use itertools::Itertools;

/// Transform expr referring to input chunk into `BoundInputRef`
fn transform_expr(
    expr: BoundExpr,
    bindings: &[ColumnRefId],
    agg_calls: &mut Vec<BoundAggCall>,
) -> BoundExpr {
    match expr.kind {
        BoundExprKind::Constant(value) => BoundExpr {
            kind: BoundExprKind::Constant(value),
            return_type: expr.return_type,
        },
        BoundExprKind::ColumnRef(column_ref) => BoundExpr {
            kind: BoundExprKind::InputRef(BoundInputRef {
                index: bindings
                    .iter()
                    .position(|col_binding| *col_binding == column_ref.column_ref_id)
                    .unwrap(),
            }),
            return_type: expr.return_type,
        },
        BoundExprKind::BinaryOp(binary_op) => BoundExpr {
            kind: BoundExprKind::BinaryOp(BoundBinaryOp {
                left_expr: Box::new(transform_expr(*binary_op.left_expr, bindings, agg_calls)),
                op: binary_op.op,
                right_expr: Box::new(transform_expr(*binary_op.right_expr, bindings, agg_calls)),
            }),
            return_type: expr.return_type,
        },
        BoundExprKind::UnaryOp(unary_op) => BoundExpr {
            kind: BoundExprKind::UnaryOp(BoundUnaryOp {
                op: unary_op.op,
                expr: Box::new(transform_expr(*unary_op.expr, bindings, agg_calls)),
            }),
            return_type: expr.return_type,
        },
        BoundExprKind::TypeCast(cast) => BoundExpr {
            kind: BoundExprKind::TypeCast(BoundTypeCast {
                expr: Box::new(transform_expr(*cast.expr, bindings, agg_calls)),
                ty: cast.ty,
            }),
            return_type: expr.return_type,
        },
        BoundExprKind::AggCall(agg) => {
            // Current agg call is appended at the rightmost of the output chunk. `bindings` here is
            // the index for group keys for further column binding resolving.
            let index = bindings.len() + agg_calls.len();
            agg_calls.push(agg);
            BoundExpr {
                kind: BoundExprKind::InputRef(BoundInputRef { index }),
                return_type: expr.return_type,
            }
        }
        BoundExprKind::InputRef(input_ref) => {
            // Simple agg and hash agg might be transformed twice
            BoundExpr {
                kind: BoundExprKind::InputRef(BoundInputRef {
                    index: input_ref.index,
                }),
                return_type: expr.return_type,
            }
        }
    }
}

fn transform_agg_args(agg_calls: Vec<BoundAggCall>, bindings: &[ColumnRefId]) -> Vec<BoundAggCall> {
    let mut inner_agg_calls = vec![];
    agg_calls
        .into_iter()
        .map(|agg| BoundAggCall {
            kind: agg.kind,
            args: agg
                .args
                .into_iter()
                .map(|arg| transform_expr(arg, bindings, &mut inner_agg_calls))
                .collect(),
            return_type: agg.return_type,
        })
        .collect()
}

/// Resolve input reference and replace expressions to `BoundInputRef`
#[derive(Default)]
pub struct InputRefResolver {}

impl InputRefResolver {
    pub fn resolve_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let (plan, _) = self.resolve_plan_inner(plan);
        plan
    }

    /// Return resolved logical plan and input reference bindings
    fn resolve_plan_inner(&mut self, plan: LogicalPlan) -> (LogicalPlan, Vec<ColumnRefId>) {
        match plan {
            LogicalPlan::Dummy => (LogicalPlan::Dummy, vec![]),
            LogicalPlan::CreateTable(plan) => self.resolve_create_table(plan),
            LogicalPlan::Drop(plan) => self.resolve_drop(plan),
            LogicalPlan::Insert(plan) => self.resolve_insert(plan),
            LogicalPlan::Join(plan) => self.resolve_join(plan),
            LogicalPlan::SeqScan(plan) => self.resolve_seq_scan(plan),
            LogicalPlan::Projection(plan) => self.resolve_projection(plan),
            LogicalPlan::Filter(plan) => self.resolve_filter(plan),
            LogicalPlan::Order(plan) => self.resolve_order(plan),
            LogicalPlan::Limit(plan) => self.resolve_limit(plan),
            LogicalPlan::Explain(plan) => self.resolve_explain(plan),
            LogicalPlan::Delete(plan) => self.resolve_delete(plan),
            LogicalPlan::SimpleAgg(plan) => self.resolve_simple_agg(plan),
            LogicalPlan::HashAgg(plan) => self.resolve_hash_agg(plan),
            LogicalPlan::Values(plan) => self.resolve_values(plan),
            LogicalPlan::CopyFromFile(plan) => self.resolve_copy_from_file(plan),
            LogicalPlan::CopyToFile(plan) => self.resolve_copy_to_file(plan),
        }
    }

    fn resolve_create_table(
        &mut self,
        plan: LogicalCreateTable,
    ) -> (LogicalPlan, Vec<ColumnRefId>) {
        (LogicalPlan::CreateTable(plan), vec![])
    }

    fn resolve_drop(&mut self, plan: LogicalDrop) -> (LogicalPlan, Vec<ColumnRefId>) {
        (LogicalPlan::Drop(plan), vec![])
    }

    fn resolve_insert(&mut self, plan: LogicalInsert) -> (LogicalPlan, Vec<ColumnRefId>) {
        (LogicalPlan::Insert(plan), vec![])
    }

    fn resolve_join(&mut self, plan: LogicalJoin) -> (LogicalPlan, Vec<ColumnRefId>) {
        let mut bindings: Vec<ColumnRefId> = vec![];
        let (relation_plan, mut relation_bindings) = self.resolve_plan_inner(*plan.relation_plan);
        bindings.append(&mut relation_bindings);
        // TODO: Make the order of bindings consistent with the output order in executor
        let mut inner_agg_calls = vec![];
        let join_table_plans = plan
            .join_table_plans
            .into_iter()
            .map(|join_table_plan| {
                let (table_plan, mut table_bindings) =
                    self.resolve_plan_inner(*join_table_plan.table_plan);
                bindings.append(&mut table_bindings);
                LogicalJoinTable {
                    table_plan: Box::new(table_plan),
                    join_op: match join_table_plan.join_op {
                        BoundJoinOperator::Inner(BoundJoinConstraint::On(expr)) => {
                            BoundJoinOperator::Inner(BoundJoinConstraint::On(transform_expr(
                                expr,
                                &bindings,
                                &mut inner_agg_calls,
                            )))
                        }
                    },
                }
            })
            .collect();
        (
            LogicalPlan::Join(LogicalJoin {
                relation_plan: Box::new(relation_plan),
                // TODO: implement `resolve_join` when `plan.join_table_plans` is not empty
                join_table_plans,
            }),
            bindings,
        )
    }

    fn resolve_seq_scan(&mut self, plan: LogicalSeqScan) -> (LogicalPlan, Vec<ColumnRefId>) {
        let bindings = plan
            .column_ids
            .iter()
            .map(|col_id| ColumnRefId {
                database_id: plan.table_ref_id.database_id,
                schema_id: plan.table_ref_id.schema_id,
                table_id: plan.table_ref_id.table_id,
                column_id: *col_id,
            })
            .collect();
        (LogicalPlan::SeqScan(plan), bindings)
    }

    fn resolve_projection(&mut self, plan: LogicalProjection) -> (LogicalPlan, Vec<ColumnRefId>) {
        let (mut child_plan, bindings) = self.resolve_plan_inner(*plan.child);
        // Collect agg calls
        let mut agg_calls = vec![];
        let project_expressions = plan
            .project_expressions
            .into_iter()
            .map(|expr| transform_expr(expr, &bindings, &mut agg_calls))
            .collect();

        // Push agg calls into the agg plan
        if !agg_calls.is_empty() {
            match child_plan {
                LogicalPlan::HashAgg(hash_agg) => {
                    child_plan = LogicalPlan::HashAgg(LogicalHashAgg {
                        agg_calls,
                        group_keys: hash_agg.group_keys,
                        child: hash_agg.child,
                    })
                }
                LogicalPlan::SimpleAgg(simple_agg) => {
                    child_plan = LogicalPlan::SimpleAgg(LogicalSimpleAgg {
                        agg_calls,
                        child: simple_agg.child,
                    })
                }
                _ => panic!("Logical plan for aggregation is not found"),
            }
            // Re-resolve agg calls here as the arguments in agg calls should be resolved by
            // the bindings from the child plan of the agg plan
            let (child_plan, bindings) = self.resolve_plan_inner(child_plan);
            (
                LogicalPlan::Projection(LogicalProjection {
                    project_expressions,
                    child: Box::new(child_plan),
                }),
                bindings,
            )
        } else {
            (
                LogicalPlan::Projection(LogicalProjection {
                    project_expressions,
                    child: Box::new(child_plan),
                }),
                bindings,
            )
        }
    }

    fn resolve_simple_agg(&mut self, plan: LogicalSimpleAgg) -> (LogicalPlan, Vec<ColumnRefId>) {
        let (child_plan, bindings) = self.resolve_plan_inner(*plan.child);
        (
            LogicalPlan::SimpleAgg(LogicalSimpleAgg {
                agg_calls: transform_agg_args(plan.agg_calls, &bindings),
                child: Box::new(child_plan),
            }),
            // Let projection resolver decide the agg call bindings
            vec![],
        )
    }

    fn resolve_hash_agg(&mut self, plan: LogicalHashAgg) -> (LogicalPlan, Vec<ColumnRefId>) {
        let (child_plan, bindings) = self.resolve_plan_inner(*plan.child);
        let agg_calls = transform_agg_args(plan.agg_calls, &bindings);
        let mut bindings = vec![];
        let mut inner_agg_calls = vec![];
        let group_keys = plan
            .group_keys
            .into_iter()
            .map(|expr| {
                match &expr.kind {
                    BoundExprKind::ColumnRef(column_ref) => bindings.push(column_ref.column_ref_id),
                    // When hash agg is resolved again, the parent resolver will not use its
                    // bindings
                    BoundExprKind::InputRef(_) => {}
                    _ => panic!("{:?} cannot be a group key", expr.kind),
                }
                transform_expr(expr, &bindings, &mut inner_agg_calls)
            })
            .collect_vec();

        (
            LogicalPlan::HashAgg(LogicalHashAgg {
                agg_calls,
                group_keys,
                child: Box::new(child_plan),
            }),
            // Only return the bindings of group keys and let projection resolver decide the agg
            // call bindings
            bindings,
        )
    }

    fn resolve_filter(&mut self, plan: LogicalFilter) -> (LogicalPlan, Vec<ColumnRefId>) {
        let (child_plan, bindings) = self.resolve_plan_inner(*plan.child);
        let mut agg_calls = vec![];
        let expr = transform_expr(plan.expr, &bindings, &mut agg_calls);
        (
            LogicalPlan::Filter(LogicalFilter {
                expr,
                child: Box::new(child_plan),
            }),
            bindings,
        )
    }

    fn resolve_order(&mut self, plan: LogicalOrder) -> (LogicalPlan, Vec<ColumnRefId>) {
        let (child_plan, bindings) = self.resolve_plan_inner(*plan.child);
        let mut agg_calls = vec![];
        let comparators = plan
            .comparators
            .into_iter()
            .map(|comp| BoundOrderBy {
                expr: transform_expr(comp.expr, &bindings, &mut agg_calls),
                descending: comp.descending,
            })
            .collect();
        (
            LogicalPlan::Order(LogicalOrder {
                comparators,
                child: Box::new(child_plan),
            }),
            bindings,
        )
    }

    fn resolve_limit(&mut self, plan: LogicalLimit) -> (LogicalPlan, Vec<ColumnRefId>) {
        let (child_plan, bindings) = self.resolve_plan_inner(*plan.child);
        (
            LogicalPlan::Limit(LogicalLimit {
                offset: plan.offset,
                limit: plan.limit,
                child: Box::new(child_plan),
            }),
            bindings,
        )
    }

    fn resolve_explain(&mut self, plan: LogicalExplain) -> (LogicalPlan, Vec<ColumnRefId>) {
        let (child_plan, bindings) = self.resolve_plan_inner(*plan.plan);
        (
            LogicalPlan::Explain(LogicalExplain {
                plan: Box::new(child_plan),
            }),
            bindings,
        )
    }

    fn resolve_delete(&mut self, plan: LogicalDelete) -> (LogicalPlan, Vec<ColumnRefId>) {
        let (filter, bindings) = self.resolve_filter(plan.filter);
        match filter {
            LogicalPlan::Filter(filter) => (
                LogicalPlan::Delete(LogicalDelete {
                    table_ref_id: plan.table_ref_id,
                    filter,
                }),
                bindings,
            ),
            _ => panic!("resolve_filter failed"),
        }
    }

    fn resolve_values(&mut self, plan: LogicalValues) -> (LogicalPlan, Vec<ColumnRefId>) {
        (LogicalPlan::Values(plan), vec![])
    }

    fn resolve_copy_from_file(
        &mut self,
        plan: LogicalCopyFromFile,
    ) -> (LogicalPlan, Vec<ColumnRefId>) {
        (LogicalPlan::CopyFromFile(plan), vec![])
    }

    fn resolve_copy_to_file(&mut self, plan: LogicalCopyToFile) -> (LogicalPlan, Vec<ColumnRefId>) {
        (LogicalPlan::CopyToFile(plan), vec![])
    }
}
