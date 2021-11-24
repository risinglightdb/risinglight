use crate::{binder::*, logical_planner::*};

mod arith_expr_simplification;
mod bool_expr_simplification;
mod constant_folding;
mod constant_moving;

use arith_expr_simplification::*;
use bool_expr_simplification::*;
use constant_folding::*;
use constant_moving::*;
/// The optimizer will do query optimization.
///
/// It will do both rule-based optimization (predicate pushdown, constant folding and common
/// expression extraction) , and cost-based optimization (Join reordering and join algorithm
/// selection). It takes LogicalPlan as input and returns a new LogicalPlan which could be used to
/// generate phyiscal plan.
#[derive(Default)]
pub struct Optimizer {}

impl Optimizer {
    pub fn optimize(&mut self, plan: LogicalPlan) -> LogicalPlan {
        // TODO: add optimization rules
        let mut plan = ConstantFolding.rewrite_plan(plan);
        plan = ArithExprSimplification.rewrite_plan(plan);
        plan = BoolExprSimplification.rewrite_plan(plan);
        ConstantMovingRule.rewrite_plan(plan)
    }
}

// PlanRewriter is a plan visitor.
// User could implement the own optimization rules by implement PlanRewriter trait easily.
// NOTE: the visitor should always visit child plan first.
pub trait PlanRewriter {
    fn rewrite_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Dummy => LogicalPlan::Dummy,
            LogicalPlan::CreateTable(plan) => self.rewrite_create_table(plan),
            LogicalPlan::Drop(plan) => self.rewrite_drop(plan),
            LogicalPlan::Insert(plan) => self.rewrite_insert(plan),
            LogicalPlan::Join(plan) => self.rewrite_join(plan),
            LogicalPlan::SeqScan(plan) => self.rewrite_seqscan(plan),
            LogicalPlan::Projection(plan) => self.rewrite_projection(plan),
            LogicalPlan::Filter(plan) => self.rewrite_filter(plan),
            LogicalPlan::Order(plan) => self.rewrite_order(plan),
            LogicalPlan::Limit(plan) => self.rewrite_limit(plan),
            LogicalPlan::Explain(plan) => self.rewrite_explain(plan),
            LogicalPlan::Aggregate(plan) => self.rewrite_aggregate(plan),
            LogicalPlan::Delete(plan) => self.rewrite_delete(plan),
            LogicalPlan::Values(plan) => self.rewrite_values(plan),
            LogicalPlan::CopyFromFile(plan) => self.rewrite_copy_from_file(plan),
            LogicalPlan::CopyToFile(plan) => self.rewrite_copy_to_file(plan),
        }
    }

    fn rewrite_create_table(&mut self, plan: LogicalCreateTable) -> LogicalPlan {
        LogicalPlan::CreateTable(plan)
    }

    fn rewrite_drop(&mut self, plan: LogicalDrop) -> LogicalPlan {
        LogicalPlan::Drop(plan)
    }

    fn rewrite_insert(&mut self, plan: LogicalInsert) -> LogicalPlan {
        LogicalPlan::Insert(LogicalInsert {
            child: self.rewrite_plan(plan.child.as_ref().clone()).into(),
            table_ref_id: plan.table_ref_id,
            column_ids: plan.column_ids,
        })
    }

    fn rewrite_join(&mut self, plan: LogicalJoin) -> LogicalPlan {
        let relation_plan = self.rewrite_plan(plan.relation_plan.as_ref().clone());
        let mut join_table_plans = vec![];
        for plan in plan.join_table_plans.into_iter() {
            use BoundJoinConstraint::*;
            use BoundJoinOperator::*;
            join_table_plans.push(LogicalJoinTable {
                table_plan: self.rewrite_plan(plan.table_plan.as_ref().clone()).into(),
                join_op: match plan.join_op {
                    Inner(On(expr)) => Inner(On(self.rewrite_expr(expr))),
                },
            });
        }
        LogicalPlan::Join(LogicalJoin {
            relation_plan: relation_plan.into(),
            join_table_plans,
        })
    }

    fn rewrite_seqscan(&mut self, plan: LogicalSeqScan) -> LogicalPlan {
        LogicalPlan::SeqScan(plan)
    }

    fn rewrite_projection(&mut self, plan: LogicalProjection) -> LogicalPlan {
        LogicalPlan::Projection(LogicalProjection {
            child: self.rewrite_plan(plan.child.as_ref().clone()).into(),
            project_expressions: plan
                .project_expressions
                .into_iter()
                .map(|expr| self.rewrite_expr(expr))
                .collect(),
        })
    }

    fn rewrite_filter(&mut self, plan: LogicalFilter) -> LogicalPlan {
        LogicalPlan::Filter(LogicalFilter {
            child: self.rewrite_plan(plan.child.as_ref().clone()).into(),
            expr: self.rewrite_expr(plan.expr),
        })
    }

    fn rewrite_order(&mut self, plan: LogicalOrder) -> LogicalPlan {
        LogicalPlan::Order(LogicalOrder {
            child: self.rewrite_plan(plan.child.as_ref().clone()).into(),
            comparators: plan
                .comparators
                .into_iter()
                .map(|orderby| BoundOrderBy {
                    expr: self.rewrite_expr(orderby.expr),
                    descending: orderby.descending,
                })
                .collect(),
        })
    }

    fn rewrite_limit(&mut self, plan: LogicalLimit) -> LogicalPlan {
        LogicalPlan::Limit(LogicalLimit {
            child: self.rewrite_plan(plan.child.as_ref().clone()).into(),
            offset: plan.offset,
            limit: plan.limit,
        })
    }

    fn rewrite_explain(&mut self, plan: LogicalExplain) -> LogicalPlan {
        LogicalPlan::Explain(LogicalExplain {
            plan: self.rewrite_plan(plan.plan.as_ref().clone()).into(),
        })
    }

    fn rewrite_aggregate(&mut self, plan: LogicalAggregate) -> LogicalPlan {
        LogicalPlan::Aggregate(LogicalAggregate {
            child: self.rewrite_plan(plan.child.as_ref().clone()).into(),
            agg_calls: plan
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
                .collect(),
            group_keys: plan.group_keys,
        })
    }

    fn rewrite_delete(&mut self, plan: LogicalDelete) -> LogicalPlan {
        LogicalPlan::Delete(LogicalDelete {
            table_ref_id: plan.table_ref_id,
            filter: LogicalFilter {
                child: self.rewrite_plan(plan.filter.child.as_ref().clone()).into(),
                expr: self.rewrite_expr(plan.filter.expr),
            },
        })
    }

    fn rewrite_values(&mut self, plan: LogicalValues) -> LogicalPlan {
        LogicalPlan::Values(plan)
    }

    fn rewrite_copy_from_file(&mut self, plan: LogicalCopyFromFile) -> LogicalPlan {
        LogicalPlan::CopyFromFile(plan)
    }

    fn rewrite_copy_to_file(&mut self, plan: LogicalCopyToFile) -> LogicalPlan {
        LogicalPlan::CopyToFile(LogicalCopyToFile {
            child: self.rewrite_plan(plan.child.as_ref().clone()).into(),
            path: plan.path,
            format: plan.format,
            column_types: plan.column_types,
        })
    }

    fn rewrite_expr(&mut self, expr: BoundExpr) -> BoundExpr {
        expr
    }
}
