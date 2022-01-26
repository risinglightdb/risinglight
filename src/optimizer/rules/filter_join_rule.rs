// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::binder::{BoundExpr, BoundJoinOperator};
use crate::optimizer::expr_utils::{merge_conjunctions, shift_input_col_refs};
use crate::optimizer::plan_nodes::{
    JoinPredicate, LogicalFilter, LogicalJoin, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::optimizer::BoundBinaryOp;
use crate::parser::BinaryOperator::And;
use crate::types::{DataTypeExt, DataTypeKind};

pub struct FilterJoinRule {}

impl Rule for FilterJoinRule {
    fn apply(&self, plan: PlanRef) -> Result<PlanRef, ()> {
        let filter = plan.as_logical_filter()?;
        let child = filter.child();
        let join = child.as_logical_join()?;
        if join.join_op() != BoundJoinOperator::Inner {
            return Err(());
        }
        let filter_cond = filter.expr().clone();
        let join_on_clause = join.predicate().to_on_clause();
        let new_inner_join_cond = BoundExpr::BinaryOp(BoundBinaryOp {
            op: And,
            left_expr: Box::new(filter_cond),
            right_expr: Box::new(join_on_clause),
            return_type: Some(DataTypeKind::Boolean.nullable()),
        });
        let left_col_num = join.left().out_types().len();
        let inner_join_predicate = JoinPredicate::create(left_col_num, new_inner_join_cond);
        let left = if inner_join_predicate.left_conds().is_empty() {
            join.left()
        } else {
            Arc::new(LogicalFilter::new(
                merge_conjunctions(inner_join_predicate.left_conds().iter().cloned()),
                join.left(),
            ))
        };
        let right = if inner_join_predicate.right_conds().is_empty() {
            join.right()
        } else {
            let mut right_cond =
                merge_conjunctions(inner_join_predicate.right_conds().iter().cloned());
            shift_input_col_refs(&mut right_cond, -(left_col_num as i32));
            Arc::new(LogicalFilter::new(right_cond, join.right()))
        };

        let new_join = Arc::new(LogicalJoin::create(
            left,
            right,
            join.join_op(),
            merge_conjunctions(
                inner_join_predicate
                    .eq_conds()
                    .into_iter()
                    .chain(inner_join_predicate.other_conds().iter().cloned()),
            ),
        ));
        Ok(new_join)
    }
}
