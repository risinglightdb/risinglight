// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::binder::BoundExpr;
use crate::optimizer::plan_nodes::{
    LogicalFilter, LogicalJoin, PlanTreeNodeBinary, PlanTreeNodeUnary,
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
        let join_cond = BoundExpr::BinaryOp(BoundBinaryOp {
            op: And,
            left_expr: Box::new(join.condition().clone()),
            right_expr: Box::new(filter.expr().clone()),
            return_type: Some(DataTypeKind::Boolean.nullable()),
        });
        let new_join = Arc::new(LogicalJoin::new(
            join.left().clone(),
            join.right().clone(),
            join.join_op(),
            join_cond,
        ));
        // FIXME:
        // Currently HashJoinExecutor ignores the condition,
        // so for correctness we have to keep filter operator.
        let new_filter = Arc::new(LogicalFilter::new(filter.expr().clone(), new_join));
        Ok(new_filter)

        // TODO: we need schema of operator to push condition to each side.
        // let filter_conds = to_cnf(filter.expr.clone());
        // let join_cond = match join.join_op {
        //     Inner(On(op)) => op.clone(),
        //     _ => unreachable!(),
        // };
        // let join_conds = to_cnf(join_cond);
        // let left_filter_expr = vec![];
        // let right_filter_expr = vec![];
        // let join_filter_expr = vec![];

        // for cond in filter_conds.into_iter().chain(join_conds.into_iter()) {
        //     let input_refs = input_col_refs(&cond);
        //     let in_left = false;
        //     let in_right = false;
        //     for index in input_refs.iter() {
        //         if(index <=)
        //     }
        // }
    }
}
