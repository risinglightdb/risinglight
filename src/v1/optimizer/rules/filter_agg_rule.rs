// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use bit_set::BitSet;

use super::*;
use crate::v1::binder::BoundExpr;
use crate::v1::optimizer::expr_utils::{conjunctions, input_col_refs, merge_conjunctions};
use crate::v1::optimizer::logical_plan_rewriter::ExprRewriter;
use crate::v1::optimizer::plan_nodes::{LogicalFilter, PlanTreeNodeUnary};

pub struct FilterAggRule {}

impl Rule for FilterAggRule {
    fn apply(&self, plan: PlanRef) -> Result<PlanRef, ()> {
        let filter = plan.as_logical_filter()?;
        let child = filter.child();
        let agg = child.as_logical_aggregate()?;
        let filter_cond = filter.expr().clone();

        let agg_calls_num = agg.agg_calls().len();
        let group_keys_num = agg.group_keys().len();

        let mut bitset = BitSet::new();
        for i in group_keys_num..group_keys_num + agg_calls_num {
            bitset.insert(i);
        }
        let conditions = conjunctions(filter_cond);
        let mut pushed_conditions = Vec::new();
        let mut remaining_conditions = Vec::new();
        // We cannot push down expressions that contain aggregate function
        for cond in conditions {
            let cond_bitset = input_col_refs(&cond);
            if cond_bitset.is_disjoint(&bitset) {
                pushed_conditions.push(cond);
            } else {
                remaining_conditions.push(cond);
            }
        }

        if pushed_conditions.is_empty() {
            return Ok(plan.clone());
        }

        let input_ref_rewriter = InputRefRewriter {
            input_refs: agg.group_keys(),
        };

        // rewrite the expression bindings
        for condition in &mut pushed_conditions {
            input_ref_rewriter.rewrite_expr(condition);
        }

        let pushed_cond = merge_conjunctions(pushed_conditions.into_iter());

        let agg_input = agg.child();
        let pushed_filter = Arc::new(LogicalFilter::new(pushed_cond, agg_input));
        let agg = Arc::new(agg.clone_with_child(pushed_filter));

        if remaining_conditions.is_empty() {
            return Ok(agg);
        }

        let remaining_filter = merge_conjunctions(remaining_conditions.into_iter());
        Ok(Arc::new(LogicalFilter::new(remaining_filter, agg)))
    }
}

struct InputRefRewriter<'a> {
    input_refs: &'a [BoundExpr],
}

impl<'a> ExprRewriter for InputRefRewriter<'a> {
    fn rewrite_input_ref(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::InputRef(bref) => {
                *expr = self.input_refs[bref.index].clone();
            }
            _ => unreachable!(),
        }
    }
}
