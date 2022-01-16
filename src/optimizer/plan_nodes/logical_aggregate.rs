use std::fmt;

use super::*;
use crate::binder::{BoundAggCall, BoundExpr};
use crate::optimizer::logical_plan_rewriter::ExprRewriter;

/// The logical plan of hash aggregate operation.
#[derive(Debug, Clone)]
pub struct LogicalAggregate {
    agg_calls: Vec<BoundAggCall>,
    /// Group keys in hash aggregation (optional)
    group_keys: Vec<BoundExpr>,
    child: PlanRef,
    data_types: Vec<DataType>,
}

impl LogicalAggregate {
    pub fn new(agg_calls: Vec<BoundAggCall>, group_keys: Vec<BoundExpr>, child: PlanRef) -> Self {
        let data_types = group_keys
            .iter()
            .map(|expr| expr.return_type().unwrap())
            .chain(
                agg_calls
                    .iter()
                    .map(|agg_call| agg_call.return_type.clone()),
            )
            .collect();
        LogicalAggregate {
            agg_calls,
            group_keys,
            child,
            data_types,
        }
    }

    /// Get a reference to the logical aggregate's agg calls.
    pub fn agg_calls(&self) -> &[BoundAggCall] {
        self.agg_calls.as_ref()
    }

    /// Get a reference to the logical aggregate's group keys.
    pub fn group_keys(&self) -> &[BoundExpr] {
        self.group_keys.as_ref()
    }

    pub fn clone_with_rewrite_expr(&self, new_child: PlanRef, rewriter: impl ExprRewriter) -> Self {
        let new_agg_calls = self
            .agg_calls()
            .iter()
            .cloned()
            .map(|agg_call| BoundAggCall {
                kind: agg_call.kind,
                args: agg_call
                    .args
                    .iter()
                    .cloned()
                    .map(|expr| rewriter.rewrite_expr(&mut expr))
                    .collect(),
                return_type: agg_call.return_type,
            })
            .collect();
        let new_keys = self
            .group_keys()
            .iter()
            .cloned()
            .map(|expr| rewriter.rewrite_expr(&mut expr))
            .collect();
        LogicalAggregate::new(new_agg_calls, new_keys, new_child)
    }
}

impl PlanTreeNodeUnary for LogicalAggregate {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.agg_calls(), self.group_keys(), child)
    }
}
impl_plan_tree_node_for_unary!(LogicalAggregate);
impl PlanNode for LogicalAggregate {
    fn out_types(&self) -> Vec<DataType> {
        self.data_types.clone()
    }
}
impl fmt::Display for LogicalAggregate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalAggregate: {} agg calls", self.agg_calls.len(),)
    }
}
