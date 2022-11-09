// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::v1::binder::{BoundJoinOperator, ExprVisitor};
use crate::v1::optimizer::logical_plan_rewriter::ExprRewriter;

/// The logical plan of join, it only records join tables and operators.
///
/// The query optimizer should decide the join orders and specific algorithms (hash join, nested
/// loop join or index join).
#[derive(Debug, Clone, Serialize)]
pub struct LogicalJoin {
    left_plan: PlanRef,
    right_plan: PlanRef,
    join_op: BoundJoinOperator,
    predicate: JoinPredicate,
    schema: Vec<ColumnDesc>,
}

impl LogicalJoin {
    pub fn new(
        left_plan: PlanRef,
        right_plan: PlanRef,
        join_op: BoundJoinOperator,
        predicate: JoinPredicate,
    ) -> Self {
        let mut schema = left_plan.schema();
        schema.append(&mut right_plan.schema());
        LogicalJoin {
            left_plan,
            right_plan,
            join_op,
            predicate,
            schema,
        }
    }
    pub fn create(
        left_plan: PlanRef,
        right_plan: PlanRef,
        join_op: BoundJoinOperator,
        on_clause: BoundExpr,
    ) -> Self {
        let left_col_num = left_plan.out_types().len();
        Self::new(
            left_plan,
            right_plan,
            join_op,
            JoinPredicate::create(left_col_num, on_clause),
        )
    }

    /// Get a reference to the logical join's join op.
    pub fn join_op(&self) -> BoundJoinOperator {
        self.join_op
    }

    pub fn clone_with_rewrite_expr(
        &self,
        left: PlanRef,
        right: PlanRef,
        rewriter: &impl ExprRewriter,
    ) -> Self {
        let left_col_num = left.out_types().len();
        let new_predicate = self
            .predicate()
            .clone_with_rewrite_expr(left_col_num, rewriter);
        LogicalJoin::new(left, right, self.join_op(), new_predicate)
    }

    /// Get a reference to the logical join's predicate.
    pub fn predicate(&self) -> &JoinPredicate {
        &self.predicate
    }
}
impl PlanTreeNodeBinary for LogicalJoin {
    fn left(&self) -> PlanRef {
        self.left_plan.clone()
    }
    fn right(&self) -> PlanRef {
        self.right_plan.clone()
    }

    #[must_use]
    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(left, right, self.join_op(), self.predicate().clone())
    }
}
impl_plan_tree_node_for_binary!(LogicalJoin);
impl PlanNode for LogicalJoin {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.schema.clone()
    }

    fn estimated_cardinality(&self) -> usize {
        self.left().estimated_cardinality() * self.right().estimated_cardinality()
    }

    fn prune_col(&self, required_cols: BitSet) -> PlanRef {
        let mut on_clause = self.predicate.to_on_clause();
        let mut visitor = CollectRequiredCols(required_cols.clone());
        visitor.visit_expr(&on_clause);
        let input_cols = visitor.0;

        let mapper = Mapper::new_with_bitset(&input_cols);
        mapper.rewrite_expr(&mut on_clause);

        let left_schema_len = self.left_plan.out_types().len();
        let left_input_cols = input_cols
            .iter()
            .filter(|&col_idx| col_idx < left_schema_len)
            .collect::<BitSet>();
        let right_input_cols = input_cols
            .iter()
            .filter(|&col_idx| col_idx >= left_schema_len)
            .map(|col_idx| col_idx - left_schema_len)
            .collect();

        let join_predicate = JoinPredicate::create(left_input_cols.len(), on_clause);

        let new_join = LogicalJoin::new(
            self.left_plan.prune_col(left_input_cols),
            self.right_plan.prune_col(right_input_cols),
            self.join_op,
            join_predicate,
        );

        if required_cols == input_cols {
            new_join.into_plan_ref()
        } else {
            let out_types = self.out_types();
            let project_expressions = required_cols
                .iter()
                .map(|col_idx| {
                    BoundExpr::InputRef(BoundInputRef {
                        index: mapper[col_idx],
                        return_type: out_types[col_idx].clone(),
                    })
                })
                .collect();
            LogicalProjection::new(project_expressions, new_join.into_plan_ref()).into_plan_ref()
        }
    }
}

impl fmt::Display for LogicalJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalJoin: op {:?}", self.join_op)
    }
}
