// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::v1::binder::statement::BoundOrderBy;
use crate::v1::binder::ExprVisitor;
use crate::v1::optimizer::logical_plan_rewriter::ExprRewriter;

/// The logical plan of top N operation.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalTopN {
    offset: usize,
    limit: usize,
    comparators: Vec<BoundOrderBy>,
    child: PlanRef,
}

impl LogicalTopN {
    pub fn new(
        offset: usize,
        limit: usize,
        comparators: Vec<BoundOrderBy>,
        child: PlanRef,
    ) -> Self {
        Self {
            offset,
            limit,
            comparators,
            child,
        }
    }

    /// Get a reference to the logical top N's offset.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Get a reference to the logical top N's limit.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Get a reference to the logical top N's comparators.
    pub fn comparators(&self) -> &[BoundOrderBy] {
        self.comparators.as_ref()
    }
}
impl PlanTreeNodeUnary for LogicalTopN {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(
            self.offset(),
            self.limit(),
            self.comparators().to_owned(),
            child,
        )
    }
}
impl_plan_tree_node_for_unary!(LogicalTopN);
impl PlanNode for LogicalTopN {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.child.schema()
    }

    fn estimated_cardinality(&self) -> usize {
        self.limit
    }

    fn prune_col(&self, required_cols: BitSet) -> PlanRef {
        let mut visitor = CollectRequiredCols(required_cols.clone());

        self.comparators
            .iter()
            .for_each(|node| visitor.visit_expr(&node.expr));

        let input_cols = visitor.0;

        let mapper = Mapper::new_with_bitset(&input_cols);
        let mut comparators = self.comparators.clone();

        comparators
            .iter_mut()
            .for_each(|node| mapper.rewrite_expr(&mut node.expr));

        let need_prune = input_cols != required_cols;

        let new_topn = LogicalTopN::new(
            self.offset,
            self.limit,
            comparators,
            self.child.prune_col(input_cols),
        )
        .into_plan_ref();

        if !need_prune {
            new_topn
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
            LogicalProjection::new(project_expressions, new_topn).into_plan_ref()
        }
    }
}

impl fmt::Display for LogicalTopN {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalTopN: offset: {}, limit: {}, order by {:?}",
            self.offset, self.limit, self.comparators
        )
    }
}
