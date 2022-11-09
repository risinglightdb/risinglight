// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::v1::binder::{BoundOrderBy, ExprVisitor};
use crate::v1::optimizer::logical_plan_rewriter::ExprRewriter;

/// The logical plan of order.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalOrder {
    comparators: Vec<BoundOrderBy>,
    child: PlanRef,
}

impl LogicalOrder {
    pub fn new(comparators: Vec<BoundOrderBy>, child: PlanRef) -> Self {
        Self { comparators, child }
    }

    /// Get a reference to the logical order's comparators.
    pub fn comparators(&self) -> &[BoundOrderBy] {
        self.comparators.as_ref()
    }
    pub fn clone_with_rewrite_expr(
        &self,
        new_child: PlanRef,
        rewriter: &impl ExprRewriter,
    ) -> Self {
        let mut new_cmps = self.comparators().to_vec();
        for cmp in &mut new_cmps {
            rewriter.rewrite_expr(&mut cmp.expr);
        }
        LogicalOrder::new(new_cmps, new_child)
    }
}
impl PlanTreeNodeUnary for LogicalOrder {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.comparators().to_vec(), child)
    }
}
impl_plan_tree_node_for_unary!(LogicalOrder);
impl PlanNode for LogicalOrder {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.child.schema()
    }

    fn estimated_cardinality(&self) -> usize {
        self.child().estimated_cardinality()
    }

    fn prune_col(&self, required_cols: BitSet) -> PlanRef {
        let mut visitor = CollectRequiredCols(required_cols);
        for node in &self.comparators {
            visitor.visit_expr(&node.expr);
        }
        let input_cols = visitor.0;

        let mapper = Mapper::new_with_bitset(&input_cols);
        let mut comparators = self.comparators.clone();
        for node in &mut comparators {
            mapper.rewrite_expr(&mut node.expr);
        }

        Self {
            comparators,
            child: self.child.prune_col(input_cols),
        }
        .into_plan_ref()
    }
}

impl fmt::Display for LogicalOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalOrder: {:?}", self.comparators)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataTypeKind;

    #[test]
    fn test_prune_order() {
        let ty = DataTypeKind::Int32.not_null();
        let col_descs = vec![
            ty.clone().to_column("v1".into()),
            ty.clone().to_column("v2".into()),
            ty.clone().to_column("v3".into()),
        ];
        let table_scan = LogicalTableScan::new(
            crate::catalog::TableRefId {
                database_id: 0,
                schema_id: 0,
                table_id: 0,
            },
            vec![1, 2, 3],
            col_descs.clone(),
            false,
            false,
            None,
        );

        let project_expressions = vec![
            BoundExpr::InputRef(BoundInputRef {
                index: 0,
                return_type: ty.clone(),
            }),
            BoundExpr::InputRef(BoundInputRef {
                index: 1,
                return_type: ty.clone(),
            }),
            BoundExpr::InputRef(BoundInputRef {
                index: 2,
                return_type: ty.clone(),
            }),
        ];

        let projection = LogicalProjection::new(project_expressions, table_scan.into_plan_ref());

        let node = vec![BoundOrderBy {
            expr: BoundExpr::InputRef(BoundInputRef {
                index: 1,
                return_type: ty.clone(),
            }),
            descending: false,
        }];

        let orderby = LogicalOrder::new(node, projection.into_plan_ref());

        let mut required_cols = BitSet::new();
        required_cols.insert(2);
        let plan = orderby.prune_col(required_cols);
        let orderby = plan.as_logical_order().unwrap();

        assert_eq!(
            orderby.comparators,
            vec![BoundOrderBy {
                expr: BoundExpr::InputRef(BoundInputRef {
                    index: 0,
                    return_type: ty,
                }),
                descending: false,
            }]
        );

        let plan = orderby.child();
        let projection = plan.as_logical_projection().unwrap();
        let plan = projection.child();
        let table_scan = plan.as_logical_table_scan().unwrap();
        assert_eq!(table_scan.column_descs(), &col_descs[1..3]);
        assert_eq!(table_scan.column_ids(), &[2, 3]);
    }
}
