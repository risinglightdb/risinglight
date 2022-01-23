// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::super::plan_nodes::*;
use super::*;
use crate::optimizer::BoundExpr::{BinaryOp, InputRef};
use crate::parser::BinaryOperator;
/// Convert all logical plan nodes to physical.
pub struct PhysicalConverter;

impl PlanRewriter for PhysicalConverter {
    fn rewrite_logical_table_scan(&mut self, logical: &LogicalTableScan) -> PlanRef {
        Arc::new(PhysicalTableScan::new(logical.clone()))
    }
    fn rewrite_logical_projection(&mut self, logical: &LogicalProjection) -> PlanRef {
        let child = self.rewrite(logical.child());
        let logical = logical.clone_with_child(child);
        Arc::new(PhysicalProjection::new(logical))
    }

    fn rewrite_logical_order(&mut self, logical: &LogicalOrder) -> PlanRef {
        let child = self.rewrite(logical.child());
        let logical = logical.clone_with_child(child);
        Arc::new(PhysicalOrder::new(logical))
    }

    fn rewrite_logical_limit(&mut self, logical: &LogicalLimit) -> PlanRef {
        let child = self.rewrite(logical.child());
        let logical = logical.clone_with_child(child);
        Arc::new(PhysicalLimit::new(logical))
    }

    fn rewrite_logical_join(&mut self, logical_join: &LogicalJoin) -> PlanRef {
        // Hash join is only used for equal join.

        /// Find the column indexes (i, j) where there is a condition `left[i] = right[j]` in expr.
        fn find_hash_join_index(expr: &BoundExpr, mid: usize) -> Option<(usize, usize)> {
            match expr {
                BinaryOp(op) => match (&op.op, &*op.left_expr, &*op.right_expr) {
                    (BinaryOperator::Eq, InputRef(x), InputRef(y)) => {
                        let i1 = x.index.min(y.index);
                        let i2 = x.index.max(y.index);
                        if i1 < mid && i2 >= mid {
                            return Some((i1, i2 - mid));
                        }
                        None
                    }
                    (BinaryOperator::And, left, right) => {
                        if let ret @ Some(_) = find_hash_join_index(left, mid) {
                            return ret;
                        }
                        find_hash_join_index(right, mid)
                    }
                    _ => None,
                },
                _ => None,
            }
        }
        let mid = logical_join.left().out_types().len();
        let hash_join_index = find_hash_join_index(logical_join.condition(), mid);

        let left = self.rewrite(logical_join.left());
        let right = self.rewrite(logical_join.right());
        let logical_join = logical_join.clone_with_left_right(left, right);

        if let Some((left_column_index, right_column_index)) = hash_join_index {
            return Arc::new(PhysicalHashJoin::new(
                logical_join,
                left_column_index,
                right_column_index,
            ));
        }
        Arc::new(PhysicalNestedLoopJoin::new(logical_join))
    }

    fn rewrite_logical_insert(&mut self, logical: &LogicalInsert) -> PlanRef {
        let child = self.rewrite(logical.child());
        let logical = logical.clone_with_child(child);
        Arc::new(PhysicalInsert::new(logical))
    }

    fn rewrite_logical_values(&mut self, logical: &LogicalValues) -> PlanRef {
        Arc::new(PhysicalValues::new(logical.clone()))
    }

    fn rewrite_logical_filter(&mut self, logical: &LogicalFilter) -> PlanRef {
        let child = self.rewrite(logical.child());
        let logical = logical.clone_with_child(child);
        Arc::new(PhysicalFilter::new(logical))
    }

    fn rewrite_logical_explain(&mut self, logical: &LogicalExplain) -> PlanRef {
        let child = self.rewrite(logical.child());
        let logical = logical.clone_with_child(child);
        Arc::new(PhysicalExplain::new(logical))
    }

    fn rewrite_logical_drop(&mut self, logical: &LogicalDrop) -> PlanRef {
        Arc::new(PhysicalDrop::new(logical.clone()))
    }

    fn rewrite_logical_delete(&mut self, logical: &LogicalDelete) -> PlanRef {
        let child = self.rewrite(logical.child());
        let logical = logical.clone_with_child(child);
        Arc::new(PhysicalDelete::new(logical))
    }

    fn rewrite_logical_create_table(&mut self, logical: &LogicalCreateTable) -> PlanRef {
        Arc::new(PhysicalCreateTable::new(logical.clone()))
    }

    fn rewrite_logical_copy_from_file(&mut self, logical: &LogicalCopyFromFile) -> PlanRef {
        Arc::new(PhysicalCopyFromFile::new(logical.clone()))
    }

    fn rewrite_logical_copy_to_file(&mut self, logical: &LogicalCopyToFile) -> PlanRef {
        let child = self.rewrite(logical.child());
        let logical = logical.clone_with_child(child);
        Arc::new(PhysicalCopyToFile::new(logical))
    }

    fn rewrite_logical_aggregate(&mut self, logical: &LogicalAggregate) -> PlanRef {
        if logical.group_keys().is_empty() {
            Arc::new(PhysicalSimpleAgg::new(
                logical.agg_calls().to_vec(),
                self.rewrite(logical.child()),
            ))
        } else {
            let child = self.rewrite(logical.child());
            let logical = logical.clone_with_child(child);
            Arc::new(PhysicalHashAgg::new(logical))
        }
    }
}
