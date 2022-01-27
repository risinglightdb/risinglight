// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::super::plan_nodes::*;
use super::*;
use crate::binder::BoundJoinOperator;
use crate::types::DataValue;
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
        let left = self.rewrite(logical_join.left());
        let right = self.rewrite(logical_join.right());
        let predicate = logical_join.predicate();
        // FIXME: Currently just Inner join use HashJoin
        if !predicate.eq_keys().is_empty() && logical_join.join_op() == BoundJoinOperator::Inner {
            // TODO: Currently hash join just use one column pair as hash index
            // TODO: Currently HashJoinExecutor ignores the condition, so for correctness we pull
            // the conditions as a filter operator. And this transformation is only correct for
            // inner join
            let left_col_num = left.out_types().len();
            let (left_column_index, right_column_index) = predicate.eq_keys()[0].clone();
            let join = Arc::new(PhysicalHashJoin::new(
                LogicalJoin::create(
                    left,
                    right,
                    BoundJoinOperator::Inner,
                    BoundExpr::Constant(DataValue::Bool(true)),
                ),
                left_column_index.index,
                right_column_index.index - left_col_num,
            ));
            // Currently hash join just use one column pair as hash index
            let need_pull_filter = predicate.eq_keys().len() != 1
                || !predicate.left_conds().is_empty()
                || !predicate.right_conds().is_empty()
                || !predicate.other_conds().is_empty();
            if need_pull_filter {
                return Arc::new(PhysicalFilter::new(LogicalFilter::new(
                    predicate.to_on_clause(),
                    join,
                )));
            } else {
                return join;
            };
        }
        Arc::new(PhysicalNestedLoopJoin::new(
            logical_join.clone_with_left_right(left, right),
        ))
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

    fn rewrite_logical_exchange(&mut self, logical: &LogicalExchange) -> PlanRef {
        Arc::new(PhysicalExchange::new(self.rewrite(logical.child())))
    }
}
