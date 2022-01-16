use super::*;
use crate::binder::BoundJoinOperator;
use crate::optimizer::BoundExpr::{BinaryOp, InputRef};
use crate::parser::BinaryOperator;
/// Convert all logical plan nodes to physical.
pub struct PhysicalConverter;

impl PlanRewriter for PhysicalConverter {
    fn rewrite_logical_table_scan(&mut self, logical: &LogicalTableScan) -> PlanRef {
        Rc::new(PhysicalTableScan::new(logical))
    }
    fn rewrite_logical_projection(&mut self, logical: &LogicalProjection) -> PlanRef {
        Rc::new(PhysicalProjection::new(logical))
    }

    fn rewrite_logical_order(&mut self, logical: &LogicalOrder) -> PlanRef {
        Rc::new(PhysicalOrder::new(logical))
    }

    fn rewrite_logical_limit(&mut self, logical: &LogicalLimit) -> PlanRef {
        Rc::new(PhysicalLimit::new(logical))
    }

    fn rewrite_logical_join(&mut self, logical_join: LogicalJoin) -> PlanRef {
        // Hash join is only used for equal join.
        // So far, we only support hash join when doing inner join.
        let left_column_size = logical_join.left_plan.out_types().len();
        let mut left_column_index = 0;
        let mut right_column_index = 0;
        let mut use_hash_join = false;

        if logical_join.join_op == BoundJoinOperator::Inner {
            if let BinaryOp(op) = &logical_join.condition {
                if let (BinaryOperator::Eq, InputRef(refx), InputRef(refy)) =
                    (&op.op, &*op.left_expr, &*op.right_expr)
                {
                    if refx.index < left_column_size && refy.index >= left_column_size {
                        left_column_index = refx.index;
                        right_column_index = refy.index - left_column_size;
                        use_hash_join = true;
                    } else if refy.index < left_column_size && refx.index >= left_column_size {
                        left_column_index = refy.index;
                        right_column_index = refx.index - left_column_size;
                        use_hash_join = true;
                    }
                }
            }
        }

        if use_hash_join {
            return Rc::new(PhysicalHashJoin::new(
                logical_join,
                left_column_index,
                right_column_index,
            ));
        }
        Rc::new(PhysicalNestedLoopJoin::new(logical_join))
    }

    fn rewrite_logical_insert(&mut self, logical: &LogicalInsert) -> PlanRef {
        Rc::new(PhysicalInsert::new(logical))
    }

    fn rewrite_logical_values(&mut self, logical: &LogicalValues) -> PlanRef {
        Rc::new(PhysicalValues::new(logical))
    }

    fn rewrite_logical_filter(&mut self, logical: &LogicalFilter) -> PlanRef {
        Rc::new(PhysicalFilter::new(logical))
    }

    fn rewrite_logical_explain(&mut self, logical: &LogicalExplain) -> PlanRef {
        Rc::new(PhysicalExplain::new(logical))
    }

    fn rewrite_logical_drop(&mut self, logical: &LogicalDrop) -> PlanRef {
        Rc::new(PhysicalDrop::new(logical))
    }

    fn rewrite_logical_delete(&mut self, logical: &LogicalDelete) -> PlanRef {
        Rc::new(PhysicalDelete::new(logical))
    }

    fn rewrite_logical_create_table(&mut self, logical: &LogicalCreateTable) -> PlanRef {
        Rc::new(PhysicalCreateTable::new(logical))
    }

    fn rewrite_logical_copy_from_file(&mut self, logical: &LogicalCopyFromFile) -> PlanRef {
        Rc::new(PhysicalCopyFromFile::new(logical))
    }

    fn rewrite_logical_copy_to_file(&mut self, logical: &LogicalCopyToFile) -> PlanRef {
        Rc::new(PhysicalCopyToFile::new(logical))
    }

    fn rewrite_logical_aggregate(&mut self, logical: &LogicalAggregate) -> PlanRef {
        if logical.group_keys.is_empty() {
            Rc::new(PhysicalSimpleAgg::new(logical.agg_calls, logical.child))
        } else {
            Rc::new(PhysicalHashAgg::new(logical))
        }
    }
}
