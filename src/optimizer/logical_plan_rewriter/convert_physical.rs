use super::*;
use crate::binder::BoundJoinOperator;
use crate::optimizer::BoundExpr::{BinaryOp, InputRef};
use crate::parser::BinaryOperator;
/// Convert all logical plan nodes to physical.
pub struct PhysicalConverter;

impl Rewriter for PhysicalConverter {
    fn rewrite_logical_seq_scan(&mut self, plan: LogicalSeqScan) -> PlanRef {
        Rc::new(PhysicalSeqScan {
            table_ref_id: plan.table_ref_id,
            column_ids: plan.column_ids,
            with_row_handler: plan.with_row_handler,
            is_sorted: plan.is_sorted,
            column_descs: plan.column_descs,
        })
    }
    fn rewrite_logical_projection(&mut self, plan: LogicalProjection) -> PlanRef {
        Rc::new(PhysicalProjection {
            project_expressions: plan.project_expressions,
            child: plan.child,
        })
    }

    fn rewrite_logical_order(&mut self, plan: LogicalOrder) -> PlanRef {
        Rc::new(PhysicalOrder {
            comparators: plan.comparators,
            child: plan.child,
        })
    }

    fn rewrite_logical_limit(&mut self, plan: LogicalLimit) -> PlanRef {
        Rc::new(PhysicalLimit {
            offset: plan.offset,
            limit: plan.limit,
            child: plan.child,
        })
    }

    fn rewrite_logical_join_is_nested(&mut self) -> bool {
        true
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
                logical_join.left_plan.rewrite(self),
                logical_join.right_plan.rewrite(self),
                logical_join.join_op,
                logical_join.condition,
                left_column_index,
                right_column_index,
            ));
        }
        Rc::new(PhysicalNestedLoopJoin::new(
            logical_join.left_plan.rewrite(self),
            logical_join.right_plan.rewrite(self),
            logical_join.join_op,
            logical_join.condition,
        ))
    }

    fn rewrite_logical_insert(&mut self, plan: LogicalInsert) -> PlanRef {
        Rc::new(PhysicalInsert {
            table_ref_id: plan.table_ref_id,
            column_ids: plan.column_ids,
            child: plan.child,
        })
    }

    fn rewrite_logical_values(&mut self, plan: LogicalValues) -> PlanRef {
        Rc::new(PhysicalValues {
            column_types: plan.column_types,
            values: plan.values,
        })
    }

    fn rewrite_logical_filter(&mut self, plan: LogicalFilter) -> PlanRef {
        Rc::new(PhysicalFilter {
            expr: plan.expr,
            child: plan.child,
        })
    }

    fn rewrite_logical_explain(&mut self, plan: LogicalExplain) -> PlanRef {
        Rc::new(PhysicalExplain { plan: plan.plan })
    }

    fn rewrite_logical_drop(&mut self, plan: LogicalDrop) -> PlanRef {
        Rc::new(PhysicalDrop {
            object: plan.object,
        })
    }

    fn rewrite_logical_delete(&mut self, plan: LogicalDelete) -> PlanRef {
        Rc::new(PhysicalDelete {
            table_ref_id: plan.table_ref_id,
            child: plan.child,
        })
    }

    fn rewrite_logical_create_table(&mut self, plan: LogicalCreateTable) -> PlanRef {
        Rc::new(PhysicalCreateTable {
            database_id: plan.database_id,
            schema_id: plan.schema_id,
            table_name: plan.table_name,
            columns: plan.columns,
        })
    }

    fn rewrite_logical_copy_from_file(&mut self, plan: LogicalCopyFromFile) -> PlanRef {
        Rc::new(PhysicalCopyFromFile {
            path: plan.path,
            format: plan.format,
            column_types: plan.column_types,
        })
    }

    fn rewrite_logical_copy_to_file(&mut self, plan: LogicalCopyToFile) -> PlanRef {
        Rc::new(PhysicalCopyToFile {
            path: plan.path,
            format: plan.format,
            column_types: plan.column_types,
            child: plan.child,
        })
    }

    fn rewrite_logical_aggregate(&mut self, plan: LogicalAggregate) -> PlanRef {
        if plan.group_keys.is_empty() {
            Rc::new(PhysicalSimpleAgg::new(plan.agg_calls, plan.child))
        } else {
            Rc::new(PhysicalHashAgg::new(
                plan.agg_calls,
                plan.group_keys,
                plan.child,
            ))
        }
    }
}
