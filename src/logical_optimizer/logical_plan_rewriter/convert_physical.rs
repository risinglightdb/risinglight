use super::LogicalPlanRewriter;
use crate::logical_optimizer::plan_nodes::*;

pub struct PhysicalConverter;

impl LogicalPlanRewriter for PhysicalConverter {
    fn rewrite_seqscan(&mut self, plan: &LogicalSeqScan) -> Option<PlanRef> {
        Some(
            Plan::PhysicalSeqScan(PhysicalSeqScan {
                table_ref_id: plan.table_ref_id,
                column_ids: plan.column_ids.clone(),
                with_row_handler: plan.with_row_handler,
                is_sorted: plan.is_sorted,
            })
            .into(),
        )
    }
    fn rewrite_projection(&mut self, plan: &LogicalProjection) -> Option<PlanRef> {
        Some(
            Plan::PhysicalProjection(PhysicalProjection {
                project_expressions: plan.project_expressions.clone(),
                child: self.rewrite_plan(plan.child.clone()),
            })
            .into(),
        )
    }

    fn rewrite_order(&mut self, plan: &LogicalOrder) -> Option<PlanRef> {
        Some(
            Plan::PhysicalOrder(PhysicalOrder {
                comparators: plan.comparators.clone(),
                child: self.rewrite_plan(plan.child.clone()),
            })
            .into(),
        )
    }

    fn rewrite_limit(&mut self, plan: &LogicalLimit) -> Option<PlanRef> {
        Some(
            Plan::PhysicalLimit(PhysicalLimit {
                offset: plan.offset,
                limit: plan.limit,
                child: self.rewrite_plan(plan.child.clone()),
            })
            .into(),
        )
    }

    fn rewrite_join(&mut self, logical_join: &LogicalJoin) -> Option<PlanRef> {
        Some(
            Plan::PhysicalJoin(PhysicalJoin {
                join_type: PhysicalJoinType::NestedLoop,
                left_plan: self.rewrite_plan(logical_join.left_plan.clone()),
                right_plan: self.rewrite_plan(logical_join.right_plan.clone()),
                join_op: logical_join.join_op.clone(),
            })
            .into(),
        )
    }

    fn rewrite_insert(&mut self, plan: &LogicalInsert) -> Option<PlanRef> {
        Some(
            Plan::PhysicalInsert(PhysicalInsert {
                table_ref_id: plan.table_ref_id,
                column_ids: plan.column_ids.clone(),
                child: self.rewrite_plan(plan.child.clone()),
            })
            .into(),
        )
    }

    fn rewrite_values(&mut self, plan: &LogicalValues) -> Option<PlanRef> {
        Some(
            Plan::PhysicalValues(PhysicalValues {
                column_types: plan.column_types.clone(),
                values: plan.values.clone(),
            })
            .into(),
        )
    }

    fn rewrite_filter(&mut self, plan: &LogicalFilter) -> Option<PlanRef> {
        Some(
            Plan::PhysicalFilter(PhysicalFilter {
                expr: plan.expr.clone(),
                child: self.rewrite_plan(plan.child.clone()),
            })
            .into(),
        )
    }

    fn rewrite_explain(&mut self, plan: &LogicalExplain) -> Option<PlanRef> {
        Some(
            Plan::PhysicalExplain(PhysicalExplain {
                plan: self.rewrite_plan(plan.child().clone()),
            })
            .into(),
        )
    }

    fn rewrite_drop(&mut self, plan: &LogicalDrop) -> Option<PlanRef> {
        Some(
            Plan::PhysicalDrop(PhysicalDrop {
                object: plan.object.clone(),
            })
            .into(),
        )
    }

    fn rewrite_delete(&mut self, plan: &LogicalDelete) -> Option<PlanRef> {
        Some(
            Plan::PhysicalDelete(PhysicalDelete {
                table_ref_id: plan.table_ref_id,
                child: self.rewrite_plan(plan.child.clone()),
            })
            .into(),
        )
    }

    fn rewrite_create_table(&mut self, plan: &LogicalCreateTable) -> Option<PlanRef> {
        Some(
            Plan::PhysicalCreateTable(PhysicalCreateTable {
                database_id: plan.database_id,
                schema_id: plan.schema_id,
                table_name: plan.table_name.clone(),
                columns: plan.columns.clone(),
            })
            .into(),
        )
    }

    fn rewrite_copy_from_file(&mut self, plan: &LogicalCopyFromFile) -> Option<PlanRef> {
        Some(
            Plan::PhysicalCopyFromFile(PhysicalCopyFromFile {
                path: plan.path.clone(),
                format: plan.format.clone(),
                column_types: plan.column_types.clone(),
            })
            .into(),
        )
    }

    fn rewrite_copy_to_file(&mut self, plan: &LogicalCopyToFile) -> Option<PlanRef> {
        Some(
            Plan::PhysicalCopyToFile(PhysicalCopyToFile {
                path: plan.path.clone(),
                format: plan.format.clone(),
                column_types: plan.column_types.clone(),
                child: self.rewrite_plan(plan.child.clone()),
            })
            .into(),
        )
    }

    fn rewrite_aggregate(&mut self, plan: &LogicalAggregate) -> Option<PlanRef> {
        if plan.group_keys.is_empty() {
            Some(
                Plan::PhysicalSimpleAgg(PhysicalSimpleAgg {
                    agg_calls: plan.agg_calls.clone(),
                    child: self.rewrite_plan(plan.child.clone()),
                })
                .into(),
            )
        } else {
            Some(
                Plan::PhysicalHashAgg(PhysicalHashAgg {
                    agg_calls: plan.agg_calls.clone(),
                    group_keys: plan.group_keys.clone(),
                    child: self.rewrite_plan(plan.child.clone()),
                })
                .into(),
            )
        }
    }
}
