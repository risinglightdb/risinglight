//! Logical planner of `select` statement.
//!
//! A `select` statement will be planned to a compose of:
//!
//! - [`LogicalSeqScan`] (from *) or dummy plan (no from)
//! - [`LogicalFilter`] (where *)
//! - [`LogicalProjection`] (select *)
//! - [`LogicalOrder`] (order by *)
use super::*;
use crate::binder::{BoundAggCall, BoundExpr, BoundInputRef, BoundSelect, BoundTableRef};
use crate::logical_optimizer::plan_nodes::logical_aggregate::LogicalAggregate;
use crate::logical_optimizer::plan_nodes::logical_filter::LogicalFilter;
use crate::logical_optimizer::plan_nodes::logical_join::LogicalJoin;
use crate::logical_optimizer::plan_nodes::logical_limit::LogicalLimit;
use crate::logical_optimizer::plan_nodes::logical_order::LogicalOrder;
use crate::logical_optimizer::plan_nodes::logical_projection::LogicalProjection;
use crate::logical_optimizer::plan_nodes::logical_seq_scan::LogicalSeqScan;
use crate::logical_optimizer::plan_nodes::{Dummy, LogicalPlan};

impl LogicalPlaner {
    pub fn plan_select(&self, mut stmt: Box<BoundSelect>) -> Result<LogicalPlan, LogicalPlanError> {
        let mut plan = LogicalPlan::Dummy(Dummy {});
        let mut is_sorted = false;

        if let Some(table_ref) = &stmt.from_table {
            // use `sorted` mode from the storage engine if the order by column is the primary key
            if stmt.orderby.len() == 1 && !stmt.orderby[0].descending {
                if let BoundExpr::ColumnRef(col_ref) = &stmt.orderby[0].expr {
                    if col_ref.is_primary_key {
                        is_sorted = true;
                    }
                }
            }
            plan = self.plan_table_ref(table_ref, false, is_sorted)?;
        }

        if let Some(expr) = stmt.where_clause {
            plan = LogicalPlan::LogicalFilter(LogicalFilter {
                expr,
                child: plan.into(),
            });
        }

        let mut agg_extractor = AggExtractor::new(stmt.group_by.len());
        for expr in &mut stmt.select_list {
            agg_extractor.visit_expr(expr);
        }
        if !agg_extractor.agg_calls.is_empty() {
            plan = LogicalPlan::LogicalAggregate(LogicalAggregate {
                agg_calls: agg_extractor.agg_calls,
                group_keys: stmt.group_by,
                child: plan.into(),
            })
        }

        // TODO: support the following clauses
        assert!(!stmt.select_distinct, "TODO: plan distinct");

        if !stmt.select_list.is_empty() {
            plan = LogicalPlan::LogicalProjection(LogicalProjection {
                project_expressions: stmt.select_list,
                child: plan.into(),
            });
        }
        if !stmt.orderby.is_empty() && !is_sorted {
            plan = LogicalPlan::LogicalOrder(LogicalOrder {
                comparators: stmt.orderby,
                child: plan.into(),
            });
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            let limit = match stmt.limit {
                Some(limit) => match limit {
                    BoundExpr::Constant(v) => v.as_usize()?.unwrap_or(usize::MAX / 2),
                    _ => panic!("limit only support constant expression"),
                },
                None => usize::MAX / 2, // avoid 'offset + limit' overflow
            };
            let offset = match stmt.offset {
                Some(offset) => match offset {
                    BoundExpr::Constant(v) => v.as_usize()?.unwrap_or(0),
                    _ => panic!("offset only support constant expression"),
                },
                None => 0,
            };
            plan = LogicalPlan::LogicalLimit(LogicalLimit {
                offset,
                limit,
                child: plan.into(),
            });
        }

        if let LogicalPlan::Dummy(_) = plan {
            return Err(LogicalPlanError::InvalidSQL);
        }
        Ok(plan)
    }

    pub fn plan_table_ref(
        &self,
        table_ref: &BoundTableRef,
        with_row_handler: bool,
        is_sorted: bool,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        match table_ref {
            BoundTableRef::BaseTableRef {
                ref_id,
                table_name: _,
                column_ids,
            } => Ok(LogicalPlan::LogicalSeqScan(LogicalSeqScan {
                table_ref_id: *ref_id,
                column_ids: column_ids.to_vec(),
                with_row_handler,
                is_sorted,
            })),
            BoundTableRef::JoinTableRef {
                relation,
                join_tables,
            } => {
                let mut plan = self.plan_table_ref(relation, with_row_handler, is_sorted)?;
                for join_table in join_tables.iter() {
                    let table_plan =
                        self.plan_table_ref(&join_table.table_ref, with_row_handler, is_sorted)?;
                    plan = LogicalPlan::LogicalJoin(LogicalJoin {
                        left_plan: plan.into(),
                        right_plan: table_plan.into(),
                        join_op: join_table.join_op.clone(),
                    });
                }
                Ok(plan)
            }
        }
    }
}

/// An expression visitor that extracts aggregation nodes and replaces them with `InputRef`.
///
/// For example:
/// In SQL: `select sum(b) + a * count(a) from t group by a;`
/// The expression `sum(b) + a * count(a)` will be rewritten to `InputRef(1) + a * InputRef(2)`,
/// because the underlying aggregate plan will output `(a, sum(b), count(a))`. The group keys appear
/// before aggregations.
#[derive(Default)]
struct AggExtractor {
    agg_calls: Vec<BoundAggCall>,
    index: usize,
}

impl AggExtractor {
    fn new(group_key_count: usize) -> Self {
        AggExtractor {
            agg_calls: vec![],
            index: group_key_count,
        }
    }

    fn visit_expr(&mut self, expr: &mut BoundExpr) {
        use BoundExpr::*;
        match expr {
            AggCall(agg) => {
                let input_ref = InputRef(BoundInputRef {
                    index: self.index,
                    return_type: agg.return_type.clone(),
                });
                match std::mem::replace(expr, input_ref) {
                    AggCall(agg) => self.agg_calls.push(agg),
                    _ => unreachable!(),
                }
                self.index += 1;
            }
            BinaryOp(bin_op) => {
                self.visit_expr(&mut bin_op.left_expr);
                self.visit_expr(&mut bin_op.right_expr);
            }
            UnaryOp(unary_op) => self.visit_expr(&mut unary_op.expr),
            TypeCast(type_cast) => self.visit_expr(&mut type_cast.expr),
            Constant(_) | ColumnRef(_) | InputRef(_) | IsNull(_) => {}
        }
    }
}
