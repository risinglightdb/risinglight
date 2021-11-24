//! Logical planner of `select` statement.
//!
//! A `select` statement will be planned to a compose of:
//!
//! - [`LogicalSeqScan`] (from *) or dummy plan (no from)
//! - [`LogicalFilter`] (where *)
//! - [`LogicalProjection`] (select *)
//! - [`LogicalOrder`] (order by *)
use super::*;
use crate::binder::{
    BoundAggCall, BoundExpr, BoundExprKind, BoundInputRef, BoundSelect, BoundTableRef,
};

impl LogicalPlaner {
    pub fn plan_select(&self, mut stmt: Box<BoundSelect>) -> Result<LogicalPlan, LogicalPlanError> {
        let mut plan = LogicalPlan::Dummy;
        let mut is_sorted = false;

        if let Some(table_ref) = stmt.from_table.get(0) {
            // use `sorted` mode from the storage engine if the order by column is the primary key
            if stmt.orderby.len() == 1 && !stmt.orderby[0].descending {
                if let BoundExprKind::ColumnRef(col_ref) = &stmt.orderby[0].expr.kind {
                    if col_ref.is_primary_key {
                        is_sorted = true;
                    }
                }
            }
            plan = self.plan_table_ref(table_ref, false, is_sorted)?;
        }

        if let Some(expr) = stmt.where_clause {
            plan = LogicalPlan::Filter(LogicalFilter {
                expr,
                child: Box::new(plan),
            });
        }

        let mut agg_extractor = AggExtractor::new(stmt.group_by.len());
        for expr in &mut stmt.select_list {
            agg_extractor.visit_expr(expr);
        }
        if !agg_extractor.agg_calls.is_empty() {
            plan = LogicalPlan::Aggregate(LogicalAggregate {
                agg_calls: agg_extractor.agg_calls,
                group_keys: stmt.group_by,
                child: Box::new(plan),
            })
        }

        // TODO: support the following clauses
        assert!(!stmt.select_distinct, "TODO: plan distinct");

        if !stmt.select_list.is_empty() {
            plan = LogicalPlan::Projection(LogicalProjection {
                project_expressions: stmt.select_list,
                child: Box::new(plan),
            });
        }
        if !stmt.orderby.is_empty() && !is_sorted {
            plan = LogicalPlan::Order(LogicalOrder {
                comparators: stmt.orderby,
                child: Box::new(plan),
            });
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            let limit = match stmt.limit {
                Some(limit) => match limit.kind {
                    BoundExprKind::Constant(v) => v.as_usize()?.unwrap_or(usize::MAX / 2),
                    _ => panic!("limit only support constant expression"),
                },
                None => usize::MAX / 2, // avoid 'offset + limit' overflow
            };
            let offset = match stmt.offset {
                Some(offset) => match offset.kind {
                    BoundExprKind::Constant(v) => v.as_usize()?.unwrap_or(0),
                    _ => panic!("offset only support constant expression"),
                },
                None => 0,
            };
            plan = LogicalPlan::Limit(LogicalLimit {
                offset,
                limit,
                child: Box::new(plan),
            });
        }

        if plan == LogicalPlan::Dummy {
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
            } => Ok(LogicalPlan::SeqScan(LogicalSeqScan {
                table_ref_id: *ref_id,
                column_ids: column_ids.to_vec(),
                with_row_handler,
                is_sorted,
            })),
            BoundTableRef::JoinTableRef {
                relation,
                join_tables,
            } => {
                let relation_plan = self.plan_table_ref(relation, with_row_handler, is_sorted)?;
                let mut join_table_plans = vec![];
                for table in join_tables.iter() {
                    let table_plan =
                        self.plan_table_ref(&table.table_ref, with_row_handler, is_sorted)?;
                    join_table_plans.push(LogicalJoinTable {
                        table_plan: Box::new(table_plan),
                        join_op: table.join_op.clone(),
                    });
                }
                if join_table_plans.is_empty() {
                    return Ok(relation_plan);
                }
                Ok(LogicalPlan::Join(LogicalJoin {
                    relation_plan: Box::new(relation_plan),
                    join_table_plans,
                }))
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
        use BoundExprKind::*;
        match &mut expr.kind {
            kind @ AggCall(_) => {
                let agg_expr =
                    std::mem::replace(kind, InputRef(BoundInputRef { index: self.index }));
                match agg_expr {
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
