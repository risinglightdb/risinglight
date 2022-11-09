// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::v1::binder::{BoundAggCall, BoundExpr, ExprVisitor};
use crate::v1::optimizer::logical_plan_rewriter::ExprRewriter;

/// The logical plan of hash aggregate operation.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalAggregate {
    agg_calls: Vec<BoundAggCall>,
    /// Group keys in hash aggregation (optional)
    group_keys: Vec<BoundExpr>,
    child: PlanRef,
}

impl LogicalAggregate {
    pub fn new(agg_calls: Vec<BoundAggCall>, group_keys: Vec<BoundExpr>, child: PlanRef) -> Self {
        LogicalAggregate {
            agg_calls,
            group_keys,
            child,
        }
    }

    /// Get a reference to the logical aggregate's agg calls.
    pub fn agg_calls(&self) -> &[BoundAggCall] {
        self.agg_calls.as_ref()
    }

    /// Get a reference to the logical aggregate's group keys.
    pub fn group_keys(&self) -> &[BoundExpr] {
        self.group_keys.as_ref()
    }

    pub fn clone_with_rewrite_expr(
        &self,
        new_child: PlanRef,
        rewriter: &impl ExprRewriter,
    ) -> Self {
        let mut new_agg_calls = self.agg_calls().to_vec();
        let mut new_keys = self.group_keys().to_vec();
        for agg in &mut new_agg_calls {
            for arg in &mut agg.args {
                rewriter.rewrite_expr(arg);
            }
        }
        for keys in &mut new_keys {
            rewriter.rewrite_expr(keys);
        }

        LogicalAggregate::new(new_agg_calls, new_keys, new_child)
    }
}

impl PlanTreeNodeUnary for LogicalAggregate {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.agg_calls().to_vec(), self.group_keys().to_vec(), child)
    }
}
impl_plan_tree_node_for_unary!(LogicalAggregate);
impl PlanNode for LogicalAggregate {
    fn schema(&self) -> Vec<ColumnDesc> {
        let child_schema = self.child.schema();
        self.group_keys
            .iter()
            .map(|expr| ColumnDesc::new(expr.return_type(), expr.format_name(&child_schema), false))
            .chain(self.agg_calls.iter().map(|agg_call| {
                agg_call
                    .return_type
                    .clone()
                    .to_column(format!("{}", agg_call.kind))
            }))
            .collect()
    }

    fn estimated_cardinality(&self) -> usize {
        self.child().estimated_cardinality()
    }

    fn prune_col(&self, required_cols: BitSet) -> PlanRef {
        let group_keys_len = self.group_keys.len();

        // Collect ref_idx of AggCall args
        let mut visitor =
            CollectRequiredCols(BitSet::with_capacity(group_keys_len + self.agg_calls.len()));
        let mut new_agg_calls: Vec<_> = required_cols
            .iter()
            .filter(|&index| index >= group_keys_len)
            .map(|index| {
                let call = &self.agg_calls[index - group_keys_len];
                call.args.iter().for_each(|expr| {
                    visitor.visit_expr(expr);
                });
                self.agg_calls[index - group_keys_len].clone()
            })
            .collect();

        // Collect ref_idx of GroupExpr
        self.group_keys
            .iter()
            .for_each(|group| visitor.visit_expr(group));

        let input_cols = visitor.0;

        let mapper = Mapper::new_with_bitset(&input_cols);
        for call in &mut new_agg_calls {
            call.args.iter_mut().for_each(|expr| {
                mapper.rewrite_expr(expr);
            })
        }

        let mut group_keys = self.group_keys.clone();
        group_keys
            .iter_mut()
            .for_each(|expr| mapper.rewrite_expr(expr));

        let new_agg = LogicalAggregate::new(
            new_agg_calls.clone(),
            group_keys,
            self.child.prune_col(input_cols),
        );

        let bitset = BitSet::from_iter(0..group_keys_len);

        if bitset.is_subset(&required_cols) {
            new_agg.into_plan_ref()
        } else {
            // Need prune
            let mut new_projection: Vec<BoundExpr> = required_cols
                .iter()
                .filter(|&i| i < group_keys_len)
                .map(|index| {
                    BoundExpr::InputRef(BoundInputRef {
                        index,
                        return_type: self.group_keys[index].return_type(),
                    })
                })
                .collect();

            for (index, item) in new_agg_calls.iter().enumerate() {
                new_projection.push(BoundExpr::InputRef(BoundInputRef {
                    index: group_keys_len + index,
                    return_type: item.return_type.clone(),
                }))
            }
            LogicalProjection::new(new_projection, new_agg.into_plan_ref()).into_plan_ref()
        }
    }
}
impl fmt::Display for LogicalAggregate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalAggregate: {} agg calls", self.agg_calls.len(),)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataTypeKind;
    use crate::v1::binder::AggKind;

    #[test]
    fn test_aggregate_out_names() {
        let plan = LogicalAggregate::new(
            vec![
                BoundAggCall {
                    kind: AggKind::Sum,
                    args: vec![],
                    return_type: DataTypeKind::Float64.not_null(),
                },
                BoundAggCall {
                    kind: AggKind::Avg,
                    args: vec![],
                    return_type: DataTypeKind::Float64.not_null(),
                },
                BoundAggCall {
                    kind: AggKind::Count,
                    args: vec![],
                    return_type: DataTypeKind::Float64.not_null(),
                },
                BoundAggCall {
                    kind: AggKind::RowCount,
                    args: vec![],
                    return_type: DataTypeKind::Float64.not_null(),
                },
            ],
            vec![],
            Arc::new(Dummy::new(Vec::new())),
        );

        let column_names = plan.out_names();
        assert_eq!(column_names[0], "sum");
        assert_eq!(column_names[1], "avg");
        assert_eq!(column_names[2], "count");
        assert_eq!(column_names[3], "count");
    }

    #[test]
    /// Pruning
    /// ```text
    /// Agg(gk: input_ref(2), call: sum(input_ref(0)), avg(input_ref(1)))
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [2] will result in
    /// ```text
    /// Projection(input_ref(1))
    ///   Agg(gk: input_ref(1), call: avg(input_ref(0)))
    ///     TableScan(v1, v3)
    /// ```
    fn test_prune_aggregate() {
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
            col_descs,
            false,
            false,
            None,
        );

        let input_refs = vec![
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
                return_type: ty,
            }),
        ];

        let aggregate = LogicalAggregate::new(
            vec![
                BoundAggCall {
                    kind: AggKind::Sum,
                    args: vec![input_refs[0].clone()],
                    return_type: DataTypeKind::Int32.not_null(),
                },
                BoundAggCall {
                    kind: AggKind::Avg,
                    args: vec![input_refs[1].clone()],
                    return_type: DataTypeKind::Int32.not_null(),
                },
            ],
            vec![input_refs[2].clone()],
            Arc::new(table_scan),
        );

        let mut required_cols = BitSet::new();
        required_cols.insert(2);
        let plan = aggregate.prune_col(required_cols);
        let plan = plan.as_logical_projection().unwrap();

        assert_eq!(plan.project_expressions(), vec![input_refs[1].clone()]);
        let plan = plan.child();
        let plan = plan.as_logical_aggregate().unwrap();

        assert_eq!(
            plan.agg_calls(),
            vec![BoundAggCall {
                kind: AggKind::Avg,
                args: vec![input_refs[0].clone()],
                return_type: DataTypeKind::Int32.not_null(),
            }]
        );
    }
}
