// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::v1::binder::{BoundExpr, ExprVisitor};
use crate::v1::optimizer::logical_plan_rewriter::ExprRewriter;

/// The logical plan of project operation.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalProjection {
    project_expressions: Vec<BoundExpr>,
    child: PlanRef,
}

/// Substitute `InputRef` with corresponding `BoundExpr`.
struct Substitute {
    mapping: Vec<BoundExpr>,
}

impl ExprRewriter for Substitute {
    fn rewrite_input_ref(&self, input_ref: &mut BoundExpr) {
        match input_ref {
            BoundExpr::InputRef(i) => {
                assert_eq!(self.mapping[i.index].return_type(), i.return_type.clone());
                *input_ref = self.mapping[i.index].clone();
            }
            _ => unreachable!(),
        }
    }
}

impl LogicalProjection {
    pub fn new(project_expressions: Vec<BoundExpr>, child: PlanRef) -> Self {
        if let Ok(child) = child.as_logical_projection() {
            let subst = Substitute {
                mapping: child.project_expressions.clone(),
            };
            let mut exprs = project_expressions;
            exprs.iter_mut().for_each(|expr| subst.rewrite_expr(expr));
            return LogicalProjection {
                project_expressions: exprs,
                child: child.child.clone(),
            };
        }
        Self {
            project_expressions,
            child,
        }
    }

    /// Get a reference to the logical projection's project expressions.
    pub fn project_expressions(&self) -> &[BoundExpr] {
        self.project_expressions.as_ref()
    }
    pub fn clone_with_rewrite_expr(
        &self,
        new_child: PlanRef,
        rewriter: &impl ExprRewriter,
    ) -> Self {
        let mut new_exprs = self.project_expressions().to_vec();
        for expr in &mut new_exprs {
            rewriter.rewrite_expr(expr);
        }
        LogicalProjection::new(new_exprs, new_child)
    }
}
impl PlanTreeNodeUnary for LogicalProjection {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.project_expressions().to_vec(), child)
    }
}
impl_plan_tree_node_for_unary!(LogicalProjection);
impl PlanNode for LogicalProjection {
    fn schema(&self) -> Vec<ColumnDesc> {
        let child_schema = self.child.schema();
        self.project_expressions
            .iter()
            .map(|expr| {
                let name = match expr {
                    BoundExpr::ColumnRef(column_ref) => column_ref.desc.name().to_string(),
                    BoundExpr::TypeCast(type_cast) => match &*type_cast.expr {
                        BoundExpr::ColumnRef(column_ref) => column_ref.desc.name().to_string(),
                        _ => type_cast.ty.to_string(),
                    },
                    BoundExpr::ExprWithAlias(expr_with_alias) => expr_with_alias.alias.clone(),
                    BoundExpr::InputRef(input_ref) => {
                        child_schema[input_ref.index].name().to_string()
                    }
                    _ => "?column?".to_string(),
                };
                expr.return_type().to_column(name)
            })
            .collect()
    }

    fn estimated_cardinality(&self) -> usize {
        self.child().estimated_cardinality()
    }

    fn prune_col(&self, required_cols: BitSet) -> PlanRef {
        let mut new_projection_expressions: Vec<BoundExpr> = required_cols
            .iter()
            .map(|index| self.project_expressions[index].clone())
            .collect();

        let mut visitor = CollectRequiredCols(BitSet::with_capacity(required_cols.len()));
        new_projection_expressions
            .iter()
            .for_each(|expr| visitor.visit_expr(expr));

        let input_cols = visitor.0;

        let mapper = Mapper::new_with_bitset(&input_cols);
        new_projection_expressions.iter_mut().for_each(|expr| {
            mapper.rewrite_expr(expr);
        });

        LogicalProjection::new(new_projection_expressions, self.child.prune_col(input_cols))
            .into_plan_ref()
    }
}

impl fmt::Display for LogicalProjection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalProjection: exprs {:?}", self.project_expressions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ColumnRefId;
    use crate::types::{DataTypeKind, DataValue};
    use crate::v1::binder::{BoundColumnRef, BoundExprWithAlias, BoundTypeCast};

    #[test]
    fn test_projection_out_names() {
        let plan = LogicalProjection::new(
            vec![
                BoundExpr::ColumnRef(BoundColumnRef {
                    column_ref_id: ColumnRefId::new(0, 0, 0, 0),
                    is_primary_key: false,
                    desc: DataTypeKind::Int32.not_null().to_column("v1".into()),
                }),
                BoundExpr::TypeCast(BoundTypeCast {
                    expr: Box::new(BoundExpr::Constant(DataValue::Int32(0))),
                    ty: DataTypeKind::Int32,
                }),
                BoundExpr::ExprWithAlias(BoundExprWithAlias {
                    expr: Box::new(BoundExpr::Constant(DataValue::Int32(0))),
                    alias: "alias".to_string(),
                }),
                BoundExpr::Constant(DataValue::Int32(0)),
            ],
            Arc::new(Dummy::new(Vec::new())),
        );

        let column_names = plan.out_names();
        assert_eq!(column_names[0], "v1");
        assert_eq!(column_names[1], DataTypeKind::Int32.to_string());
        assert_eq!(column_names[2], "alias");
        assert_eq!(column_names[3], "?column?");
    }

    #[test]
    fn test_nested_projection() {
        let inner = LogicalProjection::new(
            vec![
                BoundExpr::ColumnRef(BoundColumnRef {
                    column_ref_id: ColumnRefId::new(0, 0, 0, 0),
                    is_primary_key: false,
                    desc: DataTypeKind::Int32.not_null().to_column("v1".into()),
                }),
                BoundExpr::TypeCast(BoundTypeCast {
                    expr: Box::new(BoundExpr::Constant(DataValue::Int32(0))),
                    ty: DataTypeKind::Int32,
                }),
                BoundExpr::ExprWithAlias(BoundExprWithAlias {
                    expr: Box::new(BoundExpr::Constant(DataValue::Int32(0))),
                    alias: "alias".to_string(),
                }),
                BoundExpr::Constant(DataValue::Int32(0)),
            ],
            Arc::new(Dummy::new(Vec::new())),
        );

        let outer = LogicalProjection::new(
            vec![
                BoundExpr::InputRef(BoundInputRef {
                    index: 0,
                    return_type: DataTypeKind::Int32.not_null(),
                }),
                BoundExpr::Constant(DataValue::Int32(0)),
            ],
            Arc::new(inner),
        );

        let column_names = outer.out_names();
        assert_eq!(column_names[0], "v1");
        assert_eq!(column_names[1], "?column?");
        assert!(outer.child.as_dummy().is_ok());

        let outermost = LogicalProjection::new(
            vec![BoundExpr::InputRef(BoundInputRef {
                index: 0,
                return_type: DataTypeKind::Int32.not_null(),
            })],
            Arc::new(outer),
        );

        assert_eq!(outermost.out_names()[0], "v1");
        assert!(outermost.child.as_dummy().is_ok());
    }

    #[test]
    fn test_prune_projection() {
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

        let mut required_cols = BitSet::new();
        required_cols.insert(1);

        let plan = projection.prune_col(required_cols);
        let plan = plan.as_logical_projection().unwrap();
        assert_eq!(1, plan.project_expressions().len());
        assert_eq!(
            plan.project_expressions()[0],
            BoundExpr::InputRef(BoundInputRef {
                index: 0,
                return_type: ty,
            })
        );
        let child = plan.child.as_logical_table_scan().unwrap();
        assert_eq!(child.column_descs(), &col_descs[1..2]);
        assert_eq!(child.column_ids(), &[2]);
    }
}
