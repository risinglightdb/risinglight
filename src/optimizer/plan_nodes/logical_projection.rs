// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::binder::BoundExpr;
use crate::optimizer::logical_plan_rewriter::ExprRewriter;

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
                assert_eq!(
                    self.mapping[i.index].return_type(),
                    Some(i.return_type.clone())
                );
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
                expr.return_type().unwrap().to_column(name)
            })
            .collect()
    }

    fn estimated_cardinality(&self) -> usize {
        self.child().estimated_cardinality()
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
    use crate::binder::{BoundColumnRef, BoundExprWithAlias, BoundTypeCast};
    use crate::catalog::ColumnRefId;
    use crate::types::{DataTypeExt, DataTypeKind, DataValue};

    #[test]
    fn test_projection_out_names() {
        let plan = LogicalProjection::new(
            vec![
                BoundExpr::ColumnRef(BoundColumnRef {
                    column_ref_id: ColumnRefId::new(0, 0, 0, 0),
                    is_primary_key: false,
                    desc: DataTypeKind::Int(None).not_null().to_column("v1".into()),
                }),
                BoundExpr::TypeCast(BoundTypeCast {
                    expr: Box::new(BoundExpr::Constant(DataValue::Int32(0))),
                    ty: DataTypeKind::Int(None),
                }),
                BoundExpr::ExprWithAlias(BoundExprWithAlias {
                    expr: Box::new(BoundExpr::Constant(DataValue::Int32(0))),
                    alias: "alias".to_string(),
                }),
                BoundExpr::Constant(DataValue::Int32(0)),
            ],
            Arc::new(Dummy {}),
        );

        let column_names = plan.out_names();
        assert_eq!(column_names[0], "v1");
        assert_eq!(column_names[1], DataTypeKind::Int(None).to_string());
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
                    desc: DataTypeKind::Int(None).not_null().to_column("v1".into()),
                }),
                BoundExpr::TypeCast(BoundTypeCast {
                    expr: Box::new(BoundExpr::Constant(DataValue::Int32(0))),
                    ty: DataTypeKind::Int(None),
                }),
                BoundExpr::ExprWithAlias(BoundExprWithAlias {
                    expr: Box::new(BoundExpr::Constant(DataValue::Int32(0))),
                    alias: "alias".to_string(),
                }),
                BoundExpr::Constant(DataValue::Int32(0)),
            ],
            Arc::new(Dummy {}),
        );

        let outer = LogicalProjection::new(
            vec![
                BoundExpr::InputRef(BoundInputRef {
                    index: 0,
                    return_type: DataTypeKind::Int(None).not_null(),
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
                return_type: DataTypeKind::Int(None).not_null(),
            })],
            Arc::new(outer),
        );

        assert_eq!(outermost.out_names()[0], "v1");
        assert!(outermost.child.as_dummy().is_ok());
    }
}
