use super::*;
use crate::binder::*;
use crate::catalog::ColumnRefId;

/// Resolves column references into physical indices into the `DataChunk`.
///
/// This will rewrite all `ColumnRef` expressions to `InputRef`.
#[derive(Default)]
pub struct InputRefResolver {
    /// The output columns of the last visited plan.
    ///
    /// For those plans that don't change columns (e.g. Order, Filter), this variable should
    /// not be touched. For other plans that change columns (e.g. SeqScan, Join, Projection,
    /// Aggregate), this variable should be set before the function returns.
    bindings: Vec<Option<ColumnRefId>>,
}

impl Rewriter for InputRefResolver {
    fn rewrite_logical_join_is_nested(&mut self) -> bool {
        true
    }
    fn rewrite_logical_join(&mut self, mut plan: LogicalJoin) -> PlanRef {
        plan.left_plan = plan.left_plan.rewrite(self);
        let mut resolver = Self::default();
        plan.right_plan = plan.right_plan.rewrite(&mut resolver);
        self.bindings.append(&mut resolver.bindings);

        plan.rewrite_expr(self);
        Rc::new(plan)
    }

    fn rewrite_logical_seq_scan_is_nested(&mut self) -> bool {
        true
    }
    fn rewrite_logical_seq_scan(&mut self, plan: LogicalSeqScan) -> PlanRef {
        self.bindings = plan
            .column_ids
            .iter()
            .map(|col_id| Some(ColumnRefId::from_table(plan.table_ref_id, *col_id)))
            .collect();
        Rc::new(plan)
    }

    fn rewrite_logical_projection_is_nested(&mut self) -> bool {
        true
    }
    fn rewrite_logical_projection(&mut self, mut plan: LogicalProjection) -> PlanRef {
        plan.child = plan.child.rewrite(self);
        let bindings = plan
            .project_expressions
            .iter()
            .map(|expr| match &expr {
                BoundExpr::ColumnRef(col) => Some(col.column_ref_id),
                _ => None,
            })
            .collect();
        plan.rewrite_expr(self);
        self.bindings = bindings;
        Rc::new(plan)
    }

    fn rewrite_logical_aggregate_is_nested(&mut self) -> bool {
        true
    }
    fn rewrite_logical_aggregate(&mut self, mut plan: LogicalAggregate) -> PlanRef {
        plan.child = plan.child.rewrite(self);
        for expr in &plan.group_keys {
            match &expr {
                BoundExpr::ColumnRef(col) => self.bindings.push(Some(col.column_ref_id)),
                _ => panic!("{:?} cannot be a group key", expr),
            }
        }
        plan.rewrite_expr(self);
        Rc::new(plan)
    }

    /// Transform expr referring to input chunk into `BoundInputRef`
    fn rewrite_expr(&mut self, expr: &mut BoundExpr) {
        use BoundExpr::*;
        match expr {
            ColumnRef(column_ref) => {
                let new = InputRef(BoundInputRef {
                    index: self
                        .bindings
                        .iter()
                        .position(|col| *col == Some(column_ref.column_ref_id))
                        .expect("column reference not found"),
                    return_type: column_ref.desc.datatype().clone(),
                });
                *expr = new;
            }
            AggCall(agg) => {
                for expr in &mut agg.args {
                    self.rewrite_expr(expr);
                }
            }
            // rewrite sub-expressions
            BinaryOp(binary_op) => {
                self.rewrite_expr(&mut *binary_op.left_expr);
                self.rewrite_expr(&mut *binary_op.right_expr);
            }
            UnaryOp(unary_op) => {
                self.rewrite_expr(&mut *unary_op.expr);
            }
            TypeCast(cast) => {
                self.rewrite_expr(&mut *cast.expr);
            }
            IsNull(isnull) => {
                self.rewrite_expr(&mut *isnull.expr);
            }
            _ => {}
        }
    }
}
