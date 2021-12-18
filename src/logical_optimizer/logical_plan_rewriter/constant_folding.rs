use super::*;
use crate::array::ArrayImpl;
use crate::binder::BoundExpr;

/// Constant folding rule aims to evalute the constant expression before query execution.
///
/// For example,
/// `select 3 * 2 * a from t where a >= 100 * 300;`
/// The rule will convert it into
/// `select 6 * a from t where a >= 30000;`
#[derive(Default)]
pub struct ConstantFolding;

impl Rewriter for ConstantFolding {
    fn rewrite_expr(&mut self, expr: &mut BoundExpr) {
        use BoundExpr::*;
        match expr {
            BinaryOp(op) => {
                self.rewrite_expr(&mut *op.left_expr);
                self.rewrite_expr(&mut *op.right_expr);
                if let (Constant(v1), Constant(v2)) = (&*op.left_expr, &*op.right_expr) {
                    let res = ArrayImpl::from(v1)
                        .binary_op(&op.op, &ArrayImpl::from(v2))
                        .get(0);
                    *expr = Constant(res);
                }
            }
            UnaryOp(op) => {
                self.rewrite_expr(&mut *op.expr);
                if let Constant(v) = &*op.expr {
                    let res = ArrayImpl::from(v).unary_op(&op.op).get(0);
                    *expr = Constant(res);
                }
            }
            TypeCast(cast) => {
                self.rewrite_expr(&mut *cast.expr);
                if let Constant(v) = &*cast.expr {
                    let res = ArrayImpl::from(v).try_cast(cast.ty.clone()).unwrap().get(0);
                    *expr = Constant(res);
                }
            }
            AggCall(agg_call) => {
                for expr in &mut agg_call.args {
                    self.rewrite_expr(expr);
                }
            }
            _ => {}
        }
    }
}
