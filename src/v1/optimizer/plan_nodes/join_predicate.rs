use serde::Serialize;

use crate::parser::BinaryOperator;
use crate::types::{DataTypeKind, DataValue};
use crate::v1::binder::{BoundBinaryOp, BoundExpr, BoundInputRef};
use crate::v1::optimizer::expr_utils::{conjunctions, input_col_refs, merge_conjunctions};
use crate::v1::optimizer::logical_plan_rewriter::ExprRewriter;
use crate::v1::optimizer::BoundExpr::InputRef;

#[derive(Debug, Clone, Serialize)]
/// the join predicate used in optimizer
pub struct JoinPredicate {
    /// the conditions that all columns in the left side,
    left_conds: Vec<BoundExpr>,
    /// the conditions that all columns in the right side,
    right_conds: Vec<BoundExpr>,
    /// other conditions, linked with AND conjunction.
    other_conds: Vec<BoundExpr>,

    /// the equal columns indexes(in the input schema) both sides,
    /// the first is from the left table and the second is from the right table.
    /// now all are normal equal(not null-safe-equal),
    eq_keys: Vec<(BoundInputRef, BoundInputRef)>,
}
#[allow(dead_code)]
impl JoinPredicate {
    pub fn new(
        other_conds: Vec<BoundExpr>,
        left_conds: Vec<BoundExpr>,
        right_conds: Vec<BoundExpr>,
        eq_keys: Vec<(BoundInputRef, BoundInputRef)>,
    ) -> Self {
        Self {
            left_conds,
            right_conds,
            other_conds,
            eq_keys,
        }
    }

    /// `create` will analyze the on clause condition and construct a `JoinPredicate`.
    /// e.g.
    /// ```sql
    ///   select a.v1, a.v2, b.v1, b.v2 from a join b on a.v1 = a.v2 and a.v1 = b.v1 and a.v2 > b.v2
    /// ```
    /// will call the `create` function with `left_colsnum` = 2 and `on_clause` is (supposed
    /// `input_ref` count start from 0)
    /// ```sql
    /// input_ref(0) = input_ref(1) and input_ref(0) = input_ref(2) and input_ref(1) > input_ref(3)
    /// ```
    /// And the `create funcitons` should return `JoinPredicate`
    /// ```sql
    ///   other_conds = Vec[input_ref(1) = input_ref(1), input_ref(1) > input_ref(3)],
    ///   eq_keys= Vec[(1,1)]
    /// ```
    pub fn create(left_cols_num: usize, on_clause: BoundExpr) -> Self {
        let conds = conjunctions(on_clause);
        let mut other_conds = vec![];
        let mut left_conds = vec![];
        let mut right_conds = vec![];
        let mut eq_keys = vec![];

        for cond in conds {
            if let BoundExpr::Constant(DataValue::Bool(f)) = cond {
                if f {
                    continue;
                }
            }
            let cols = input_col_refs(&cond);
            let from_left = cols
                .iter()
                .min()
                .map(|mx| mx < left_cols_num)
                .unwrap_or(false);
            let from_right = cols
                .iter()
                .max()
                .map(|mx| mx >= left_cols_num)
                .unwrap_or(false);
            match (from_left, from_right) {
                (true, true) => {
                    // TODO: refactor with if_chain
                    let mut is_other = true;
                    if let BoundExpr::BinaryOp(op) = &cond {
                        // TODO: if the eq condition's input is another expression, we should
                        // insert project as the join's input plan node
                        if let (BinaryOperator::Eq, InputRef(x), InputRef(y)) =
                            (&op.op, &*op.left_expr, &*op.right_expr)
                        {
                            if x.index < y.index {
                                eq_keys.push((x.clone(), y.clone()));
                            } else {
                                eq_keys.push((y.clone(), x.clone()));
                            }
                            is_other = false;
                        }
                    }
                    if is_other {
                        other_conds.push(cond)
                    }
                }
                (true, false) => left_conds.push(cond),
                (false, true) => right_conds.push(cond),
                (false, false) => other_conds.push(cond),
            }
        }
        Self::new(other_conds, left_conds, right_conds, eq_keys)
    }

    /// Get a reference to the join predicate's non eq conds.
    pub fn other_conds(&self) -> &[BoundExpr] {
        self.other_conds.as_ref()
    }

    /// Get a reference to the join predicate's left conds.
    pub fn left_conds(&self) -> &[BoundExpr] {
        self.left_conds.as_ref()
    }

    /// Get a reference to the join predicate's right conds.
    pub fn right_conds(&self) -> &[BoundExpr] {
        self.right_conds.as_ref()
    }

    /// Get join predicate's eq conds.
    pub fn eq_conds(&self) -> Vec<BoundExpr> {
        self.eq_keys
            .iter()
            .cloned()
            .map(|(l, r)| {
                BoundExpr::BinaryOp(BoundBinaryOp {
                    op: BinaryOperator::Eq,
                    left_expr: Box::new(InputRef(l)),
                    right_expr: Box::new(InputRef(r)),
                    return_type: DataTypeKind::Bool.nullable(),
                })
            })
            .collect()
    }

    /// Get a reference to the join predicate's eq keys.
    pub fn eq_keys(&self) -> &[(BoundInputRef, BoundInputRef)] {
        self.eq_keys.as_ref()
    }
    pub fn left_eq_keys(&self) -> Vec<BoundInputRef> {
        self.eq_keys.iter().map(|(left, _)| left.clone()).collect()
    }
    pub fn right_eq_keys(&self) -> Vec<BoundInputRef> {
        self.eq_keys
            .iter()
            .map(|(_, right)| right.clone())
            .collect()
    }
    pub fn to_on_clause(&self) -> BoundExpr {
        merge_conjunctions(
            self.left_conds
                .iter()
                .cloned()
                .chain(self.right_conds.iter().cloned())
                .chain(self.other_conds.iter().cloned())
                .chain(self.eq_conds().into_iter()),
        )
    }

    pub fn clone_with_rewrite_expr(
        &self,
        left_cols_num: usize,
        rewriter: &impl ExprRewriter,
    ) -> Self {
        let mut new_cond = self.to_on_clause();
        rewriter.rewrite_expr(&mut new_cond);
        Self::create(left_cols_num, new_cond)
    }
}
impl std::fmt::Display for JoinPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.to_on_clause())
    }
}
