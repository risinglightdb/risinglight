use crate::binder::BoundExpr;
use crate::optimizer::expr_utils::{conjunctions, input_col_refs};
use crate::optimizer::BoundExpr::InputRef;
use crate::parser::BinaryOperator;

#[derive(Debug, Clone)]
/// the join predicate used in optimizer
pub struct JoinPredicate {
    /// the conditions that all columns in the left side,
    left_conds: Vec<BoundExpr>,
    /// the conditions that all columns in the right side,
    right_conds: Vec<BoundExpr>,
    /// other conditions, linked with AND conjunction.
    other_conds: Vec<BoundExpr>,

    /// the equal columns indexes(in the input schema) both sides, now all are normal equal(not
    /// null-safe-equal),
    eq_keys: Vec<(usize, usize)>,
}
#[allow(dead_code)]
impl JoinPredicate {
    pub fn new(
        other_conds: Vec<BoundExpr>,
        left_conds: Vec<BoundExpr>,
        right_conds: Vec<BoundExpr>,
        eq_keys: Vec<(usize, usize)>,
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
    ///   select a.v1, a.v2, b.v1, b.v2 from a,b on a.v1 = a.v2 and a.v1 = b.v1 and a.v2 > b.v2
    /// ```
    /// will call the `create` function with left_colsnum = 2 and on_clause is (supposed input_ref
    /// count start from 0)
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
            let cols = input_col_refs(&cond);
            let from_left = cols
                .iter()
                .min()
                .map(|mx| mx < left_cols_num)
                .unwrap_or(false);
            let from_right = cols
                .iter()
                .min()
                .map(|mx| mx >= left_cols_num)
                .unwrap_or(false);
            match (from_left, from_right) {
                (true, true) => {
                    // TODO: refactor with if_chain
                    let is_other = true;
                    if let BoundExpr::BinaryOp(op) = &cond {
                        match (&op.op, &*op.left_expr, &*op.right_expr) {
                            // TODO: if the eq condition's input is another expression, we should
                            // insert project as the join's input plan node
                            (BinaryOperator::Eq, InputRef(x), InputRef(y)) => {
                                let l = x.index.min(y.index);
                                let r = x.index.max(y.index);
                                eq_keys.push((l, r));
                            }
                            _ => {}
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

    /// Get a reference to the join predicate's eq keys.
    pub fn eq_keys(&self) -> &[(usize, usize)] {
        self.eq_keys.as_ref()
    }
    pub fn left_eq_keys(&self) -> Vec<usize> {
        self.eq_keys.iter().map(|(left, _)| *left).collect()
    }
    pub fn right_eq_keys(&self) -> Vec<usize> {
        self.eq_keys.iter().map(|(_, right)| *right).collect()
    }
}
