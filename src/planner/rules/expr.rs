//! Expression simplification rules and constant folding.

use super::*;
use crate::array::ArrayImpl;
use crate::types::DataValue;

/// Returns all rules of expression simplification.
#[rustfmt::skip]
pub fn rules() -> Vec<Rewrite> { vec![
    rw!("add-zero";  "(+ ?a 0)" => "?a"),
    rw!("add-comm";  "(+ ?a ?b)" => "(+ ?b ?a)"),
    rw!("add-assoc"; "(+ ?a (+ ?b ?c))" => "(+ (+ ?a ?b) ?c)"),
    rw!("add-same";  "(+ ?a ?a)" => "(* ?a 2)"),
    rw!("add-neg";   "(+ ?a (- ?b))" => "(- ?a ?b)"),

    rw!("mul-zero";  "(* ?a 0)" => "0"),
    rw!("mul-one";   "(* ?a 1)" => "?a"),
    rw!("mul-minus"; "(* ?a -1)" => "(- ?a)"),
    rw!("mul-comm";  "(* ?a ?b)"        => "(* ?b ?a)"),
    rw!("mul-assoc"; "(* ?a (* ?b ?c))" => "(* (* ?a ?b) ?c)"),

    // rw!("sub-canon"; "(- ?a ?b)" => "(+ ?a (* -1 ?b))"),
    // rw!("canon-sub"; "(+ ?a (* -1 ?b))" => "(- ?a ?b)"),

    rw!("neg-neg";    "(- (- ?a))" => "?a"),
    rw!("neg-sub";    "(- (- ?a ?b))" => "(- ?b ?a)"),

    rw!("sub-zero";   "(- ?a 0)" => "?a"),
    rw!("zero-sub";   "(- 0 ?a)" => "(- ?a)"),
    rw!("sub-cancel"; "(- ?a ?a)" => "0"),

    rw!("div-cancel"; "(/ ?a ?a)" => "1" if is_not_zero("?a")),

    rw!("mul-add-distri";   "(* ?a (+ ?b ?c))" => "(+ (* ?a ?b) (* ?a ?c))"),
    rw!("mul-add-factor";   "(+ (* ?a ?b) (* ?a ?c))" => "(* ?a (+ ?b ?c))"),

    rw!("recip-mul-div"; "(* ?x (/ 1 ?x))" => "1" if is_not_zero("?x")),

    rw!("eq-eq";     "(=  ?a ?a)" => "true"),
    rw!("ne-eq";     "(<> ?a ?a)" => "false"),
    rw!("gt-eq";     "(>  ?a ?a)" => "false"),
    rw!("lt-eq";     "(<  ?a ?a)" => "false"),
    rw!("ge-eq";     "(>= ?a ?a)" => "true"),
    rw!("le-eq";     "(<= ?a ?a)" => "true"),
    rw!("eq-comm";   "(=  ?a ?b)" => "(=  ?b ?a)"),
    rw!("ne-comm";   "(<> ?a ?b)" => "(<> ?b ?a)"),
    rw!("gt-comm";   "(>  ?a ?b)" => "(<  ?b ?a)"),
    rw!("lt-comm";   "(<  ?a ?b)" => "(>  ?b ?a)"),
    rw!("ge-comm";   "(>= ?a ?b)" => "(<= ?b ?a)"),
    rw!("le-comm";   "(<= ?a ?b)" => "(>= ?b ?a)"),
    rw!("eq-add";    "(=  (+ ?a ?b) ?c)" => "(=  ?a (- ?c ?b))"),
    rw!("ne-add";    "(<> (+ ?a ?b) ?c)" => "(<> ?a (- ?c ?b))"),
    rw!("gt-add";    "(>  (+ ?a ?b) ?c)" => "(>  ?a (- ?c ?b))"),
    rw!("lt-add";    "(<  (+ ?a ?b) ?c)" => "(<  ?a (- ?c ?b))"),
    rw!("ge-add";    "(>= (+ ?a ?b) ?c)" => "(>= ?a (- ?c ?b))"),
    rw!("le-add";    "(<= (+ ?a ?b) ?c)" => "(<= ?a (- ?c ?b))"),
    rw!("eq-trans";  "(and (= ?a ?b) (= ?b ?c))" => "(and (= ?a ?b) (= ?a ?c))"),

    rw!("not-eq";    "(not (=  ?a ?b))" => "(<> ?a ?b)"),
    rw!("not-ne";    "(not (<> ?a ?b))" => "(=  ?a ?b)"),
    rw!("not-gt";    "(not (>  ?a ?b))" => "(<= ?a ?b)"),
    rw!("not-ge";    "(not (>= ?a ?b))" => "(<  ?a ?b)"),
    rw!("not-lt";    "(not (<  ?a ?b))" => "(>= ?a ?b)"),
    rw!("not-le";    "(not (<= ?a ?b))" => "(>  ?a ?b)"),
    rw!("not-and";   "(not (and ?a ?b))" => "(or  (not ?a) (not ?b))"),
    rw!("not-or";    "(not (or  ?a ?b))" => "(and (not ?a) (not ?b))"),
    rw!("not-not";   "(not (not ?a))"    => "?a"),

    rw!("and-false"; "(and false ?a)"   => "false"),
    rw!("and-true";  "(and true ?a)"    => "?a"),
    rw!("and-null";  "(and null ?a)"    => "null"),
    rw!("and-comm";  "(and ?a ?b)"      => "(and ?b ?a)"),
    rw!("and-assoc"; "(and ?a (and ?b ?c))" => "(and (and ?a ?b) ?c)"),

    rw!("or-false";  "(or false ?a)" => "?a"),
    rw!("or-true";   "(or true ?a)"  => "true"),
    rw!("or-null";   "(or null ?a)"  => "null"),
    rw!("or-comm";   "(or ?a ?b)"    => "(or ?b ?a)"),
    rw!("or-assoc";  "(or ?a (or ?b ?c))" => "(or (or ?a ?b) ?c)"),

    rw!("if-false";  "(if false ?then ?else)" => "?else"),
    rw!("if-true";   "(if true ?then ?else)" => "?then"),
    rw!("if-not";    "(if (not ?cond) ?then ?else)" => "(if ?cond ?else ?then)"),

    rw!("avg";       "(avg ?a)" => "(/ (sum ?a) (count ?a))"),
]}

/// The data type of constant analysis.
///
/// `Some` for a known constant, `None` for unknown.
pub type ConstValue = Option<DataValue>;

/// Evaluate constant for a node.
pub fn eval_constant(egraph: &EGraph, enode: &Expr) -> ConstValue {
    use Expr::*;
    let x = |i: Id| egraph[i].data.constant.as_ref();
    if let Constant(v) = enode {
        Some(v.clone())
    } else if let Nested(e) = enode {
        Some(x(*e)?.clone())
    } else if let Some((op, a, b)) = enode.binary_op() {
        let (a, b) = (x(a)?, x(b)?);
        if a.is_null() || b.is_null() {
            return Some(DataValue::Null);
        }
        let array_a = ArrayImpl::from(a);
        let array_b = ArrayImpl::from(b);
        Some(array_a.binary_op(&op, &array_b).ok()?.get(0))
    } else if let Some((op, a)) = enode.unary_op() {
        let a = x(a)?;
        if a.is_null() {
            return Some(DataValue::Null);
        }
        let array_a = ArrayImpl::from(a);
        Some(array_a.unary_op(&op).ok()?.get(0))
    } else if let &IsNull(a) = enode {
        Some(DataValue::Bool(x(a)?.is_null()))
    } else if let &Cast([ty, a]) = enode {
        let a = x(a)?;
        if a.is_null() {
            return Some(DataValue::Null);
        }
        let ty = egraph[ty].nodes[0].as_type();
        // TODO: handle cast error
        a.cast(ty).ok()
    } else if let &Max(a) | &Min(a) | &Avg(a) | &First(a) | &Last(a) = enode {
        x(a).cloned()
    } else {
        None
    }
}

/// Union `id` with a new constant node if it's constant.
pub fn union_constant(egraph: &mut EGraph, id: Id) {
    if let Some(val) = &egraph[id].data.constant {
        let added = egraph.add(Expr::Constant(val.clone()));
        egraph.union(id, added);
        // prune other nodes
        egraph[id].nodes.retain(|n| n.is_leaf());
    }
}

/// Returns true if the expression is a non-zero constant.
fn is_not_zero(var: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    value_is(var, |v| !v.is_zero())
}

fn value_is(v: &str, f: impl Fn(&DataValue) -> bool) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let v = var(v);
    move |egraph, _, subst| {
        if let Some(n) = &egraph[subst[v]].data.constant {
            f(n)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::rules;

    egg::test_fn! {
        and_eq_const,
        rules(),
        "(and (= a 1) (= a b))" => "(and (= a 1) (= b 1))",
    }

    egg::test_fn! {
        #[cfg_attr(feature = "simd", ignore)] // FIXME: 'attempt to divide by zero'
        constant_folding,
        rules(),
        "(* (- (+ 1 2) 4) (/ 6 2))" => "-3",
    }

    egg::test_fn! {
        constant_moving,
        rules(),
        "(> (+ 100 a) 300)" => "(> a 200)",
    }

    egg::test_fn! {
        constant_type_cast,
        rules(),
        "(cast BOOLEAN 1)" => "true",
    }
}
