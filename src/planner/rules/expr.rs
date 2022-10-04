use super::*;
use crate::array::ArrayImpl;
use crate::types::DataValue;

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
    rw!("and-null";  "(and null ?a)"    => "?a"),
    rw!("and-comm";  "(and ?a ?b)"      => "(and ?b ?a)"),
    rw!("and-assoc"; "(and ?a (and ?b ?c))" => "(and (and ?a ?b) ?c)"),

    rw!("or-false";  "(or false ?a)" => "?a"),
    rw!("or-true";   "(or true ?a)"  => "true"),
    rw!("or-null";   "(or null ?a)"  => "?a"),
    rw!("or-comm";   "(or ?a ?b)"    => "(or ?b ?a)"),
    rw!("or-assoc";  "(or ?a (or ?b ?c))" => "(or (or ?a ?b) ?c)"),

    rw!("avg";       "(avg ?a)" => "(/ (sum ?a) (count ?a))"),
]}

pub type ConstValue = Option<DataValue>;

/// Evaluate constant.
pub fn eval_constant(egraph: &EGraph, enode: &Expr) -> ConstValue {
    use Expr::*;
    let x = |i: Id| egraph[i].data.val.as_ref();
    if let Constant(v) = enode {
        Some(v.clone())
    } else if let Some((op, a, b)) = enode.binary_op() {
        let array_a = ArrayImpl::from(x(a)?);
        let array_b = ArrayImpl::from(x(b)?);
        Some(array_a.binary_op(&op, &array_b).get(0))
    } else if let Some((op, a)) = enode.unary_op() {
        let array_a = ArrayImpl::from(x(a)?);
        Some(array_a.unary_op(&op).get(0))
    } else if let &IsNull(a) = enode {
        Some(DataValue::Bool(x(a)?.is_null()))
    } else if let &Cast(_) = enode {
        // TODO: evaluate type cast
        None
    } else if let &Max(a) | &Min(a) | &Avg(a) | &First(a) | &Last(a) = enode {
        x(a).cloned()
    } else {
        None
    }
}

pub fn modify(egraph: &mut EGraph, id: Id) {
    // add a new constant node
    if let Some(val) = &egraph[id].data.val {
        let added = egraph.add(Expr::Constant(val.clone()));
        egraph.union(id, added);
    }
}

fn is_not_zero(var: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    value_is(var, |v| !v.is_zero())
}

fn is_const(var: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    value_is(var, |_| true)
}

fn value_is(v: &str, f: impl Fn(&DataValue) -> bool) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let v = var(v);
    move |egraph, _, subst| {
        if let Some(n) = &egraph[subst[v]].data.val {
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
        constant_folding,
        rules(),
        "(* (- (+ 1 2) 4) (/ 6 2))" => "-3",
    }

    egg::test_fn! {
        constant_moving,
        rules(),
        "(> (+ 100 a) 300)" => "(> a 200)",
    }
}
