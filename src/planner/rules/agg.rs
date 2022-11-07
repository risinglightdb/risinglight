use super::*;

/// The data type of aggragation analysis.
pub type AggSet = Vec<Expr>;

/// Returns all aggragations in the tree.
///
/// Note: if there is an agg over agg, e.g. `sum(count(a))`, only the upper one will be returned.
pub fn analyze_aggs(enode: &Expr, x: impl Fn(&Id) -> AggSet) -> AggSet {
    use Expr::*;
    if let RowCount | Max(_) | Min(_) | Sum(_) | Avg(_) | Count(_) | First(_) | Last(_) = enode {
        return vec![enode.clone()];
    }
    // merge the set from all children
    // TODO: ignore plan nodes
    enode.children().iter().map(x).flatten().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rules() -> Vec<Rewrite> {
        let mut rules = vec![];
        rules.append(&mut expr::rules());
        rules.append(&mut plan::always_better_rules());
        rules.append(&mut plan::join_rules());
        rules
    }

    egg::test_fn! {
        plan_select,
        rules(),
        // SELECT sum(a + b) + a FROM t
        // WHERE b > 1 GROUP BY a HAVING count(a) > 1;
        "
        (select
            (list)
            (list (+ (sum (+ $1.1 $1.2)) $1.1))
            (scan (list $1.1 $1.2 $1.3))
            (> $1.2 1)
            (list $1.1)
            (> (count $1.1) 1)
            (list)
        )" => "
        (proj (list (+ (sum (+ $1.1 $1.2)) $1.1))
            (filter (> (count $1.1) 1)
                (agg
                    (list (sum (+ $1.1 $1.2)) (count $1.1))
                    (list $1.1)
                    (filter (> $1.2 1)
                        (scan (list $1.1 $1.2 $1.3))
                    )
                )
            )
        )"
    }

    egg::test_fn! {
        select_group,
        rules(),
        // SELECT a FROM t GROUP BY a;
        "
        (select
            (list)
            (list $1.1)
            (scan (list $1.1 $1.2 $1.3))
            true (list $1.1) true (list)
        )" => "
        (proj (list $1.1)
            (agg (list) (list $1.1)
                (scan (list $1.1 $1.2 $1.3))
            )
        )"
    }

    egg::test_fn! {
        no_agg,
        rules(),
        // SELECT a FROM t;
        "
        (select
            (list)
            (list $1.1)
            (scan (list $1.1 $1.2 $1.3))
            true (list) true (list)
        )" => "
        (proj (list $1.1)
            (scan (list $1.1 $1.2 $1.3))
        )"
    }

    egg::test_fn! {
        distinct_on,
        rules(),
        // SELECT DISTINCT ON (a, b) a, c FROM t;
        // => SELECT a, FIRST(c) FROM t GROUP BY a, b;
        "
        (select
            (list $1.1 $1.2)
            (list $1.1 $1.3)
            (scan (list $1.1 $1.2 $1.3))
            true (list) true (list)
        )" => "
        (proj (list $1.1 (first $1.3))
            (agg (list (first $1.3)) (list $1.1 $1.2)
                (scan (list $1.1 $1.2 $1.3))
            )
        )"
    }

    egg::test_fn! {
        cmu15445_fall2021_lecture13_p17,
        rules(),
        // SELECT s.name, e.cid
        // FROM student AS s, enrolled AS e
        // WHERE s.sid = e.sid AND e.grade = 'A'
        "
        (select
            (list)
            (list $1.2 $2.2)
            (join inner true
                (scan (list $1.1 $1.2))
                (scan (list $2.1 $2.2 $2.3))
            )
            (and (= $1.1 $2.1) (= $2.3 'A'))
            (list) true (list)
        )" => "
        (proj (list $1.2 $2.2)
        (join inner (= $1.1 $2.1)
            (scan (list $1.1 $1.2))
            (filter (= $2.3 'A')
                (scan (list $2.1 $2.2 $2.3))
            )
        ))"
    }

    egg::test_fn! {
        tpch_q3,
        rules(),
        "(limit 10 0
            (select list (list $7.0 (sum (* $7.5 (- 1 $7.6))) $6.4 $6.7)
                (join inner true
                    (join inner true
                        (scan (list $5.0 $5.6))
                        (scan (list $6.0 $6.1 $6.4 $6.7)))
                    (scan (list $7.0 $7.5 $7.6 $7.10)))
                (and (and (and (and (= $5.6 'BUILDING') (= $5.0 $6.1)) (= $7.0 $6.0)) (< $6.4 1995-03-15)) (> $7.10 1995-03-15))
                (list $7.0 $6.4 $6.7)
                true
                (list (desc (sum (* $7.5 (- 1 $7.6)))) (asc $6.4))
            ))" => "
        (proj (list $7.0 (sum (* $7.5 (- 1 $7.6))) $6.4 $6.7)
            (topn 10 0 (list (desc (sum (* $7.5 (- 1 $7.6)))) (asc $6.4))
                (agg
                    (list (sum (* $7.5 (- 1 $7.6))))
                    (list $7.0 $6.4 $6.7)
                    (hashjoin inner (list $6.0) (list $7.0)
                        (hashjoin inner (list $5.0) (list $6.1)
                            (filter (= $5.6 'BUILDING')
                                (scan (list $5.0 $5.6)))
                            (filter (< $6.4 1995-03-15)
                                (scan (list $6.0 $6.1 $6.4 $6.7))))
                        (filter (> $7.10 1995-03-15)
                            (scan (list $7.0 $7.5 $7.6 $7.10)))))))"
    }
}
