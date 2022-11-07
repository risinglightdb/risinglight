use super::*;

/// Returns all rules of aggregation extraction.
#[rustfmt::skip]
pub fn rules() -> Vec<Rewrite> { vec![
    rw!("proj-distinct-to-agg"; 
        "(proj ?exprs (order ?orderby (distinct ?on ?child)))" =>
        { DistinctToAgg {
            no_distinct: pattern("(proj ?exprs (order ?orderby ?child))"),
            has_distinct: pattern("(proj ?exprs (order ?orderby (agg ?aggs ?on ?child)))"),
            projection: var("?exprs"),
            distinct_on: var("?on"),
            aggs: var("?aggs"),
        }}
    ),
]}

/// The data type of aggragation analysis.
pub type AggSet = Result<Vec<Expr>, AggError>;

/// The error type of aggregation.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AggError {
    #[error("aggregate function calls cannot be nested")]
    Nested(Expr),
    #[error("column {0} must appear in the GROUP BY clause or be used in an aggregate function")]
    ColumnNotInAgg(String),
}

/// Returns all aggragations in the tree.
pub fn analyze_aggs(enode: &Expr, x: impl Fn(&Id) -> AggSet) -> AggSet {
    use Expr::*;
    if let RowCount = enode {
        return Ok(vec![enode.clone()]);
    }
    if let Max(c) | Min(c) | Sum(c) | Avg(c) | Count(c) | First(c) | Last(c) = enode {
        if !x(c)?.is_empty() {
            return Err(AggError::Nested(enode.clone()));
        }
        return Ok(vec![enode.clone()]);
    }
    // merge the set from all children
    // TODO: ignore plan nodes
    let mut aggs = vec![];
    for child in enode.children() {
        aggs.extend(x(child)?);
    }
    Ok(aggs)
}

/// Convert `distinct` to `agg`.
///
/// If `distinct_on` is empty, apply `no_distinct`.
/// Otherwise, apply `has_distinct`. The expressions in the `projection` who are not in
/// `distinct_on` will be aggregated by `first` and put to `aggs`.
struct DistinctToAgg {
    no_distinct: Pattern,
    has_distinct: Pattern,
    projection: Var,  // inout
    distinct_on: Var, // in
    aggs: Var,        // out
}

impl Applier<Expr, ExprAnalysis> for DistinctToAgg {
    fn apply_one(
        &self,
        egraph: &mut EGraph,
        eclass: Id,
        subst: &Subst,
        searcher_ast: Option<&PatternAst<Expr>>,
        rule_name: Symbol,
    ) -> Vec<Id> {
        let get_list = |var: Var| match &egraph[subst[var]].nodes[0] {
            Expr::List(list) => list.clone(),
            _ => panic!("not a list"),
        };
        let distinct_on = get_list(self.distinct_on);
        if distinct_on.is_empty() {
            return self
                .no_distinct
                .apply_one(egraph, eclass, subst, searcher_ast, rule_name);
        }
        let mut aggs = vec![];
        let mut projection = get_list(self.projection);
        for id in projection.iter_mut() {
            if !distinct_on.contains(id) {
                *id = egraph.add(Expr::First(*id));
                aggs.push(*id);
            }
        }
        let mut subst = subst.clone();
        subst.insert(self.aggs, egraph.add(Expr::List(aggs.into())));
        subst.insert(self.projection, egraph.add(Expr::List(projection)));
        self.has_distinct
            .apply_one(egraph, eclass, &subst, searcher_ast, rule_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rules() -> Vec<Rewrite> {
        let mut rules = vec![];
        rules.append(&mut expr::rules());
        rules.append(&mut plan::always_better_rules());
        rules.append(&mut plan::join_rules());
        rules.append(&mut agg::rules());
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
