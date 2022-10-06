use super::*;

/// Returns all rules of aggregation extraction.
#[rustfmt::skip]
pub fn rules() -> Vec<Rewrite> { vec![
    rw!("extract-agg-from-select-list";
        "(select ?exprs ?from ?where ?groupby ?having)" =>
        { ExtractAgg {
            has_agg: pattern("
            (proj ?exprs
                (filter ?having
                    (agg ?aggs ?groupby
                        (filter ?where
                            ?from
                        )
                    )
                )
            )"),
            no_agg: pattern("(proj ?exprs (filter ?where ?from))"),
            src: var("?exprs"),
            output: var("?aggs"),
        }}
    ),
]}

/// The data type of aggragation analysis.
pub type AggSet = HashSet<Expr>;

/// Returns all aggragations in the tree.
pub fn analyze_aggs(egraph: &EGraph, enode: &Expr) -> AggSet {
    use Expr::*;
    let x = |i: &Id| &egraph[*i].data.aggs;
    if let RowCount = enode {
        return [enode.clone()].into_iter().collect();
    }
    if let Max(c) | Min(c) | Sum(c) | Avg(c) | Count(c) | First(c) | Last(c) = enode {
        assert!(x(c).is_empty(), "agg in agg");
        return [enode.clone()].into_iter().collect();
    }
    // TODO: ignore plan nodes
    // merge the set from all children
    (enode.children().iter())
        .flat_map(|id| x(id).iter().cloned())
        .collect()
}

/// Extracts all agg expressions from `src`.
/// If any, apply `has_agg` and put those aggs to `output`.
/// Otherwise, apply `no_agg`.
struct ExtractAgg {
    has_agg: Pattern<Expr>,
    no_agg: Pattern<Expr>,
    src: Var,
    output: Var,
}

impl Applier<Expr, ExprAnalysis> for ExtractAgg {
    fn apply_one(
        &self,
        egraph: &mut EGraph,
        eclass: Id,
        subst: &Subst,
        searcher_ast: Option<&PatternAst<Expr>>,
        rule_name: Symbol,
    ) -> Vec<Id> {
        let aggs = egraph[subst[self.src]].data.aggs.clone();
        if aggs.is_empty() {
            // FIXME: what if groupby not empty??
            return self
                .no_agg
                .apply_one(egraph, eclass, subst, searcher_ast, rule_name);
        }
        let mut list: Box<[Id]> = aggs.into_iter().map(|agg| egraph.add(agg)).collect();
        // make sure the order of the aggs is deterministic
        list.sort();
        let mut subst = subst.clone();
        subst.insert(self.output, egraph.add(Expr::List(list)));
        self.has_agg
            .apply_one(egraph, eclass, &subst, searcher_ast, rule_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rules() -> Vec<Rewrite> {
        let mut rules = vec![];
        rules.append(&mut expr::rules());
        rules.append(&mut plan::rules());
        rules.append(&mut agg::rules());
        rules
    }

    egg::test_fn! {
        plan_select,
        rules(),
        // SELECT sum(a + b) + count(a) + a FROM t
        // WHERE b > 1 GROUP BY a HAVING count(a) > 1;
        "
        (select
            (list (+ (+ (sum (+ $1.1 $1.2)) (count $1.1)) $1.1))
            (scan (list $1.1 $1.2 $1.3))
            (> $1.2 1)
            (list $1.1)
            (> (count $1.1) 1)
        )" => "
        (proj (list (+ (+ (sum (+ $1.1 $1.2)) (count $1.1)) $1.1))
            (filter (> (count $1.1) 1)
                (agg (list (sum (+ $1.1 $1.2)) (count $1.1)) (list $1.1)
                    (filter (> $1.2 1)
                        (scan (list $1.1 $1.2 $1.3))
                    )
                )
            )
        )"
    }

    egg::test_fn! {
        no_agg,
        rules(),
        // SELECT a FROM t;
        "
        (select
            (list $1.1)
            (scan (list $1.1 $1.2 $1.3))
            true (list) true
        )" => "
        (proj (list $1.1)
            (scan (list $1.1 $1.2 $1.3))
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
            (list $1.2 $2.2)
            (join inner true
                (scan (list $1.1 $1.2))
                (scan (list $2.1 $2.2 $2.3))
            )
            (and (= $1.1 $2.1) (= $2.3 'A'))
            (list)
            true
        )" => "
        (proj (list $1.2 $2.2)
        (join inner (= $1.1 $2.1)
            (scan (list $1.1 $1.2))
            (filter (= $2.3 'A')
                (scan (list $2.1 $2.2 $2.3))
            )
        ))"
    }
}
