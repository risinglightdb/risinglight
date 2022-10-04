use super::*;

#[rustfmt::skip]
pub fn rules() -> Vec<Rewrite> { vec![
    rw!("split-projagg";
        "(projagg ?exprs ?groupby ?child)" =>
        { ExtractAgg {
            has_agg: pattern("(proj ?exprs (agg ?aggs ?groupby ?child))"),
            no_agg: pattern("(proj ?exprs ?child)"),
            src: var("?exprs"),
            output: var("?aggs"),
        }}
    ),
]}

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
                .apply_one(egraph, eclass, &subst, searcher_ast, rule_name);
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
    use super::rules;

    egg::test_fn! {
        split_proj_agg,
        rules(),
        // SELECT sum(a + b) + count(a) + a FROM t GROUP BY a;
        "
        (projagg
            (list (+ (+ (sum (+ $1.1 $1.2)) (count $1.1)) $1.1))
            (list $1.1)
            (scan (list $1.1 $1.2 $1.3))
        )" => "
        (proj
            (list (+ (+ (sum (+ $1.1 $1.2)) (count $1.1)) $1.1))
            (agg
                (list (sum (+ $1.1 $1.2)) (count $1.1))
                (list $1.1)
                (scan (list $1.1 $1.2 $1.3))
            )
        )"
    }

    egg::test_fn! {
        no_agg,
        rules(),
        // SELECT a FROM t;
        "
        (projagg (list $1.1) (list)
            (scan (list $1.1 $1.2 $1.3))
        )" => "
        (proj (list $1.1)
            (scan (list $1.1 $1.2 $1.3))
        )"
    }
}
