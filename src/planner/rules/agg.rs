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
    enode.children().iter().flat_map(x).collect()
}
