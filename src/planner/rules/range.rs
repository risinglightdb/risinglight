// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! Range filter.

use std::ops::Bound;

use super::*;
use crate::catalog::ColumnRefId;
use crate::storage::KeyRange;

/// The data type of range analysis.
///
/// If Some, the expression is a range condition.
///
/// ```text
/// a = 1
/// a > 1
/// a <= 1
/// -1 < a < 1
/// ```
pub type RangeCondition = Option<(ColumnRefId, KeyRange)>;

/// Returns all columns involved in the node.
pub fn analyze_range(egraph: &EGraph, enode: &Expr) -> RangeCondition {
    use Expr::*;
    let column = |i: &Id| {
        egraph[*i].nodes.iter().find_map(|e| match e {
            Expr::Column(c) => Some(*c),
            _ => None,
        })
    };
    let range = |i: &Id| egraph[*i].data.range.as_ref();
    let constant = |i: &Id| egraph[*i].data.constant.as_ref();
    match enode {
        Eq([a, b]) | Gt([a, b]) | GtEq([a, b]) | Lt([a, b]) | LtEq([a, b]) => {
            // normalize `v op k` to `k op v`
            let (k, v, enode) = if let (Some(v), Some(k)) = (constant(a), column(b)) {
                let revnode = match enode {
                    Eq(_) => Eq([*b, *a]),
                    Gt(_) => Lt([*b, *a]),
                    GtEq(_) => LtEq([*b, *a]),
                    Lt(_) => Gt([*b, *a]),
                    LtEq(_) => GtEq([*b, *a]),
                    _ => unreachable!(),
                };
                (k, v, revnode)
            } else if let (Some(k), Some(v)) = (column(a), constant(b)) {
                (k, v, enode.clone())
            } else {
                return None;
            };
            let start = match enode {
                Eq(_) | GtEq(_) => Bound::Included(v.clone()),
                Gt(_) => Bound::Excluded(v.clone()),
                Lt(_) | LtEq(_) => Bound::Unbounded,
                _ => unreachable!(),
            };
            let end = match enode {
                Eq(_) | LtEq(_) => Bound::Included(v.clone()),
                Lt(_) => Bound::Excluded(v.clone()),
                Gt(_) | GtEq(_) => Bound::Unbounded,
                _ => unreachable!(),
            };
            Some((k, KeyRange { start, end }))
        }
        And([a, b]) => {
            let (ka, ra) = range(a)?;
            let (kb, rb) = range(b)?;
            if ka != kb {
                return None;
            }
            // if both a and b have bound at start or end, return None
            let start = match (&ra.start, &rb.start) {
                (Bound::Unbounded, s) | (s, Bound::Unbounded) => s.clone(),
                _ => return None,
            };
            let end = match (&ra.end, &rb.end) {
                (Bound::Unbounded, s) | (s, Bound::Unbounded) => s.clone(),
                _ => return None,
            };
            Some((*ka, KeyRange { start, end }))
        }
        _ => None,
    }
}

#[rustfmt::skip]
pub fn filter_scan_rule() -> Vec<Rewrite> { vec![
    // pushdown range condition to scan
    rw!("filter-scan";
        "(filter ?cond (scan ?table ?columns true))" =>
        "(scan ?table ?columns ?cond)"
        if is_primary_key_range("?cond")
    ),
    rw!("filter-scan-1";
        "(filter (and ?cond1 ?cond2) (scan ?table ?columns true))" =>
        "(filter ?cond2 (scan ?table ?columns ?cond1))"
        if is_primary_key_range("?cond1")
    ),
]}

/// Returns true if the expression is a primary key range.
fn is_primary_key_range(expr: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let var = var(expr);
    move |egraph, _, subst| {
        let Some((column, _)) = &egraph[subst[var]].data.range else {
            return false;
        };
        if let Some(col) = egraph.analysis.catalog.get_column(column) {
            col.is_primary()
        } else {
            // handle the case that catalog is not initialized, like in test cases
            false
        }
    }
}
