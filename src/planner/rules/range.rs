// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

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
        Eq([k, v]) | Gt([k, v]) | GtEq([k, v]) | Lt([k, v]) | LtEq([k, v]) => {
            let k = column(k)?;
            let v = constant(v)?;
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
        let Some((column, _)) = &egraph[subst[var]].data.range else { return false };
        egraph
            .analysis
            .catalog
            .get_column(column)
            .unwrap()
            .is_primary()
    }
}
