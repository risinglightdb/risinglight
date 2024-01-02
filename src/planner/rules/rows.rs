// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use super::*;
use crate::catalog::{ColumnRefId, TableRefId};
use crate::types::DataValue;

/// The data type of row number analysis.
pub type Rows = f32;

/// Returns the estimated rows for plans, or selectivity for expressions.
pub fn analyze_rows(egraph: &EGraph, enode: &Expr) -> Rows {
    use Expr::*;
    let x = |i: &Id| egraph[*i].data.rows;
    let get_limit_num = |id: &Id| {
        (egraph[*id].data.constant.as_ref())
            .expect("limit should be constant")
            .as_usize()
            .unwrap()
            .map_or(f32::MAX, |x| x as f32)
    };
    let list_len = |id: &Id| egraph[*id].as_list().len();
    match enode {
        // for plan nodes, the result represents estimated rows
        Values(v) => v.len() as f32,
        Scan([tid, _, _]) => {
            let table_id = egraph[*tid].nodes[0].as_table();
            egraph
                .analysis
                .stat
                .get_row_count(table_id)
                .unwrap_or(DEFAULT_ROW_COUNT) as f32
        }
        Proj([_, c]) | Order([_, c]) | Window([_, c]) => x(c),
        Agg(_) => 1.0,
        HashAgg([_, group, _]) | SortAgg([_, group, _]) => {
            // TODO: consider distinct values of group keys
            10_u32.pow(list_len(group) as u32) as f32
        }
        Filter([cond, c]) => x(c) * x(cond),
        Limit([limit, _, c]) | TopN([limit, _, _, c]) => x(c).min(get_limit_num(limit)),
        Join([_, on, l, r]) => x(l) * x(r) * x(on),
        HashJoin([_, _, _, l, r]) | MergeJoin([_, _, _, l, r]) => x(l).max(x(r)),
        Empty(_) => 0.0,

        // for boolean expressions, the result represents selectivity
        Ref(a) => x(a),
        Constant(DataValue::Bool(false)) => 0.0,
        Constant(DataValue::Bool(true)) => 1.0,
        And([a, b]) => x(a) * x(b), // TODO: consider dependency
        Or([a, b]) => x(a) + x(b) - x(a) * x(b), // TODO: consider dependency
        Xor([a, b]) => x(a) + x(b) - 2.0 * x(a) * x(b),
        Not(a) => 1.0 - x(a),
        Gt(_) | Lt(_) | GtEq(_) | LtEq(_) | Eq(_) | NotEq(_) | Like(_) => 0.5,
        In([_, b]) => 1.0 - 1.0 / (list_len(b) as f32 + 1.0),

        _ => 1.0,
    }
}

const DEFAULT_ROW_COUNT: u32 = 1000;

/// Statistic from storage for row estimation.
#[derive(Debug, Clone, Default)]
pub struct Statistics {
    row_counts: HashMap<TableRefId, u32>,
    distinct_values: HashMap<ColumnRefId, u32>,
}

impl Statistics {
    pub fn add_row_count(&mut self, table_id: TableRefId, count: u32) {
        self.row_counts.insert(table_id, count);
    }

    pub fn get_row_count(&self, table_id: TableRefId) -> Option<u32> {
        self.row_counts.get(&table_id).copied()
    }

    pub fn add_distinct_values(&mut self, mut column_id: ColumnRefId, count: u32) {
        column_id.table_occurrence = 0;
        self.distinct_values.insert(column_id, count);
    }

    pub fn get_distinct_values(&self, mut column_id: ColumnRefId) -> Option<u32> {
        column_id.table_occurrence = 0;
        self.distinct_values.get(&column_id).copied()
    }
}
