-- count(*) is special
explain select count(*) from t

/*
Projection { exprs: [ rowcount ], cost: 1.13, rows: 1 }
└── Agg { aggs: [ rowcount ], cost: 1.11, rows: 1 }
    └── Scan { table: t, list: [], filter: null, cost: 0, rows: 1 }
*/

-- count(*) with projection
explain select count(*) + 1 from t

/*
Projection { exprs: [ + { lhs: rowcount, rhs: 1 } ], cost: 1.33, rows: 1 }
└── Agg { aggs: [ rowcount ], cost: 1.11, rows: 1 }
    └── Scan { table: t, list: [], filter: null, cost: 0, rows: 1 }
*/

