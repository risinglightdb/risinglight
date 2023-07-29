-- count(*) is special
explain select count(*) from t

/*
Projection { exprs: [ rowcount ], cost: 3.11, rows: 1 }
└── Agg { aggs: [ rowcount ], cost: 2.1, rows: 1 }
    └── Scan { table: t, list: [], filter: null, cost: 0, rows: 1 }
*/

-- count(*) with projection
explain select count(*) + 1 from t

/*
Projection { exprs: [ + { lhs: 1, rhs: rowcount } ], cost: 5.1099997, rows: 1 }
└── Agg { aggs: [ rowcount ], cost: 2.1, rows: 1 }
    └── Scan { table: t, list: [], filter: null, cost: 0, rows: 1 }
*/

