-- count(*) is special
explain select count(*) from t

/*
Projection { exprs: [ rowcount ], cost: 201.11, rows: 1 }
└── Agg { aggs: [ rowcount ], cost: 201, rows: 1 }
    └── Scan { table: t, list: [], filter: null, cost: 0, rows: 1000 }
*/

-- count(*) with projection
explain select count(*) + 1 from t

/*
Projection { exprs: [ + { lhs: 1, rhs: rowcount } ], cost: 201.31, rows: 1 }
└── Agg { aggs: [ rowcount ], cost: 201, rows: 1 }
    └── Scan { table: t, list: [], filter: null, cost: 0, rows: 1000 }
*/

