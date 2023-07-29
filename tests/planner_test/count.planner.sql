-- count(*) is special
explain select count(*) from t

/*
Projection { exprs: [ rowcount ], cost: 201.11 }
└── Agg { aggs: [ rowcount ], cost: 201 }
    └── Scan { table: t, list: [], filter: null, cost: 0 }
*/

-- count(*) with projection
explain select count(*) + 1 from t

/*
Projection { exprs: [ + { lhs: 1, rhs: rowcount } ], cost: 201.31 }
└── Agg { aggs: [ rowcount ], cost: 201 }
    └── Scan { table: t, list: [], filter: null, cost: 0 }
*/

