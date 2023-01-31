-- count(*) is special
explain select count(*) from t

/*
Projection { exprs: [ rowcount ], cost: 302.3 }              
└── Aggregate { aggs: [ rowcount ], group_by: [], cost: 301 }
    └── Scan { table: t, list: [], cost: 0 }
*/

-- count(*) with projection
explain select count(*) + 1 from t

/*
Projection { exprs: [ + { lhs: 1, rhs: rowcount } ], cost: 302.5 }
└── Aggregate { aggs: [ rowcount ], group_by: [], cost: 301 }     
    └── Scan { table: t, list: [], cost: 0 }
*/

