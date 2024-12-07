-- count(*) is special
explain select count(*) from t

/*
Projection { exprs: [ #4 ], cost: 1.13, rows: 1 }
└── Agg { aggs: [ count(*) as #4 ], cost: 1.11, rows: 1 }
    └── Scan { table: t, list: [], filter: true, cost: 0, rows: 1 }
*/

-- count(*) with projection
explain select count(*) + 1 from t

/*
Projection { exprs: [ (#4 + 1) as #9 ], cost: 1.33, rows: 1 }
└── Agg { aggs: [ count(*) as #4 ], cost: 1.11, rows: 1 }
    └── Scan { table: t, list: [], filter: true, cost: 0, rows: 1 }
*/

