-- count(*) is special
explain select count(*) from t

/*

Projection { cost: 302.3, exprs: [ rowcount ] }              
└── Aggregate { aggs: [ rowcount ], cost: 301, group_by: [] }
    └── Scan { cost: 0, list: [], table: t }
*/

-- count(*) with projection
explain select count(*) + 1 from t

/*

Projection { cost: 302.5, exprs: [ + { lhs: 1, rhs: rowcount } ] }
└── Aggregate { aggs: [ rowcount ], cost: 301, group_by: [] }     
    └── Scan { cost: 0, list: [], table: t }
*/

