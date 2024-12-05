-- count(*) is special
explain select count(*) from t

/*
Projection
├── exprs:ref
│   └── count(*)_4
├── cost: 1.23
├── rows: 1
└── Agg { aggs: [ count(*)_4 ], cost: 1.21, rows: 1 }
    └── Scan { table: t, list: [], filter: true, cost: 0, rows: 1 }
*/

-- count(*) with projection
explain select count(*) + 1 from t

/*
Projection
├── exprs:+
│   ├── lhs:ref
│   │   └── count(*)_4
│   ├── rhs: 1

├── cost: 1.4300001
├── rows: 1
└── Agg { aggs: [ count(*)_4 ], cost: 1.21, rows: 1 }
    └── Scan { table: t, list: [], filter: true, cost: 0, rows: 1 }
*/

