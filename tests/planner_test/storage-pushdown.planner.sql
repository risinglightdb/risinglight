-- use merge join for primary key joins
explain select * from t1 join t2 on a = c;

/*
Join { type: inner, on: = { lhs: c, rhs: a }, cost: 0, rows: 0 }
├── Scan { table: t1, list: [ a, b ], filter: null, cost: 0, rows: 0 }
└── Scan { table: t2, list: [ c, d ], filter: null, cost: 0, rows: 0 }
*/

-- use storage order by instead of sorting by primary key
explain select * from t1 order by a;

/*
Scan { table: t1, list: [ a, b ], filter: null, cost: 0, rows: 0 }
*/

-- use storage filter for primary key
explain select * from t1 where a = 10;

/*
Scan { table: t1, list: [ a, b ], filter: = { lhs: a, rhs: 10 }, cost: 10, rows: 5 }
*/

-- use storage filter for a combination of primary key and other keys
explain select * from t1 where a > 10 and b > 10;

/*
Filter { cond: and { lhs: > { lhs: b, rhs: 10 }, rhs: > { lhs: a, rhs: 10 } }, cost: 0, rows: 0 }
└── Scan { table: t1, list: [ a, b ], filter: null, cost: 0, rows: 0 }
*/

