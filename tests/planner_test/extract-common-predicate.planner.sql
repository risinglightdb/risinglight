-- extract common predicate
explain select * from t where (a = 1 and b = 2) or (a = 1 and c = 3)

/*
Filter
├── cond: and { lhs: = { lhs: a, rhs: 1 }, rhs: or { lhs: = { lhs: b, rhs: 2 }, rhs: = { lhs: c, rhs: 3 } } }
├── cost: 4.955
├── rows: 0.375
└── Scan { table: t, list: [ a, b, c ], filter: true, cost: 3, rows: 1 }
*/

