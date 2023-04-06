-- extract common predicate
explain select * from t where (a = 1 and b = 2) or (a = 1 and c = 3)

/*
Filter                                                                                                       
├── cond: and { lhs: = { lhs: a, rhs: 1 }, rhs: or { lhs: = { lhs: c, rhs: 3 }, rhs: = { lhs: b, rhs: 2 } } }
├── cost: 4316                                                                                               
└── Scan { table: t, list: [ a, b, c ], cost: 3000 }
*/

