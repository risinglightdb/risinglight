-- extract common predicate
explain select * from t where (a = 1 and b = 2) or (a = 1 and c = 3)

/*
Filter { cond: ((a = 1) and ((b = 2) or (c = 3))), cost: 4.955, rows: 0.375 }
└── Scan { table: t, list: [ a, b, c ], filter: true, cost: 3, rows: 1 }
*/

