-- match the index
explain select * from t order by a <-> '[0, 0, 1]'::VECTOR(3);

/*
IndexScan { table: t, columns: [ a, b ], filter: true, op: <->, key: a, vector: [0,0,1], cost: 0, rows: 1 }
*/

-- match the index
explain select * from t order by a <=> '[0, 0, 1]'::VECTOR(3);

/*
Order { by: [ VectorCosineDistance { lhs: a, rhs: [0,0,1] } ], cost: 18, rows: 3 }
└── Scan { table: t, list: [ a, b ], filter: true, cost: 6, rows: 3 }
*/

