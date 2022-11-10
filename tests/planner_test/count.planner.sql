-- count(*) is special
explain select count(*) from t

/*
Projection: [rowcount] (cost=302.3)
  Aggregate: [rowcount], groupby=[] (cost=301)
    Scan: t[] (cost=0)
*/

-- count(*) with projection
explain select count(*) + 1 from t

/*
Projection: [(1 + rowcount)] (cost=302.5)
  Aggregate: [rowcount], groupby=[] (cost=301)
    Scan: t[] (cost=0)
*/

