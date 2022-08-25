-- eliminate cse
EXPLAIN SELECT count(x + 5), count(x + 5) FROM test;

/*
PhysicalProjection:
    InputRef #0
    InputRef #0
  PhysicalSimpleAgg:
      count(InputRef #0) -> INT
    PhysicalProjection:
        (InputRef #0 + 5)
      PhysicalTableScan:
          table #0,
          columns [0],
          with_row_handler: false,
          is_sorted: false,
          expr: None
*/

-- eliminate cse
EXPLAIN SELECT x + y + 5, x + y, x + y + 5 FROM test;

/*
PhysicalProjection:
    InputRef #0
    InputRef #1
    InputRef #0
  PhysicalProjection:
      (InputRef #0 + 5)
      InputRef #0
    PhysicalProjection:
        (InputRef #0 + InputRef #1)
      PhysicalTableScan:
          table #0,
          columns [0, 1],
          with_row_handler: false,
          is_sorted: false,
          expr: None
*/

-- eliminate cse
EXPLAIN SELECT x + 5 < y, x + 5 FROM test;

/*
PhysicalProjection:
    (InputRef #0 < InputRef #1)
    InputRef #0
  PhysicalProjection:
      (InputRef #0 + 5)
      InputRef #1
    PhysicalTableScan:
        table #0,
        columns [0, 1],
        with_row_handler: false,
        is_sorted: false,
        expr: None
*/

-- keep short circuit
EXPLAIN SELECT x + 5 < y AND x + y < 3, x + y FROM test;

/*
PhysicalProjection:
    (((InputRef #0 + 5) < InputRef #1) AND ((InputRef #0 + InputRef #1) < 3))
    (InputRef #0 + InputRef #1)
  PhysicalTableScan:
      table #0,
      columns [0, 1],
      with_row_handler: false,
      is_sorted: false,
      expr: None
*/

-- eliminate cse
EXPLAIN SELECT x + y < 3 AND x + 5 < y, x + y FROM test;

/*
PhysicalProjection:
    ((InputRef #0 < 3) AND ((InputRef #1 + 5) < InputRef #2))
    InputRef #0
  PhysicalProjection:
      (InputRef #0 + InputRef #1)
      InputRef #0
      InputRef #1
    PhysicalTableScan:
        table #0,
        columns [0, 1],
        with_row_handler: false,
        is_sorted: false,
        expr: None
*/

