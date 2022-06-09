-- count(*) is special
explain select count(*) from t

/*
PhysicalProjection:
    InputRef #0
  PhysicalSimpleAgg:
      count(InputRef #0) -> INT
    PhysicalTableScan:
        table #0,
        columns [],
        with_row_handler: true,
        is_sorted: false,
        expr: None
*/

-- count(*) with projection
explain select count(*) + 1 from t

/*
PhysicalProjection:
    (InputRef #0 + 1)
  PhysicalSimpleAgg:
      count(InputRef #0) -> INT
    PhysicalTableScan:
        table #0,
        columns [],
        with_row_handler: true,
        is_sorted: false,
        expr: None
*/

