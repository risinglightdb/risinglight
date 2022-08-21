-- prepare
CREATE TABLE NATION  (
    N_NATIONKEY  INT NOT NULL,
    N_NAME       CHAR(25) NOT NULL,
    N_REGIONKEY  INT NOT NULL,
    N_COMMENT    VARCHAR(152)
);

CREATE TABLE REGION  (
    R_REGIONKEY  INT NOT NULL,
    R_NAME       CHAR(25) NOT NULL,
    R_COMMENT    VARCHAR(152)
);

CREATE TABLE PART  (
    P_PARTKEY     INT NOT NULL,
    P_NAME        VARCHAR(55) NOT NULL,
    P_MFGR        CHAR(25) NOT NULL,
    P_BRAND       CHAR(10) NOT NULL,
    P_TYPE        VARCHAR(25) NOT NULL,
    P_SIZE        INT NOT NULL,
    P_CONTAINER   CHAR(10) NOT NULL,
    P_RETAILPRICE DECIMAL(15,2) NOT NULL,
    P_COMMENT     VARCHAR(23) NOT NULL
);

CREATE TABLE SUPPLIER (
    S_SUPPKEY     INT NOT NULL,
    S_NAME        CHAR(25) NOT NULL,
    S_ADDRESS     VARCHAR(40) NOT NULL,
    S_NATIONKEY   INT NOT NULL,
    S_PHONE       CHAR(15) NOT NULL,
    S_ACCTBAL     DECIMAL(15,2) NOT NULL,
    S_COMMENT     VARCHAR(101) NOT NULL
);

CREATE TABLE PARTSUPP (
    PS_PARTKEY     INT NOT NULL,
    PS_SUPPKEY     INT NOT NULL,
    PS_AVAILQTY    INT NOT NULL,
    PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
    PS_COMMENT     VARCHAR(199) NOT NULL
);

CREATE TABLE CUSTOMER (
    C_CUSTKEY     INT NOT NULL,
    C_NAME        VARCHAR(25) NOT NULL,
    C_ADDRESS     VARCHAR(40) NOT NULL,
    C_NATIONKEY   INT NOT NULL,
    C_PHONE       CHAR(15) NOT NULL,
    C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
    C_MKTSEGMENT  CHAR(10) NOT NULL,
    C_COMMENT     VARCHAR(117) NOT NULL
);

CREATE TABLE ORDERS (
    O_ORDERKEY       INT NOT NULL,
    O_CUSTKEY        INT NOT NULL,
    O_ORDERSTATUS    CHAR(1) NOT NULL,
    O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
    O_ORDERDATE      DATE NOT NULL,
    O_ORDERPRIORITY  CHAR(15) NOT NULL,  
    O_CLERK          CHAR(15) NOT NULL, 
    O_SHIPPRIORITY   INT NOT NULL,
    O_COMMENT        VARCHAR(79) NOT NULL
);

CREATE TABLE LINEITEM (
    L_ORDERKEY      INT NOT NULL,
    L_PARTKEY       INT NOT NULL,
    L_SUPPKEY       INT NOT NULL,
    L_LINENUMBER    INT NOT NULL,
    L_QUANTITY      DECIMAL(15,2) NOT NULL,
    L_EXTENDEDPRICE DECIMAL(15,2) NOT NULL,
    L_DISCOUNT      DECIMAL(15,2) NOT NULL,
    L_TAX           DECIMAL(15,2) NOT NULL,
    L_RETURNFLAG    CHAR(1) NOT NULL,
    L_LINESTATUS    CHAR(1) NOT NULL,
    L_SHIPDATE      DATE NOT NULL,
    L_COMMITDATE    DATE NOT NULL,
    L_RECEIPTDATE   DATE NOT NULL,
    L_SHIPINSTRUCT  CHAR(25) NOT NULL,
    L_SHIPMODE      CHAR(10) NOT NULL,
    L_COMMENT       VARCHAR(44) NOT NULL
);

/*

*/

-- tpch-q1: TPC-H Q1
explain select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '71' day
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;

/*
PhysicalOrder:
    [InputRef #0 (asc), InputRef #1 (asc)]
  PhysicalProjection:
      InputRef #0
      InputRef #1
      InputRef #2 (alias to sum_qty)
      InputRef #3 (alias to sum_base_price)
      InputRef #4 (alias to sum_disc_price)
      InputRef #5 (alias to sum_charge)
      (InputRef #2 / InputRef #6) (alias to avg_qty)
      (InputRef #3 / InputRef #7) (alias to avg_price)
      (InputRef #8 / InputRef #9) (alias to avg_disc)
      InputRef #10 (alias to count_order)
    PhysicalHashAgg:
        InputRef #1
        InputRef #2
        sum(InputRef #3) -> NUMERIC(15,2)
        sum(InputRef #4) -> NUMERIC(15,2)
        sum((InputRef #4 * (1 - InputRef #5))) -> NUMERIC(15,2) (null)
        sum(((InputRef #4 * (1 - InputRef #5)) * (1 + InputRef #6))) -> NUMERIC(15,2) (null)
        count(InputRef #3) -> INT
        count(InputRef #4) -> INT
        sum(InputRef #5) -> NUMERIC(15,2)
        count(InputRef #5) -> INT
        count(InputRef #0) -> INT
      PhysicalTableScan:
          table #7,
          columns [10, 8, 9, 4, 5, 6, 7],
          with_row_handler: false,
          is_sorted: false,
          expr: LtEq(InputRef #0, Date(Date(10490)) (const))
*/

-- tpch-q3: TPC-H Q3
explain select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    customer,
    orders,
    lineitem
where
    c_mktsegment = 'BUILDING'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-15'
    and l_shipdate > date '1995-03-15'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10;

/*
PhysicalTopN: offset: 0, limit: 10, order by [InputRef #1 (desc), InputRef #2 (asc)]
  PhysicalProjection:
      InputRef #0
      InputRef #3 (alias to revenue)
      InputRef #1
      InputRef #2
    PhysicalHashAgg:
        InputRef #2
        InputRef #0
        InputRef #1
        sum((InputRef #3 * (1 - InputRef #4))) -> NUMERIC(15,2) (null)
      PhysicalProjection:
          InputRef #1
          InputRef #2
          InputRef #3
          InputRef #4
          InputRef #5
        PhysicalHashJoin:
            op Inner,
            predicate: Eq(InputRef #0, InputRef #3)
          PhysicalProjection:
              InputRef #2
              InputRef #3
              InputRef #4
            PhysicalHashJoin:
                op Inner,
                predicate: Eq(InputRef #0, InputRef #1)
              PhysicalProjection:
                  InputRef #0
                PhysicalTableScan:
                    table #5,
                    columns [0, 6],
                    with_row_handler: false,
                    is_sorted: false,
                    expr: Eq(InputRef #1, String("BUILDING") (const))
              PhysicalTableScan:
                  table #6,
                  columns [1, 0, 4, 7],
                  with_row_handler: false,
                  is_sorted: false,
                  expr: Lt(InputRef #2, Date(Date(9204)) (const))
          PhysicalProjection:
              InputRef #0
              InputRef #1
              InputRef #2
            PhysicalTableScan:
                table #7,
                columns [0, 5, 6, 10],
                with_row_handler: false,
                is_sorted: false,
                expr: Gt(InputRef #3, Date(Date(9204)) (const))
*/

-- tpch-q5: TPC-H Q5
explain select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'AFRICA'
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '1' year
group by
    n_name
order by
    revenue desc;

/*
PhysicalOrder:
    [InputRef #1 (desc)]
  PhysicalProjection:
      InputRef #0
      InputRef #1 (alias to revenue)
    PhysicalHashAgg:
        InputRef #2
        sum((InputRef #0 * (1 - InputRef #1))) -> NUMERIC(15,2) (null)
      PhysicalProjection:
          InputRef #0
          InputRef #1
          InputRef #3
        PhysicalHashJoin:
            op Inner,
            predicate: Eq(InputRef #2, InputRef #4)
          PhysicalProjection:
              InputRef #0
              InputRef #1
              InputRef #4
              InputRef #5
            PhysicalHashJoin:
                op Inner,
                predicate: Eq(InputRef #2, InputRef #3)
              PhysicalProjection:
                  InputRef #2
                  InputRef #3
                  InputRef #5
                PhysicalHashJoin:
                    op Inner,
                    predicate: And(Eq(InputRef #1, InputRef #4), Eq(InputRef #0, InputRef #5))
                  PhysicalProjection:
                      InputRef #0
                      InputRef #3
                      InputRef #4
                      InputRef #5
                    PhysicalHashJoin:
                        op Inner,
                        predicate: Eq(InputRef #1, InputRef #2)
                      PhysicalProjection:
                          InputRef #1
                          InputRef #3
                        PhysicalHashJoin:
                            op Inner,
                            predicate: Eq(InputRef #0, InputRef #2)
                          PhysicalTableScan:
                              table #5,
                              columns [0, 3],
                              with_row_handler: false,
                              is_sorted: false,
                              expr: None
                          PhysicalProjection:
                              InputRef #0
                              InputRef #1
                            PhysicalTableScan:
                                table #6,
                                columns [1, 0, 4],
                                with_row_handler: false,
                                is_sorted: false,
                                expr: And(GtEq(InputRef #2, Date(Date(8766)) (const)), Lt(InputRef #2, Date(Date(9131)) (const)))
                      PhysicalTableScan:
                          table #7,
                          columns [0, 2, 5, 6],
                          with_row_handler: false,
                          is_sorted: false,
                          expr: None
                  PhysicalTableScan:
                      table #3,
                      columns [0, 3],
                      with_row_handler: false,
                      is_sorted: false,
                      expr: None
              PhysicalTableScan:
                  table #0,
                  columns [0, 2, 1],
                  with_row_handler: false,
                  is_sorted: false,
                  expr: None
          PhysicalProjection:
              InputRef #0
            PhysicalTableScan:
                table #1,
                columns [0, 1],
                with_row_handler: false,
                is_sorted: false,
                expr: Eq(InputRef #1, String("AFRICA") (const))
*/

-- tpch-q6
explain select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between 0.08 - 0.01 and 0.08 + 0.01
    and l_quantity < 24;

/*
PhysicalProjection:
    InputRef #0 (alias to revenue)
  PhysicalSimpleAgg:
      sum((InputRef #1 * InputRef #0)) -> NUMERIC(15,2) (null)
    PhysicalProjection:
        InputRef #0
        InputRef #1
      PhysicalTableScan:
          table #7,
          columns [6, 5, 10, 4],
          with_row_handler: false,
          is_sorted: false,
          expr: And(And(And(GtEq(InputRef #2, Date(Date(8766)) (const)), Lt(InputRef #2, Date(Date(9131)) (const))), And(GtEq(InputRef #0, Decimal(0.07) (const)), LtEq(InputRef #0, Decimal(0.09) (const)))), Lt(InputRef #3, Decimal(24) (const)))
*/

-- tpch-q10: TPC-H Q10
explain select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate >= date '1993-10-01'
    and o_orderdate < date '1993-10-01' + interval '3' month
    and l_returnflag = 'R'
    and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
limit 20;

/*
PhysicalTopN: offset: 0, limit: 20, order by [InputRef #2 (desc)]
  PhysicalProjection:
      InputRef #0
      InputRef #1
      InputRef #7 (alias to revenue)
      InputRef #2
      InputRef #4
      InputRef #5
      InputRef #3
      InputRef #6
    PhysicalHashAgg:
        InputRef #0
        InputRef #1
        InputRef #2
        InputRef #4
        InputRef #8
        InputRef #3
        InputRef #5
        sum((InputRef #6 * (1 - InputRef #7))) -> NUMERIC(15,2) (null)
      PhysicalProjection:
          InputRef #0
          InputRef #2
          InputRef #3
          InputRef #4
          InputRef #5
          InputRef #6
          InputRef #7
          InputRef #8
          InputRef #10
        PhysicalHashJoin:
            op Inner,
            predicate: Eq(InputRef #1, InputRef #9)
          PhysicalProjection:
              InputRef #0
              InputRef #1
              InputRef #2
              InputRef #3
              InputRef #4
              InputRef #5
              InputRef #6
              InputRef #9
              InputRef #10
            PhysicalHashJoin:
                op Inner,
                predicate: Eq(InputRef #7, InputRef #8)
              PhysicalProjection:
                  InputRef #0
                  InputRef #1
                  InputRef #2
                  InputRef #3
                  InputRef #4
                  InputRef #5
                  InputRef #6
                  InputRef #8
                PhysicalHashJoin:
                    op Inner,
                    predicate: Eq(InputRef #0, InputRef #7)
                  PhysicalTableScan:
                      table #5,
                      columns [0, 3, 1, 5, 2, 4, 7],
                      with_row_handler: false,
                      is_sorted: false,
                      expr: None
                  PhysicalProjection:
                      InputRef #0
                      InputRef #1
                    PhysicalTableScan:
                        table #6,
                        columns [1, 0, 4],
                        with_row_handler: false,
                        is_sorted: false,
                        expr: And(GtEq(InputRef #2, Date(Date(8674)) (const)), Lt(InputRef #2, Date(Date(8766)) (const)))
              PhysicalProjection:
                  InputRef #0
                  InputRef #1
                  InputRef #2
                PhysicalTableScan:
                    table #7,
                    columns [0, 5, 6, 8],
                    with_row_handler: false,
                    is_sorted: false,
                    expr: Eq(InputRef #3, String("R") (const))
          PhysicalTableScan:
              table #0,
              columns [0, 1],
              with_row_handler: false,
              is_sorted: false,
              expr: None
*/

