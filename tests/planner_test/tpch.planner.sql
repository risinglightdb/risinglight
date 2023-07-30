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

SET mock_rowcount_customer =  150000;
SET mock_rowcount_lineitem = 6001215;
SET mock_rowcount_nation   =      25;
SET mock_rowcount_orders   = 1500000;
SET mock_rowcount_part     =  200000;
SET mock_rowcount_partsupp =  800000;
SET mock_rowcount_region   =       5;
SET mock_rowcount_supplier =   10000;

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
Projection
├── exprs:
│   ┌── l_returnflag
│   ├── l_linestatus
│   ├── sum
│   │   └── l_quantity
│   ├── sum
│   │   └── l_extendedprice
│   ├── sum
│   │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
│   ├── sum
│   │   └── *
│   │       ├── lhs: + { lhs: l_extendedprice, rhs: * { lhs: l_tax, rhs: l_extendedprice } }
│   │       └── rhs: - { lhs: 1, rhs: l_discount }
│   ├── /
│   │   ├── lhs:sum
│   │   │   └── l_quantity
│   │   ├── rhs:count
│   │   │   └── l_quantity

│   ├── /
│   │   ├── lhs:sum
│   │   │   └── l_extendedprice
│   │   ├── rhs:count
│   │   │   └── l_extendedprice

│   ├── /
│   │   ├── lhs:sum
│   │   │   └── l_discount
│   │   ├── rhs:count
│   │   │   └── l_discount

│   └── rowcount
├── cost: 70266880
├── rows: 100
└── Order { by: [ l_returnflag, l_linestatus ], cost: 70266840, rows: 100 }
    └── HashAgg
        ├── aggs:
        │   ┌── sum
        │   │   └── l_quantity
        │   ├── sum
        │   │   └── l_extendedprice
        │   ├── sum
        │   │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        │   ├── sum
        │   │   └── *
        │   │       ├── lhs: + { lhs: l_extendedprice, rhs: * { lhs: l_tax, rhs: l_extendedprice } }
        │   │       └── rhs: - { lhs: 1, rhs: l_discount }
        │   ├── count
        │   │   └── l_quantity
        │   ├── count
        │   │   └── l_extendedprice
        │   ├── sum
        │   │   └── l_discount
        │   ├── count
        │   │   └── l_discount
        │   └── rowcount
        ├── group_by: [ l_returnflag, l_linestatus ]
        ├── cost: 70265070
        ├── rows: 100
        └── Projection
            ├── exprs: [ l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus ]
            ├── cost: 64483056
            ├── rows: 3000607.5
            └── Filter { cond: >= { lhs: 1998-09-21, rhs: l_shipdate }, cost: 64273012, rows: 3000607.5 }
                └── Scan
                    ├── table: lineitem
                    ├── list: [ l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate ]
                    ├── filter: null
                    ├── cost: 42008504
                    └── rows: 6001215
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
Projection
├── exprs:
│   ┌── l_orderkey
│   ├── sum
│   │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
│   ├── o_orderdate
│   └── o_shippriority
├── cost: 71731704
├── rows: 10
└── TopN
    ├── limit: 10
    ├── offset: 0
    ├── order_by:
    │   ┌── desc
    │   │   └── sum
    │   │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    │   └── o_orderdate
    ├── cost: 71731704
    ├── rows: 10
    └── HashAgg
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ l_orderkey, o_orderdate, o_shippriority ]
        ├── cost: 71728210
        ├── rows: 1000
        └── Projection
            ├── exprs: [ o_orderdate, o_shippriority, l_orderkey, l_extendedprice, l_discount ]
            ├── cost: 70014850
            ├── rows: 3000607.5
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                ├── cost: 69834810
                ├── rows: 3000607.5
                ├── Projection { exprs: [ o_orderkey, o_orderdate, o_shippriority ], cost: 13711606, rows: 750000 }
                │   └── HashJoin
                │       ├── type: inner
                │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                │       ├── cost: 13681606
                │       ├── rows: 750000
                │       ├── Projection { exprs: [ c_custkey ], cost: 483000, rows: 75000 }
                │       │   └── Filter { cond: = { lhs: c_mktsegment, rhs: 'BUILDING' }, cost: 481500, rows: 75000 }
                │       │       └── Scan
                │       │           ├── table: customer
                │       │           ├── list: [ c_custkey, c_mktsegment ]
                │       │           ├── filter: null
                │       │           ├── cost: 300000
                │       │           └── rows: 150000
                │       └── Filter { cond: > { lhs: 1995-03-15, rhs: o_orderdate }, cost: 9315000, rows: 750000 }
                │           └── Scan
                │               ├── table: orders
                │               ├── list: [ o_orderkey, o_custkey, o_orderdate, o_shippriority ]
                │               ├── filter: null
                │               ├── cost: 6000000
                │               └── rows: 1500000
                └── Projection { exprs: [ l_orderkey, l_extendedprice, l_discount ], cost: 37387570, rows: 3000607.5 }
                    └── Filter { cond: > { lhs: l_shipdate, rhs: 1995-03-15 }, cost: 37267544, rows: 3000607.5 }
                        └── Scan
                            ├── table: lineitem
                            ├── list: [ l_orderkey, l_extendedprice, l_discount, l_shipdate ]
                            ├── filter: null
                            ├── cost: 24004860
                            └── rows: 6001215
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
Projection
├── exprs:
│   ┌── n_name
│   └── sum
│       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
├── cost: 115827250
├── rows: 10
└── Order
    ├── by:desc
    │   └── sum
    │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    ├── cost: 115827250
    ├── rows: 10
    └── HashAgg
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ n_name ]
        ├── cost: 115827190
        ├── rows: 10
        └── Projection { exprs: [ n_name, l_extendedprice, l_discount ], cost: 112919016, rows: 6001215 }
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ s_suppkey, s_nationkey ], rhs: [ l_suppkey, c_nationkey ] }
                ├── cost: 112678970
                ├── rows: 6001215
                ├── Projection { exprs: [ n_name, s_suppkey, s_nationkey ], cost: 61063.566, rows: 10000 }
                │   └── HashJoin
                │       ├── type: inner
                │       ├── on: = { lhs: [ n_nationkey ], rhs: [ s_nationkey ] }
                │       ├── cost: 60663.566
                │       ├── rows: 10000
                │       ├── Projection { exprs: [ n_nationkey, n_name ], cost: 192.34702, rows: 25 }
                │       │   └── HashJoin
                │       │       ├── type: inner
                │       │       ├── on: = { lhs: [ r_regionkey ], rhs: [ n_regionkey ] }
                │       │       ├── cost: 191.59702
                │       │       ├── rows: 25
                │       │       ├── Projection { exprs: [ r_regionkey ], cost: 16.099998, rows: 2.5 }
                │       │       │   └── Filter { cond: = { lhs: r_name, rhs: 'AFRICA' }, cost: 16.05, rows: 2.5 }
                │       │       │       └── Scan
                │       │       │           ├── table: region
                │       │       │           ├── list: [ r_regionkey, r_name ]
                │       │       │           ├── filter: null
                │       │       │           ├── cost: 10
                │       │       │           └── rows: 5
                │       │       └── Scan
                │       │           ├── table: nation
                │       │           ├── list: [ n_nationkey, n_name, n_regionkey ]
                │       │           ├── filter: null
                │       │           ├── cost: 75
                │       │           └── rows: 25
                │       └── Scan
                │           ├── table: supplier
                │           ├── list: [ s_suppkey, s_nationkey ]
                │           ├── filter: null
                │           ├── cost: 20000
                │           └── rows: 10000
                └── Projection
                    ├── exprs: [ c_nationkey, l_suppkey, l_extendedprice, l_discount ]
                    ├── cost: 69810640
                    ├── rows: 6001215
                    └── HashJoin
                        ├── type: inner
                        ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                        ├── cost: 69510580
                        ├── rows: 6001215
                        ├── Projection { exprs: [ c_nationkey, o_orderkey ], cost: 8317772, rows: 375000 }
                        │   └── HashJoin
                        │       ├── type: inner
                        │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                        │       ├── cost: 8306522
                        │       ├── rows: 375000
                        │       ├── Scan
                        │       │   ├── table: customer
                        │       │   ├── list: [ c_custkey, c_nationkey ]
                        │       │   ├── filter: null
                        │       │   ├── cost: 300000
                        │       │   └── rows: 150000
                        │       └── Projection { exprs: [ o_orderkey, o_custkey ], cost: 6416250, rows: 375000 }
                        │           └── Filter
                        │               ├── cond:and
                        │               │   ├── lhs: >= { lhs: o_orderdate, rhs: 1994-01-01 }
                        │               │   └── rhs: > { lhs: 1995-01-01, rhs: o_orderdate }
                        │               ├── cost: 6405000
                        │               ├── rows: 375000
                        │               └── Scan
                        │                   ├── table: orders
                        │                   ├── list: [ o_orderkey, o_custkey, o_orderdate ]
                        │                   ├── filter: null
                        │                   ├── cost: 4500000
                        │                   └── rows: 1500000
                        └── Scan
                            ├── table: lineitem
                            ├── list: [ l_orderkey, l_suppkey, l_extendedprice, l_discount ]
                            ├── filter: null
                            ├── cost: 24004860
                            └── rows: 6001215
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
Projection
├── exprs:sum
│   └── * { lhs: l_discount, rhs: l_extendedprice }
├── cost: 33505534
├── rows: 1
└── Agg
    ├── aggs:sum
    │   └── * { lhs: l_discount, rhs: l_extendedprice }
    ├── cost: 33505534
    ├── rows: 1
    └── Projection { exprs: [ l_extendedprice, l_discount ], cost: 33462400, rows: 187537.97 }
        └── Filter
            ├── cond:and
            │   ├── lhs: > { lhs: 24, rhs: l_quantity }
            │   └── rhs:and
            │       ├── lhs: and { lhs: >= { lhs: 0.09, rhs: l_discount }, rhs: >= { lhs: l_discount, rhs: 0.07 } }
            │       └── rhs:and
            │           ├── lhs: > { lhs: 1995-01-01, rhs: l_shipdate }
            │           └── rhs: >= { lhs: l_shipdate, rhs: 1994-01-01 }
            ├── cost: 33456774
            ├── rows: 187537.97
            └── Scan
                ├── table: lineitem
                ├── list: [ l_quantity, l_extendedprice, l_discount, l_shipdate ]
                ├── filter: null
                ├── cost: 24004860
                └── rows: 6001215
*/

-- tpch-q7
explain select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume
        from
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        where
            s_suppkey = l_suppkey
            and o_orderkey = l_orderkey
            and c_custkey = o_custkey
            and s_nationkey = n1.n_nationkey
            and c_nationkey = n2.n_nationkey
            and (
                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
            )
            and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;

/*
Projection
├── exprs:
│   ┌── n_name
│   ├── n_name(1)
│   ├── Extract { from: l_shipdate, field: YEAR }
│   └── sum
│       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
├── cost: 78478200
├── rows: 1000
└── Order { by: [ n_name, n_name(1), Extract { from: l_shipdate, field: YEAR } ], cost: 78478150, rows: 1000 }
    └── HashAgg
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ n_name, n_name(1), Extract { from: l_shipdate, field: YEAR } ]
        ├── cost: 78464184
        ├── rows: 1000
        └── Projection
            ├── exprs:
            │   ┌── n_name
            │   ├── n_name(1)
            │   ├── Extract { from: l_shipdate, field: YEAR }
            │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
            ├── cost: 78289736
            ├── rows: 656382.9
            └── Filter
                ├── cond:or
                │   ├── lhs: and { lhs: = { lhs: n_name, rhs: 'GERMANY' }, rhs: = { lhs: n_name(1), rhs: 'FRANCE' } }
                │   └── rhs: and { lhs: = { lhs: n_name, rhs: 'FRANCE' }, rhs: = { lhs: n_name(1), rhs: 'GERMANY' } }
                ├── cost: 77922160
                ├── rows: 656382.9
                └── Projection
                    ├── exprs: [ n_name, n_name(1), l_extendedprice, l_discount, l_shipdate ]
                    ├── cost: 72929896
                    ├── rows: 1500303.8
                    └── HashJoin
                        ├── type: inner
                        ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                        ├── cost: 72839880
                        ├── rows: 1500303.8
                        ├── Projection { exprs: [ n_name(1), o_orderkey ], cost: 10240313, rows: 1500000 }
                        │   └── HashJoin
                        │       ├── type: inner
                        │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                        │       ├── cost: 10195313
                        │       ├── rows: 1500000
                        │       ├── Projection { exprs: [ n_name(1), c_custkey ], cost: 911601.8, rows: 150000 }
                        │       │   └── HashJoin
                        │       │       ├── type: inner
                        │       │       ├── on: = { lhs: [ n_nationkey(1) ], rhs: [ c_nationkey ] }
                        │       │       ├── cost: 907101.8
                        │       │       ├── rows: 150000
                        │       │       ├── Scan
                        │       │       │   ├── table: nation
                        │       │       │   ├── list: [ n_nationkey(1), n_name(1) ]
                        │       │       │   ├── filter: null
                        │       │       │   ├── cost: 50
                        │       │       │   └── rows: 25
                        │       │       └── Scan
                        │       │           ├── table: customer
                        │       │           ├── list: [ c_custkey, c_nationkey ]
                        │       │           ├── filter: null
                        │       │           ├── cost: 300000
                        │       │           └── rows: 150000
                        │       └── Scan
                        │           ├── table: orders
                        │           ├── list: [ o_orderkey, o_custkey ]
                        │           ├── filter: null
                        │           ├── cost: 3000000
                        │           └── rows: 1500000
                        └── Projection
                            ├── exprs: [ n_name, l_orderkey, l_extendedprice, l_discount, l_shipdate ]
                            ├── cost: 51481884
                            ├── rows: 1500303.8
                            └── HashJoin
                                ├── type: inner
                                ├── on: = { lhs: [ s_suppkey ], rhs: [ l_suppkey ] }
                                ├── cost: 51391864
                                ├── rows: 1500303.8
                                ├── Projection { exprs: [ n_name, s_suppkey ], cost: 60821.22, rows: 10000 }
                                │   └── HashJoin
                                │       ├── type: inner
                                │       ├── on: = { lhs: [ n_nationkey ], rhs: [ s_nationkey ] }
                                │       ├── cost: 60521.22
                                │       ├── rows: 10000
                                │       ├── Scan
                                │       │   ├── table: nation
                                │       │   ├── list: [ n_nationkey, n_name ]
                                │       │   ├── filter: null
                                │       │   ├── cost: 50
                                │       │   └── rows: 25
                                │       └── Scan
                                │           ├── table: supplier
                                │           ├── list: [ s_suppkey, s_nationkey ]
                                │           ├── filter: null
                                │           ├── cost: 20000
                                │           └── rows: 10000
                                └── Filter
                                    ├── cond:and
                                    │   ├── lhs: >= { lhs: l_shipdate, rhs: 1995-01-01 }
                                    │   └── rhs: >= { lhs: 1996-12-31, rhs: l_shipdate }
                                    ├── cost: 40628228
                                    ├── rows: 1500303.8
                                    └── Scan
                                        ├── table: lineitem
                                        ├── list: [ l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate ]
                                        ├── filter: null
                                        ├── cost: 30006076
                                        └── rows: 6001215
*/

-- tpch-q8
explain select
    o_year,
    sum(case
        when nation = 'BRAZIL' then volume
        else 0
    end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = 'AMERICA'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year;

/*
Projection
├── exprs:
│   ┌── Extract { from: o_orderdate, field: YEAR }
│   └── /
│       ├── lhs:sum
│       │   └── If
│       │       ├── cond: = { lhs: n_name(1), rhs: 'BRAZIL' }
│       │       ├── then: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
│       │       ├── else:Cast { type: 0 }
│       │       │   └── DECIMAL(30,4)

│       ├── rhs:sum
│       │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }

├── cost: 215401920
├── rows: 10
└── Order { by: [ Extract { from: o_orderdate, field: YEAR } ], cost: 215401920, rows: 10 }
    └── HashAgg
        ├── aggs:
        │   ┌── sum
        │   │   └── If
        │   │       ├── cond: = { lhs: n_name(1), rhs: 'BRAZIL' }
        │   │       ├── then: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        │   │       ├── else:Cast { type: 0 }
        │   │       │   └── DECIMAL(30,4)

        │   └── sum
        │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ Extract { from: o_orderdate, field: YEAR } ]
        ├── cost: 215401860
        ├── rows: 10
        └── Projection
            ├── exprs:
            │   ┌── Extract { from: o_orderdate, field: YEAR }
            │   ├── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
            │   └── n_name(1)
            ├── cost: 210033170
            ├── rows: 6001215
            └── Projection
                ├── exprs: [ n_nationkey(1), n_name(1), s_nationkey, o_orderdate, l_extendedprice, l_discount ]
                ├── cost: 206732500
                ├── rows: 6001215
                └── HashJoin
                    ├── type: inner
                    ├── on: = { lhs: [ r_regionkey, n_nationkey(1) ], rhs: [ n_regionkey, s_nationkey ] }
                    ├── cost: 206312420
                    ├── rows: 6001215
                    ├── Join { type: inner, cost: 259.85, rows: 62.5 }
                    │   ├── Scan
                    │   │   ├── table: nation
                    │   │   ├── list: [ n_nationkey(1), n_name(1) ]
                    │   │   ├── filter: null
                    │   │   ├── cost: 50
                    │   │   └── rows: 25
                    │   └── Projection { exprs: [ r_regionkey ], cost: 16.099998, rows: 2.5 }
                    │       └── Filter { cond: = { lhs: r_name, rhs: 'AMERICA' }, cost: 16.05, rows: 2.5 }
                    │           └── Scan
                    │               ├── table: region
                    │               ├── list: [ r_regionkey, r_name ]
                    │               ├── filter: null
                    │               ├── cost: 10
                    │               └── rows: 5
                    └── Projection
                        ├── exprs: [ n_regionkey, s_nationkey, o_orderdate, l_extendedprice, l_discount ]
                        ├── cost: 157943040
                        ├── rows: 6001215
                        └── HashJoin
                            ├── type: inner
                            ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                            ├── cost: 157582960
                            ├── rows: 6001215
                            ├── Projection
                            │   ├── exprs: [ n_regionkey, o_orderkey, o_orderdate ]
                            │   ├── cost: 9296874
                            │   ├── rows: 375000
                            │   └── HashJoin
                            │       ├── type: inner
                            │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                            │       ├── cost: 9281874
                            │       ├── rows: 375000
                            │       ├── Projection { exprs: [ n_regionkey, c_custkey ], cost: 911601.8, rows: 150000 }
                            │       │   └── HashJoin
                            │       │       ├── type: inner
                            │       │       ├── on: = { lhs: [ n_nationkey ], rhs: [ c_nationkey ] }
                            │       │       ├── cost: 907101.8
                            │       │       ├── rows: 150000
                            │       │       ├── Scan
                            │       │       │   ├── table: nation
                            │       │       │   ├── list: [ n_nationkey, n_regionkey ]
                            │       │       │   ├── filter: null
                            │       │       │   ├── cost: 50
                            │       │       │   └── rows: 25
                            │       │       └── Scan
                            │       │           ├── table: customer
                            │       │           ├── list: [ c_custkey, c_nationkey ]
                            │       │           ├── filter: null
                            │       │           ├── cost: 300000
                            │       │           └── rows: 150000
                            │       └── Filter
                            │           ├── cond:and
                            │           │   ├── lhs: >= { lhs: 1996-12-31, rhs: o_orderdate }
                            │           │   └── rhs: >= { lhs: o_orderdate, rhs: 1995-01-01 }
                            │           ├── cost: 6405000
                            │           ├── rows: 375000
                            │           └── Scan
                            │               ├── table: orders
                            │               ├── list: [ o_orderkey, o_custkey, o_orderdate ]
                            │               ├── filter: null
                            │               ├── cost: 4500000
                            │               └── rows: 1500000
                            └── Projection
                                ├── exprs: [ s_nationkey, l_orderkey, l_extendedprice, l_discount ]
                                ├── cost: 105096930
                                ├── rows: 6001215
                                └── HashJoin
                                    ├── type: inner
                                    ├── on: = { lhs: [ s_suppkey ], rhs: [ l_suppkey ] }
                                    ├── cost: 104796860
                                    ├── rows: 6001215
                                    ├── Scan
                                    │   ├── table: supplier
                                    │   ├── list: [ s_suppkey, s_nationkey ]
                                    │   ├── filter: null
                                    │   ├── cost: 20000
                                    │   └── rows: 10000
                                    └── Projection
                                        ├── exprs: [ l_orderkey, l_suppkey, l_extendedprice, l_discount ]
                                        ├── cost: 67970820
                                        ├── rows: 6001215
                                        └── HashJoin
                                            ├── type: inner
                                            ├── on: = { lhs: [ p_partkey ], rhs: [ l_partkey ] }
                                            ├── cost: 67670750
                                            ├── rows: 6001215
                                            ├── Projection { exprs: [ p_partkey ], cost: 644000, rows: 100000 }
                                            │   └── Filter
                                            │       ├── cond: = { lhs: p_type, rhs: 'ECONOMY ANODIZED STEEL' }
                                            │       ├── cost: 642000
                                            │       ├── rows: 100000
                                            │       └── Scan
                                            │           ├── table: part
                                            │           ├── list: [ p_partkey, p_type ]
                                            │           ├── filter: null
                                            │           ├── cost: 400000
                                            │           └── rows: 200000
                                            └── Scan
                                                ├── table: lineitem
                                                ├── list:
                                                │   ┌── l_orderkey
                                                │   ├── l_partkey
                                                │   ├── l_suppkey
                                                │   ├── l_extendedprice
                                                │   └── l_discount
                                                ├── filter: null
                                                ├── cost: 30006076
                                                └── rows: 6001215
*/

-- tpch-q9
explain select
    nation,
    o_year,
    sum(amount) as sum_profit
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        where
            s_suppkey = l_suppkey
            and ps_suppkey = l_suppkey
            and ps_partkey = l_partkey
            and p_partkey = l_partkey
            and o_orderkey = l_orderkey
            and s_nationkey = n_nationkey
            and p_name like '%green%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;

/*
Projection
├── exprs:
│   ┌── n_name
│   ├── Extract { from: o_orderdate, field: YEAR }
│   └── sum
│       └── -
│           ├── lhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
│           └── rhs: * { lhs: ps_supplycost, rhs: l_quantity }
├── cost: 252359860
├── rows: 100
└── Order
    ├── by:
    │   ┌── n_name
    │   └── desc
    │       └── Extract { from: o_orderdate, field: YEAR }
    ├── cost: 252359860
    ├── rows: 100
    └── HashAgg
        ├── aggs:sum
        │   └── -
        │       ├── lhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        │       └── rhs: * { lhs: ps_supplycost, rhs: l_quantity }
        ├── group_by: [ n_name, Extract { from: o_orderdate, field: YEAR } ]
        ├── cost: 252358900
        ├── rows: 100
        └── Projection
            ├── exprs:
            │   ┌── n_name
            │   ├── Extract { from: o_orderdate, field: YEAR }
            │   └── -
            │       ├── lhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
            │       └── rhs: * { lhs: ps_supplycost, rhs: l_quantity }
            ├── cost: 251058850
            ├── rows: 6001215
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                ├── cost: 246437920
                ├── rows: 6001215
                ├── Scan
                │   ├── table: orders
                │   ├── list: [ o_orderkey, o_orderdate ]
                │   ├── filter: null
                │   ├── cost: 3000000
                │   └── rows: 1500000
                └── Projection
                    ├── exprs: [ n_name, ps_supplycost, l_orderkey, l_quantity, l_extendedprice, l_discount ]
                    ├── cost: 193889220
                    ├── rows: 6001215
                    └── HashJoin
                        ├── type: inner
                        ├── on: = { lhs: [ ps_suppkey, ps_partkey ], rhs: [ l_suppkey, l_partkey ] }
                        ├── cost: 193469140
                        ├── rows: 6001215
                        ├── Scan
                        │   ├── table: partsupp
                        │   ├── list: [ ps_partkey, ps_suppkey, ps_supplycost ]
                        │   ├── filter: null
                        │   ├── cost: 2400000
                        │   └── rows: 800000
                        └── Projection
                            ├── exprs:
                            │   ┌── n_name
                            │   ├── l_orderkey
                            │   ├── l_partkey
                            │   ├── l_suppkey
                            │   ├── l_quantity
                            │   ├── l_extendedprice
                            │   └── l_discount
                            ├── cost: 129723300
                            ├── rows: 6001215
                            └── HashJoin
                                ├── type: inner
                                ├── on: = { lhs: [ s_suppkey ], rhs: [ l_suppkey ] }
                                ├── cost: 129243200
                                ├── rows: 6001215
                                ├── Projection { exprs: [ n_name, s_suppkey ], cost: 60821.22, rows: 10000 }
                                │   └── HashJoin
                                │       ├── type: inner
                                │       ├── on: = { lhs: [ n_nationkey ], rhs: [ s_nationkey ] }
                                │       ├── cost: 60521.22
                                │       ├── rows: 10000
                                │       ├── Scan
                                │       │   ├── table: nation
                                │       │   ├── list: [ n_nationkey, n_name ]
                                │       │   ├── filter: null
                                │       │   ├── cost: 50
                                │       │   └── rows: 25
                                │       └── Scan
                                │           ├── table: supplier
                                │           ├── list: [ s_suppkey, s_nationkey ]
                                │           ├── filter: null
                                │           ├── cost: 20000
                                │           └── rows: 10000
                                └── Projection
                                    ├── exprs:
                                    │   ┌── l_orderkey
                                    │   ├── l_partkey
                                    │   ├── l_suppkey
                                    │   ├── l_quantity
                                    │   ├── l_extendedprice
                                    │   └── l_discount
                                    ├── cost: 80373896
                                    ├── rows: 6001215
                                    └── HashJoin
                                        ├── type: inner
                                        ├── on: = { lhs: [ p_partkey ], rhs: [ l_partkey ] }
                                        ├── cost: 79953810
                                        ├── rows: 6001215
                                        ├── Projection { exprs: [ p_partkey ], cost: 846000, rows: 200000 }
                                        │   └── Filter
                                        │       ├── cond: like { lhs: p_name, rhs: '%green%' }
                                        │       ├── cost: 842000
                                        │       ├── rows: 200000
                                        │       └── Scan
                                        │           ├── table: part
                                        │           ├── list: [ p_partkey, p_name ]
                                        │           ├── filter: null
                                        │           ├── cost: 400000
                                        │           └── rows: 200000
                                        └── Scan
                                            ├── table: lineitem
                                            ├── list:
                                            │   ┌── l_orderkey
                                            │   ├── l_partkey
                                            │   ├── l_suppkey
                                            │   ├── l_quantity
                                            │   ├── l_extendedprice
                                            │   └── l_discount
                                            ├── filter: null
                                            ├── cost: 36007290
                                            └── rows: 6001215
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
Projection
├── exprs:
│   ┌── c_custkey
│   ├── c_name
│   ├── sum
│   │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
│   ├── c_acctbal
│   ├── n_name
│   ├── c_address
│   ├── c_phone
│   └── c_comment
├── cost: 209805120
├── rows: 20
└── TopN
    ├── limit: 20
    ├── offset: 0
    ├── order_by:desc
    │   └── sum
    │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    ├── cost: 209805120
    ├── rows: 20
    └── HashAgg
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ]
        ├── cost: 165881800
        ├── rows: 10000000
        └── Projection
            ├── exprs:
            │   ┌── n_name
            │   ├── c_custkey
            │   ├── c_name
            │   ├── c_address
            │   ├── c_phone
            │   ├── c_acctbal
            │   ├── c_comment
            │   ├── l_extendedprice
            │   └── l_discount
            ├── cost: 83653736
            ├── rows: 3000607.5
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                ├── cost: 83353670
                ├── rows: 3000607.5
                ├── Projection
                │   ├── exprs: [ n_name, c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, o_orderkey ]
                │   ├── cost: 12334374
                │   ├── rows: 375000
                │   └── HashJoin
                │       ├── type: inner
                │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                │       ├── cost: 12300624
                │       ├── rows: 375000
                │       ├── Projection
                │       │   ├── exprs: [ n_name, c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment ]
                │       │   ├── cost: 2419102
                │       │   ├── rows: 150000
                │       │   └── HashJoin
                │       │       ├── type: inner
                │       │       ├── on: = { lhs: [ n_nationkey ], rhs: [ c_nationkey ] }
                │       │       ├── cost: 2407102
                │       │       ├── rows: 150000
                │       │       ├── Scan
                │       │       │   ├── table: nation
                │       │       │   ├── list: [ n_nationkey, n_name ]
                │       │       │   ├── filter: null
                │       │       │   ├── cost: 50
                │       │       │   └── rows: 25
                │       │       └── Scan
                │       │           ├── table: customer
                │       │           ├── list:
                │       │           │   ┌── c_custkey
                │       │           │   ├── c_name
                │       │           │   ├── c_address
                │       │           │   ├── c_nationkey
                │       │           │   ├── c_phone
                │       │           │   ├── c_acctbal
                │       │           │   └── c_comment
                │       │           ├── filter: null
                │       │           ├── cost: 1050000
                │       │           └── rows: 150000
                │       └── Projection { exprs: [ o_orderkey, o_custkey ], cost: 6416250, rows: 375000 }
                │           └── Filter
                │               ├── cond:and
                │               │   ├── lhs: > { lhs: 1994-01-01, rhs: o_orderdate }
                │               │   └── rhs: >= { lhs: o_orderdate, rhs: 1993-10-01 }
                │               ├── cost: 6405000
                │               ├── rows: 375000
                │               └── Scan
                │                   ├── table: orders
                │                   ├── list: [ o_orderkey, o_custkey, o_orderdate ]
                │                   ├── filter: null
                │                   ├── cost: 4500000
                │                   └── rows: 1500000
                └── Projection { exprs: [ l_orderkey, l_extendedprice, l_discount ], cost: 37387570, rows: 3000607.5 }
                    └── Filter { cond: = { lhs: l_returnflag, rhs: 'R' }, cost: 37267544, rows: 3000607.5 }
                        └── Scan
                            ├── table: lineitem
                            ├── list: [ l_orderkey, l_extendedprice, l_discount, l_returnflag ]
                            ├── filter: null
                            ├── cost: 24004860
                            └── rows: 6001215
*/

-- tpch-q12
explain select
    l_shipmode,
    sum(case
        when o_orderpriority = '1-URGENT'
            or o_orderpriority = '2-HIGH'
            then 1
        else 0
    end) as high_line_count,
    sum(case
        when o_orderpriority <> '1-URGENT'
            and o_orderpriority <> '2-HIGH'
            then 1
        else 0
    end) as low_line_count
from
    orders,
    lineitem
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
group by
    l_shipmode
order by
    l_shipmode;

/*
Projection
├── exprs:
│   ┌── l_shipmode
│   ├── sum
│   │   └── If
│   │       ├── cond:or
│   │       │   ├── lhs: = { lhs: o_orderpriority, rhs: '1-URGENT' }
│   │       │   └── rhs: = { lhs: o_orderpriority, rhs: '2-HIGH' }
│   │       ├── then: 1
│   │       └── else: 0
│   └── sum
│       └── If
│           ├── cond:and
│           │   ├── lhs: <> { lhs: o_orderpriority, rhs: '1-URGENT' }
│           │   └── rhs: <> { lhs: o_orderpriority, rhs: '2-HIGH' }
│           ├── then: 1
│           └── else: 0
├── cost: 50489800
├── rows: 10
└── Order { by: [ l_shipmode ], cost: 50489800, rows: 10 }
    └── HashAgg
        ├── aggs:
        │   ┌── sum
        │   │   └── If
        │   │       ├── cond:or
        │   │       │   ├── lhs: = { lhs: o_orderpriority, rhs: '1-URGENT' }
        │   │       │   └── rhs: = { lhs: o_orderpriority, rhs: '2-HIGH' }
        │   │       ├── then: 1
        │   │       └── else: 0
        │   └── sum
        │       └── If
        │           ├── cond:and
        │           │   ├── lhs: <> { lhs: o_orderpriority, rhs: '1-URGENT' }
        │           │   └── rhs: <> { lhs: o_orderpriority, rhs: '2-HIGH' }
        │           ├── then: 1
        │           └── else: 0
        ├── group_by: [ l_shipmode ]
        ├── cost: 50489736
        ├── rows: 10
        └── Projection { exprs: [ o_orderpriority, l_shipmode ], cost: 47632816, rows: 1500000 }
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ l_orderkey ], rhs: [ o_orderkey ] }
                ├── cost: 47587816
                ├── rows: 1500000
                ├── Filter
                │   ├── cond:In { in: [ 'MAIL', 'SHIP' ] }
                │   │   └── l_shipmode
                │   ├── cost: 38274000
                │   ├── rows: 250050.61
                │   └── Projection { exprs: [ l_orderkey, l_shipmode ], cost: 37653876, rows: 375075.94 }
                │       └── Filter
                │           ├── cond:and
                │           │   ├── lhs:and
                │           │   │   ├── lhs: > { lhs: l_receiptdate, rhs: l_commitdate }
                │           │   │   └── rhs: > { lhs: l_commitdate, rhs: l_shipdate }
                │           │   └── rhs:and
                │           │       ├── lhs: >= { lhs: l_receiptdate, rhs: 1994-01-01 }
                │           │       └── rhs: > { lhs: 1995-01-01, rhs: l_receiptdate }
                │           ├── cost: 37642624
                │           ├── rows: 375075.94
                │           └── Scan
                │               ├── table: lineitem
                │               ├── list: [ l_orderkey, l_shipdate, l_commitdate, l_receiptdate, l_shipmode ]
                │               ├── filter: null
                │               ├── cost: 30006076
                │               └── rows: 6001215
                └── Scan
                    ├── table: orders
                    ├── list: [ o_orderkey, o_orderpriority ]
                    ├── filter: null
                    ├── cost: 3000000
                    └── rows: 1500000
*/

