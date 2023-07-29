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
│   │       ├── lhs: - { lhs: 1, rhs: l_discount }
│   │       └── rhs: + { lhs: l_extendedprice, rhs: * { lhs: l_tax, rhs: l_extendedprice } }
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
├── cost: 23421.156
└── Order { by: [ l_returnflag, l_linestatus ], cost: 23288.656 }
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
        │   │       ├── lhs: - { lhs: 1, rhs: l_discount }
        │   │       └── rhs: + { lhs: l_extendedprice, rhs: * { lhs: l_tax, rhs: l_extendedprice } }
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
        ├── cost: 18545.771
        └── Projection
            ├── exprs: [ l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus ]
            ├── cost: 10790
            └── Filter { cond: >= { lhs: 1998-09-21, rhs: l_shipdate }, cost: 10710 }
                └── Scan
                    ├── table: lineitem
                    ├── list: [ l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate ]
                    ├── filter: null
                    └── cost: 7000
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
├── cost: 45544.367
└── TopN
    ├── limit: 10
    ├── offset: 0
    ├── order_by:
    │   ┌── desc
    │   │   └── sum
    │   │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    │   └── o_orderdate
    ├── cost: 45542.97
    └── HashAgg
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ l_orderkey, o_orderdate, o_shippriority ]
        ├── cost: 44638.11
        └── Projection
            ├── exprs: [ o_orderdate, o_shippriority, l_orderkey, l_extendedprice, l_discount ]
            ├── cost: 39327.336
            └── HashJoin { type: inner, on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }, cost: 39252.336 }
                ├── Projection { exprs: [ o_orderkey, o_orderdate, o_shippriority ], cost: 21008.668 }
                │   └── HashJoin { type: inner, on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }, cost: 20943.668 }
                │       ├── Projection { exprs: [ c_custkey ], cost: 3265 }
                │       │   └── Filter { cond: = { lhs: c_mktsegment, rhs: 'BUILDING' }, cost: 3210 }
                │       │       └── Scan
                │       │           ├── table: customer
                │       │           ├── list: [ c_custkey, c_mktsegment ]
                │       │           ├── filter: null
                │       │           └── cost: 2000
                │       └── Filter { cond: > { lhs: 1995-03-15, rhs: o_orderdate }, cost: 6210 }
                │           └── Scan
                │               ├── table: orders
                │               ├── list: [ o_orderkey, o_custkey, o_orderdate, o_shippriority ]
                │               ├── filter: null
                │               └── cost: 4000
                └── Projection { exprs: [ l_orderkey, l_extendedprice, l_discount ], cost: 6275 }
                    └── Filter { cond: > { lhs: l_shipdate, rhs: 1995-03-15 }, cost: 6210 }
                        └── Scan
                            ├── table: lineitem
                            ├── list: [ l_orderkey, l_extendedprice, l_discount, l_shipdate ]
                            ├── filter: null
                            └── cost: 4000
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
├── cost: 145588.78
└── Order
    ├── by:desc
    │   └── sum
    │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    ├── cost: 145528.78
    └── HashAgg
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ n_name ]
        ├── cost: 140044.45
        └── Projection { exprs: [ n_name, l_extendedprice, l_discount ], cost: 129445.79 }
            └── HashJoin { type: inner, on: = { lhs: [ r_regionkey ], rhs: [ n_regionkey ] }, cost: 129315.79 }
                ├── Projection { exprs: [ r_regionkey ], cost: 3265 }
                │   └── Filter { cond: = { lhs: r_name, rhs: 'AFRICA' }, cost: 3210 }
                │       └── Scan { table: region, list: [ r_regionkey, r_name ], filter: null, cost: 2000 }
                └── Projection { exprs: [ n_name, n_regionkey, l_extendedprice, l_discount ], cost: 107597.79 }
                    └── HashJoin { type: inner, on: = { lhs: [ s_nationkey ], rhs: [ n_nationkey ] }, cost: 107457.79 }
                        ├── Projection { exprs: [ s_nationkey, l_extendedprice, l_discount ], cost: 78523.336 }
                        │   └── HashJoin
                        │       ├── type: inner
                        │       ├── on: = { lhs: [ l_suppkey, c_nationkey ], rhs: [ s_suppkey, s_nationkey ] }
                        │       ├── cost: 78393.336
                        │       ├── Projection
                        │       │   ├── exprs: [ c_nationkey, l_suppkey, l_extendedprice, l_discount ]
                        │       │   ├── cost: 50458.883
                        │       │   └── HashJoin
                        │       │       ├── type: inner
                        │       │       ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                        │       │       ├── cost: 50318.883
                        │       │       ├── Projection { exprs: [ c_nationkey, o_orderkey ], cost: 20384.43 }
                        │       │       │   └── HashJoin
                        │       │       │       ├── type: inner
                        │       │       │       ├── on: = { lhs: [ o_custkey ], rhs: [ c_custkey ] }
                        │       │       │       ├── cost: 20264.43
                        │       │       │       ├── Projection { exprs: [ o_orderkey, o_custkey ], cost: 4300 }
                        │       │       │       │   └── Filter
                        │       │       │       │       ├── cond:and
                        │       │       │       │       │   ├── lhs: >= { lhs: o_orderdate, rhs: 1994-01-01 }
                        │       │       │       │       │   └── rhs: > { lhs: 1995-01-01, rhs: o_orderdate }
                        │       │       │       │       ├── cost: 4270
                        │       │       │       │       └── Scan
                        │       │       │       │           ├── table: orders
                        │       │       │       │           ├── list: [ o_orderkey, o_custkey, o_orderdate ]
                        │       │       │       │           ├── filter: null
                        │       │       │       │           └── cost: 3000
                        │       │       │       └── Scan
                        │       │       │           ├── table: customer
                        │       │       │           ├── list: [ c_custkey, c_nationkey ]
                        │       │       │           ├── filter: null
                        │       │       │           └── cost: 2000
                        │       │       └── Scan
                        │       │           ├── table: lineitem
                        │       │           ├── list: [ l_orderkey, l_suppkey, l_extendedprice, l_discount ]
                        │       │           ├── filter: null
                        │       │           └── cost: 4000
                        │       └── Scan { table: supplier, list: [ s_suppkey, s_nationkey ], filter: null, cost: 2000 }
                        └── Scan { table: nation, list: [ n_nationkey, n_name, n_regionkey ], filter: null, cost: 3000 }
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
├── cost: 5589.86
└── Agg
    ├── aggs:sum
    │   └── * { lhs: l_discount, rhs: l_extendedprice }
    ├── cost: 5589.75
    └── Projection { exprs: [ l_extendedprice, l_discount ], cost: 5578.75 }
        └── Filter
            ├── cond:and
            │   ├── lhs: > { lhs: 24, rhs: l_quantity }
            │   └── rhs:and
            │       ├── lhs: and { lhs: >= { lhs: 0.09, rhs: l_discount }, rhs: >= { lhs: l_discount, rhs: 0.07 } }
            │       └── rhs:and
            │           ├── lhs: > { lhs: 1995-01-01, rhs: l_shipdate }
            │           └── rhs: >= { lhs: l_shipdate, rhs: 1994-01-01 }
            ├── cost: 5575
            └── Scan
                ├── table: lineitem
                ├── list: [ l_quantity, l_extendedprice, l_discount, l_shipdate ]
                ├── filter: null
                └── cost: 4000
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
├── cost: 152868.92
└── Order { by: [ n_name, n_name(1), Extract { from: l_shipdate, field: YEAR } ], cost: 152838.3 }
    └── HashAgg
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ n_name, n_name(1), Extract { from: l_shipdate, field: YEAR } ]
        ├── cost: 150261.48
        └── Projection
            ├── exprs:
            │   ┌── n_name
            │   ├── n_name(1)
            │   ├── Extract { from: l_shipdate, field: YEAR }
            │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
            ├── cost: 145834.11
            └── Filter
                ├── cond:or
                │   ├── lhs: and { lhs: = { lhs: n_name, rhs: 'FRANCE' }, rhs: = { lhs: n_name(1), rhs: 'GERMANY' } }
                │   └── rhs: and { lhs: = { lhs: n_name, rhs: 'GERMANY' }, rhs: = { lhs: n_name(1), rhs: 'FRANCE' } }
                ├── cost: 145549.73
                └── Projection
                    ├── exprs: [ n_name, n_name(1), l_extendedprice, l_discount, l_shipdate ]
                    ├── cost: 142222.23
                    └── HashJoin
                        ├── type: inner
                        ├── on: = { lhs: [ c_nationkey ], rhs: [ n_nationkey(1) ] }
                        ├── cost: 142072.23
                        ├── Projection
                        │   ├── exprs: [ n_name, c_nationkey, l_extendedprice, l_discount, l_shipdate ]
                        │   ├── cost: 113137.79
                        │   └── HashJoin
                        │       ├── type: inner
                        │       ├── on: = { lhs: [ s_nationkey ], rhs: [ n_nationkey ] }
                        │       ├── cost: 112987.79
                        │       ├── Projection
                        │       │   ├── exprs: [ s_nationkey, c_nationkey, l_extendedprice, l_discount, l_shipdate ]
                        │       │   ├── cost: 84053.336
                        │       │   └── HashJoin
                        │       │       ├── type: inner
                        │       │       ├── on: = { lhs: [ o_custkey ], rhs: [ c_custkey ] }
                        │       │       ├── cost: 83903.336
                        │       │       ├── Projection
                        │       │       │   ├── exprs:
                        │       │       │   │   ┌── s_nationkey
                        │       │       │   │   ├── o_custkey
                        │       │       │   │   ├── l_extendedprice
                        │       │       │   │   ├── l_discount
                        │       │       │   │   └── l_shipdate
                        │       │       │   ├── cost: 54968.883
                        │       │       │   └── HashJoin
                        │       │       │       ├── type: inner
                        │       │       │       ├── on: = { lhs: [ l_orderkey ], rhs: [ o_orderkey ] }
                        │       │       │       ├── cost: 54818.883
                        │       │       │       ├── Projection
                        │       │       │       │   ├── exprs:
                        │       │       │       │   │   ┌── s_nationkey
                        │       │       │       │   │   ├── l_orderkey
                        │       │       │       │   │   ├── l_extendedprice
                        │       │       │       │   │   ├── l_discount
                        │       │       │       │   │   └── l_shipdate
                        │       │       │       │   ├── cost: 25884.43
                        │       │       │       │   └── HashJoin
                        │       │       │       │       ├── type: inner
                        │       │       │       │       ├── on: = { lhs: [ l_suppkey ], rhs: [ s_suppkey ] }
                        │       │       │       │       ├── cost: 25734.43
                        │       │       │       │       ├── Filter
                        │       │       │       │       │   ├── cond:and
                        │       │       │       │       │   │   ├── lhs: >= { lhs: l_shipdate, rhs: 1995-01-01 }
                        │       │       │       │       │   │   └── rhs: >= { lhs: 1996-12-31, rhs: l_shipdate }
                        │       │       │       │       │   ├── cost: 6770
                        │       │       │       │       │   └── Scan
                        │       │       │       │       │       ├── table: lineitem
                        │       │       │       │       │       ├── list:
                        │       │       │       │       │       │   ┌── l_orderkey
                        │       │       │       │       │       │   ├── l_suppkey
                        │       │       │       │       │       │   ├── l_extendedprice
                        │       │       │       │       │       │   ├── l_discount
                        │       │       │       │       │       │   └── l_shipdate
                        │       │       │       │       │       ├── filter: null
                        │       │       │       │       │       └── cost: 5000
                        │       │       │       │       └── Scan
                        │       │       │       │           ├── table: supplier
                        │       │       │       │           ├── list: [ s_suppkey, s_nationkey ]
                        │       │       │       │           ├── filter: null
                        │       │       │       │           └── cost: 2000
                        │       │       │       └── Scan
                        │       │       │           ├── table: orders
                        │       │       │           ├── list: [ o_orderkey, o_custkey ]
                        │       │       │           ├── filter: null
                        │       │       │           └── cost: 2000
                        │       │       └── Scan
                        │       │           ├── table: customer
                        │       │           ├── list: [ c_custkey, c_nationkey ]
                        │       │           ├── filter: null
                        │       │           └── cost: 2000
                        │       └── Scan { table: nation, list: [ n_nationkey, n_name ], filter: null, cost: 2000 }
                        └── Scan { table: nation, list: [ n_nationkey(1), n_name(1) ], filter: null, cost: 2000 }
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

├── cost: 205536.23
└── Order { by: [ Extract { from: o_orderdate, field: YEAR } ], cost: 205421.23 }
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
        ├── cost: 199436.9
        └── Projection
            ├── exprs:
            │   ┌── Extract { from: o_orderdate, field: YEAR }
            │   ├── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
            │   └── n_name(1)
            ├── cost: 187928.23
            └── HashJoin { type: inner, on: = { lhs: [ r_regionkey ], rhs: [ n_regionkey ] }, cost: 187288.23 }
                ├── Projection { exprs: [ r_regionkey ], cost: 3265 }
                │   └── Filter { cond: = { lhs: r_name, rhs: 'AMERICA' }, cost: 3210 }
                │       └── Scan { table: region, list: [ r_regionkey, r_name ], filter: null, cost: 2000 }
                └── Projection
                    ├── exprs: [ n_regionkey, n_name(1), o_orderdate, l_extendedprice, l_discount ]
                    ├── cost: 164570.23
                    └── HashJoin
                        ├── type: inner
                        ├── on: = { lhs: [ s_nationkey ], rhs: [ n_nationkey(1) ] }
                        ├── cost: 164420.23
                        ├── Projection
                        │   ├── exprs: [ n_regionkey, s_nationkey, o_orderdate, l_extendedprice, l_discount ]
                        │   ├── cost: 135485.78
                        │   └── HashJoin
                        │       ├── type: inner
                        │       ├── on: = { lhs: [ c_nationkey ], rhs: [ n_nationkey ] }
                        │       ├── cost: 135335.78
                        │       ├── Projection
                        │       │   ├── exprs: [ s_nationkey, c_nationkey, o_orderdate, l_extendedprice, l_discount ]
                        │       │   ├── cost: 106401.336
                        │       │   └── HashJoin
                        │       │       ├── type: inner
                        │       │       ├── on: = { lhs: [ o_custkey ], rhs: [ c_custkey ] }
                        │       │       ├── cost: 106251.336
                        │       │       ├── Projection
                        │       │       │   ├── exprs:
                        │       │       │   │   ┌── s_nationkey
                        │       │       │   │   ├── o_custkey
                        │       │       │   │   ├── o_orderdate
                        │       │       │   │   ├── l_extendedprice
                        │       │       │   │   └── l_discount
                        │       │       │   ├── cost: 77316.88
                        │       │       │   └── HashJoin
                        │       │       │       ├── type: inner
                        │       │       │       ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                        │       │       │       ├── cost: 77166.88
                        │       │       │       ├── Filter
                        │       │       │       │   ├── cond:and
                        │       │       │       │   │   ├── lhs: >= { lhs: o_orderdate, rhs: 1995-01-01 }
                        │       │       │       │   │   └── rhs: >= { lhs: 1996-12-31, rhs: o_orderdate }
                        │       │       │       │   ├── cost: 4270
                        │       │       │       │   └── Scan
                        │       │       │       │       ├── table: orders
                        │       │       │       │       ├── list: [ o_orderkey, o_custkey, o_orderdate ]
                        │       │       │       │       ├── filter: null
                        │       │       │       │       └── cost: 3000
                        │       │       │       └── Projection
                        │       │       │           ├── exprs: [ s_nationkey, l_orderkey, l_extendedprice, l_discount ]
                        │       │       │           ├── cost: 55932.453
                        │       │       │           └── HashJoin
                        │       │       │               ├── type: inner
                        │       │       │               ├── on: = { lhs: [ s_suppkey ], rhs: [ l_suppkey ] }
                        │       │       │               ├── cost: 55792.453
                        │       │       │               ├── Scan
                        │       │       │               │   ├── table: supplier
                        │       │       │               │   ├── list: [ s_suppkey, s_nationkey ]
                        │       │       │               │   ├── filter: null
                        │       │       │               │   └── cost: 2000
                        │       │       │               └── Projection
                        │       │       │                   ├── exprs:
                        │       │       │                   │   ┌── l_orderkey
                        │       │       │                   │   ├── l_suppkey
                        │       │       │                   │   ├── l_extendedprice
                        │       │       │                   │   └── l_discount
                        │       │       │                   ├── cost: 27858
                        │       │       │                   └── HashJoin
                        │       │       │                       ├── type: inner
                        │       │       │                       ├── on: = { lhs: [ p_partkey ], rhs: [ l_partkey ] }
                        │       │       │                       ├── cost: 27718
                        │       │       │                       ├── Projection { exprs: [ p_partkey ], cost: 3265 }
                        │       │       │                       │   └── Filter
                        │       │       │                       │       ├── cond:=
                        │       │       │                       │       │   ├── lhs: p_type
                        │       │       │                       │       │   └── rhs: 'ECONOMY ANODIZED STEEL'
                        │       │       │                       │       ├── cost: 3210
                        │       │       │                       │       └── Scan
                        │       │       │                       │           ├── table: part
                        │       │       │                       │           ├── list: [ p_partkey, p_type ]
                        │       │       │                       │           ├── filter: null
                        │       │       │                       │           └── cost: 2000
                        │       │       │                       └── Scan
                        │       │       │                           ├── table: lineitem
                        │       │       │                           ├── list:
                        │       │       │                           │   ┌── l_orderkey
                        │       │       │                           │   ├── l_partkey
                        │       │       │                           │   ├── l_suppkey
                        │       │       │                           │   ├── l_extendedprice
                        │       │       │                           │   └── l_discount
                        │       │       │                           ├── filter: null
                        │       │       │                           └── cost: 5000
                        │       │       └── Scan
                        │       │           ├── table: customer
                        │       │           ├── list: [ c_custkey, c_nationkey ]
                        │       │           ├── filter: null
                        │       │           └── cost: 2000
                        │       └── Scan { table: nation, list: [ n_nationkey, n_regionkey ], filter: null, cost: 2000 }
                        └── Scan { table: nation, list: [ n_nationkey(1), n_name(1) ], filter: null, cost: 2000 }
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
Order
├── by:
│   ┌── n_name
│   └── desc
│       └── Extract { from: o_orderdate, field: YEAR }
├── cost: 178350.27
└── Projection
    ├── exprs:
    │   ┌── n_name
    │   ├── Extract { from: o_orderdate, field: YEAR }
    │   └── sum
    │       └── -
    │           ├── lhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    │           └── rhs: * { lhs: ps_supplycost, rhs: l_quantity }
    ├── cost: 172365.94
    └── HashAgg
        ├── aggs:sum
        │   └── -
        │       ├── lhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        │       └── rhs: * { lhs: ps_supplycost, rhs: l_quantity }
        ├── group_by: [ n_name, Extract { from: o_orderdate, field: YEAR } ]
        ├── cost: 172300.94
        └── Projection
            ├── exprs:
            │   ┌── n_name
            │   ├── Extract { from: o_orderdate, field: YEAR }
            │   └── -
            │       ├── lhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
            │       └── rhs: * { lhs: ps_supplycost, rhs: l_quantity }
            ├── cost: 161502.27
            └── HashJoin { type: inner, on: = { lhs: [ s_nationkey ], rhs: [ n_nationkey ] }, cost: 160642.27 }
                ├── Projection
                │   ├── exprs: [ s_nationkey, ps_supplycost, o_orderdate, l_quantity, l_extendedprice, l_discount ]
                │   ├── cost: 130707.81
                │   └── HashJoin { type: inner, on: = { lhs: [ l_orderkey ], rhs: [ o_orderkey ] }, cost: 130547.81 }
                │       ├── Projection
                │       │   ├── exprs:
                │       │   │   ┌── s_nationkey
                │       │   │   ├── ps_supplycost
                │       │   │   ├── l_orderkey
                │       │   │   ├── l_quantity
                │       │   │   ├── l_extendedprice
                │       │   │   └── l_discount
                │       │   ├── cost: 100613.36
                │       │   └── HashJoin
                │       │       ├── type: inner
                │       │       ├── on: = { lhs: [ l_suppkey, l_partkey ], rhs: [ ps_suppkey, ps_partkey ] }
                │       │       ├── cost: 100453.36
                │       │       ├── Projection
                │       │       │   ├── exprs:
                │       │       │   │   ┌── s_nationkey
                │       │       │   │   ├── l_orderkey
                │       │       │   │   ├── l_partkey
                │       │       │   │   ├── l_suppkey
                │       │       │   │   ├── l_quantity
                │       │       │   │   ├── l_extendedprice
                │       │       │   │   └── l_discount
                │       │       │   ├── cost: 67518.91
                │       │       │   └── HashJoin
                │       │       │       ├── type: inner
                │       │       │       ├── on: = { lhs: [ s_suppkey ], rhs: [ l_suppkey ] }
                │       │       │       ├── cost: 67348.91
                │       │       │       ├── Scan
                │       │       │       │   ├── table: supplier
                │       │       │       │   ├── list: [ s_suppkey, s_nationkey ]
                │       │       │       │   ├── filter: null
                │       │       │       │   └── cost: 2000
                │       │       │       └── Projection
                │       │       │           ├── exprs:
                │       │       │           │   ┌── l_orderkey
                │       │       │           │   ├── l_partkey
                │       │       │           │   ├── l_suppkey
                │       │       │           │   ├── l_quantity
                │       │       │           │   ├── l_extendedprice
                │       │       │           │   └── l_discount
                │       │       │           ├── cost: 37414.453
                │       │       │           └── HashJoin
                │       │       │               ├── type: inner
                │       │       │               ├── on: = { lhs: [ l_partkey ], rhs: [ p_partkey ] }
                │       │       │               ├── cost: 37254.453
                │       │       │               ├── Scan
                │       │       │               │   ├── table: lineitem
                │       │       │               │   ├── list:
                │       │       │               │   │   ┌── l_orderkey
                │       │       │               │   │   ├── l_partkey
                │       │       │               │   │   ├── l_suppkey
                │       │       │               │   │   ├── l_quantity
                │       │       │               │   │   ├── l_extendedprice
                │       │       │               │   │   └── l_discount
                │       │       │               │   ├── filter: null
                │       │       │               │   └── cost: 6000
                │       │       │               └── Projection { exprs: [ p_partkey ], cost: 4320 }
                │       │       │                   └── Filter
                │       │       │                       ├── cond: like { lhs: p_name, rhs: '%green%' }
                │       │       │                       ├── cost: 4210
                │       │       │                       └── Scan
                │       │       │                           ├── table: part
                │       │       │                           ├── list: [ p_partkey, p_name ]
                │       │       │                           ├── filter: null
                │       │       │                           └── cost: 2000
                │       │       └── Scan
                │       │           ├── table: partsupp
                │       │           ├── list: [ ps_partkey, ps_suppkey, ps_supplycost ]
                │       │           ├── filter: null
                │       │           └── cost: 3000
                │       └── Scan { table: orders, list: [ o_orderkey, o_orderdate ], filter: null, cost: 2000 }
                └── Scan { table: nation, list: [ n_nationkey, n_name ], filter: null, cost: 2000 }
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
├── cost: 110505.305
└── TopN
    ├── limit: 20
    ├── offset: 0
    ├── order_by:desc
    │   └── sum
    │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    ├── cost: 110501.7
    └── HashAgg
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ]
        ├── cost: 108145.55
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
            ├── cost: 94486.88
            └── HashJoin { type: inner, on: = { lhs: [ c_nationkey ], rhs: [ n_nationkey ] }, cost: 94296.88 }
                ├── Projection
                │   ├── exprs:
                │   │   ┌── c_custkey
                │   │   ├── c_name
                │   │   ├── c_address
                │   │   ├── c_nationkey
                │   │   ├── c_phone
                │   │   ├── c_acctbal
                │   │   ├── c_comment
                │   │   ├── l_extendedprice
                │   │   └── l_discount
                │   ├── cost: 61362.43
                │   └── HashJoin { type: inner, on: = { lhs: [ l_orderkey ], rhs: [ o_orderkey ] }, cost: 61172.43 }
                │       ├── Projection { exprs: [ l_orderkey, l_extendedprice, l_discount ], cost: 6275 }
                │       │   └── Filter { cond: = { lhs: l_returnflag, rhs: 'R' }, cost: 6210 }
                │       │       └── Scan
                │       │           ├── table: lineitem
                │       │           ├── list: [ l_orderkey, l_extendedprice, l_discount, l_returnflag ]
                │       │           ├── filter: null
                │       │           └── cost: 4000
                │       └── Projection
                │           ├── exprs:
                │           │   ┌── c_custkey
                │           │   ├── c_name
                │           │   ├── c_address
                │           │   ├── c_nationkey
                │           │   ├── c_phone
                │           │   ├── c_acctbal
                │           │   ├── c_comment
                │           │   └── o_orderkey
                │           ├── cost: 30444.43
                │           └── HashJoin
                │               ├── type: inner
                │               ├── on: = { lhs: [ o_custkey ], rhs: [ c_custkey ] }
                │               ├── cost: 30264.43
                │               ├── Projection { exprs: [ o_orderkey, o_custkey ], cost: 4300 }
                │               │   └── Filter
                │               │       ├── cond:and
                │               │       │   ├── lhs: >= { lhs: o_orderdate, rhs: 1993-10-01 }
                │               │       │   └── rhs: > { lhs: 1994-01-01, rhs: o_orderdate }
                │               │       ├── cost: 4270
                │               │       └── Scan
                │               │           ├── table: orders
                │               │           ├── list: [ o_orderkey, o_custkey, o_orderdate ]
                │               │           ├── filter: null
                │               │           └── cost: 3000
                │               └── Scan
                │                   ├── table: customer
                │                   ├── list:
                │                   │   ┌── c_custkey
                │                   │   ├── c_name
                │                   │   ├── c_address
                │                   │   ├── c_nationkey
                │                   │   ├── c_phone
                │                   │   ├── c_acctbal
                │                   │   └── c_comment
                │                   ├── filter: null
                │                   └── cost: 7000
                └── Scan { table: nation, list: [ n_nationkey, n_name ], filter: null, cost: 2000 }
*/

