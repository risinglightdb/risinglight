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
│   │   └──  l_quantity
│   ├── sum
│   │   └──  l_extendedprice
│   ├── sum
│   │   └──  * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
│   ├── sum
│   │   └──  * { lhs: + { lhs: l_tax, rhs: 1 }, rhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } } }
│   ├── /
│   │   ├── lhs:sum
│   │   │   └──  l_quantity
│   │   ├── rhs:count
│   │   │   └──  l_quantity

│   ├── /
│   │   ├── lhs:sum
│   │   │   └──  l_extendedprice
│   │   ├── rhs:count
│   │   │   └──  l_extendedprice

│   ├── /
│   │   ├── lhs:sum
│   │   │   └──  l_discount
│   │   ├── rhs:count
│   │   │   └──  l_discount

│   └── rowcount
├── cost: 37356.95
└── Order { by: [ l_returnflag, l_linestatus ], cost: 35276.95 }
    └── HashAgg
        ├── aggs:
        │   ┌── sum
        │   │   └──  l_quantity
        │   ├── sum
        │   │   └──  l_extendedprice
        │   ├── sum
        │   │   └──  * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        │   ├── sum
        │   │   └── *
        │   │       ├── lhs: + { lhs: l_tax, rhs: 1 }
        │   │       └── rhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        │   ├── count
        │   │   └──  l_quantity
        │   ├── count
        │   │   └──  l_extendedprice
        │   ├── sum
        │   │   └──  l_discount
        │   ├── count
        │   │   └──  l_discount
        │   └── rowcount
        ├── group_by: [ l_returnflag, l_linestatus ]
        ├── cost: 27417.967
        └── Projection
            ├── exprs: [ l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus ]
            ├── cost: 13460
            └── Filter { cond: >= { lhs: 1998-09-21, rhs: l_shipdate }, cost: 12900 }
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
│   │   └──  * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
│   ├── o_orderdate
│   └── o_shippriority
├── cost: 61579.844
└── TopN
    ├── limit: 10
    ├── offset: 0
    ├── order_by:
    │   ┌── desc
    │   │   └── sum
    │   │       └──  * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    │   └── o_orderdate
    ├── cost: 61565.844
    └── HashAgg
        ├── aggs:sum
        │   └──  * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ l_orderkey, o_orderdate, o_shippriority ]
        ├── cost: 60142.07
        └── Projection
            ├── exprs: [ o_orderdate, o_shippriority, l_orderkey, l_extendedprice, l_discount ]
            ├── cost: 50744.105
            └── HashJoin { type: inner, on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }, cost: 50264.105 }
                ├── Projection { exprs: [ o_orderkey, o_orderdate, o_shippriority ], cost: 22211.05 }
                │   └── HashJoin { type: inner, on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }, cost: 21891.05 }
                │       ├── Projection { exprs: [ c_custkey ], cost: 2740 }
                │       │   └── Filter { cond: = { lhs: c_mktsegment, rhs: 'BUILDING' }, cost: 2700 }
                │       │       └── Scan
                │       │           ├── table: customer
                │       │           ├── list: [ c_custkey, c_mktsegment ]
                │       │           ├── filter: null
                │       │           └── cost: 2000
                │       └── Filter { cond: > { lhs: 1995-03-15, rhs: o_orderdate }, cost: 7500 }
                │           └── Scan
                │               ├── table: orders
                │               ├── list: [ o_orderkey, o_custkey, o_orderdate, o_shippriority ]
                │               ├── filter: null
                │               └── cost: 4000
                └── Projection { exprs: [ l_orderkey, l_extendedprice, l_discount ], cost: 7820 }
                    └── Filter { cond: > { lhs: l_shipdate, rhs: 1995-03-15 }, cost: 7500 }
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
│       └──  * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
├── cost: 153615.28
└── Order
    ├── by:desc
    │   └── sum
    │       └──  * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    ├── cost: 153115.28
    └── HashAgg
        ├── aggs:sum
        │   └──  * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ n_name ]
        ├── cost: 147630.95
        └── Projection { exprs: [ n_name, l_extendedprice, l_discount ], cost: 136762.28 }
            └── HashJoin { type: inner, on: = { lhs: [ n_regionkey ], rhs: [ r_regionkey ] }, cost: 136362.28 }
                ├── Projection { exprs: [ n_name, n_regionkey, l_extendedprice, l_discount ], cost: 116661.61 }
                │   └── HashJoin { type: inner, on: = { lhs: [ s_nationkey ], rhs: [ n_nationkey ] }, cost: 116161.61 }
                │       ├── Projection { exprs: [ s_nationkey, l_extendedprice, l_discount ], cost: 87227.16 }
                │       │   └── HashJoin
                │       │       ├── type: inner
                │       │       ├── on: = { lhs: [ l_suppkey, c_nationkey ], rhs: [ s_suppkey, s_nationkey ] }
                │       │       ├── cost: 86827.16
                │       │       ├── Projection
                │       │       │   ├── exprs: [ c_nationkey, l_suppkey, l_extendedprice, l_discount ]
                │       │       │   ├── cost: 58892.703
                │       │       │   └── HashJoin
                │       │       │       ├── type: inner
                │       │       │       ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                │       │       │       ├── cost: 58392.703
                │       │       │       ├── Projection { exprs: [ c_nationkey, o_orderkey ], cost: 28458.25 }
                │       │       │       │   └── HashJoin
                │       │       │       │       ├── type: inner
                │       │       │       │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                │       │       │       │       ├── cost: 28158.25
                │       │       │       │       ├── Scan
                │       │       │       │       │   ├── table: customer
                │       │       │       │       │   ├── list: [ c_custkey, c_nationkey ]
                │       │       │       │       │   ├── filter: null
                │       │       │       │       │   └── cost: 2000
                │       │       │       │       └── Projection { exprs: [ o_orderkey, o_custkey ], cost: 5812 }
                │       │       │       │           └── Filter
                │       │       │       │               ├── cond:and
                │       │       │       │               │   ├── lhs: >= { lhs: o_orderdate, rhs: 1994-01-01 }
                │       │       │       │               │   └── rhs: > { lhs: 1995-01-01, rhs: o_orderdate }
                │       │       │       │               ├── cost: 5620
                │       │       │       │               └── Scan
                │       │       │       │                   ├── table: orders
                │       │       │       │                   ├── list: [ o_orderkey, o_custkey, o_orderdate ]
                │       │       │       │                   ├── filter: null
                │       │       │       │                   └── cost: 3000
                │       │       │       └── Scan
                │       │       │           ├── table: lineitem
                │       │       │           ├── list: [ l_orderkey, l_suppkey, l_extendedprice, l_discount ]
                │       │       │           ├── filter: null
                │       │       │           └── cost: 4000
                │       │       └──  Scan { table: supplier, list: [ s_suppkey, s_nationkey ], filter: null, cost: 2000 }
                │       └──  Scan { table: nation, list: [ n_nationkey, n_name, n_regionkey ], filter: null, cost: 3000 }
                └── Projection { exprs: [ r_regionkey ], cost: 2740 }
                    └── Filter { cond: = { lhs: r_name, rhs: 'AFRICA' }, cost: 2700 }
                        └──  Scan { table: region, list: [ r_regionkey, r_name ], filter: null, cost: 2000 }
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
│   └──  * { lhs: l_discount, rhs: l_extendedprice }
├── cost: 7474.4644
└── Agg
    ├── aggs:sum
    │   └──  * { lhs: l_discount, rhs: l_extendedprice }
    ├── cost: 7473.8643
    └── Projection { exprs: [ l_extendedprice, l_discount ], cost: 7309.0244 }
        └── Filter
            ├── cond:and
            │   ├── lhs: > { lhs: 24, rhs: l_quantity }
            │   └── rhs:and
            │       ├── lhs: >= { lhs: 0.09, rhs: l_discount }
            │       └── rhs:and
            │           ├── lhs: >= { lhs: l_discount, rhs: 0.07 }
            │           └── rhs:and
            │               ├── lhs: > { lhs: 1995-01-01, rhs: l_shipdate }
            │               └── rhs: >= { lhs: l_shipdate, rhs: 1994-01-01 }
            ├── cost: 7210.72
            └── Scan
                ├── table: lineitem
                ├── list: [ l_quantity, l_extendedprice, l_discount, l_shipdate ]
                ├── filter: null
                └── cost: 4000
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
├── cost: 155872880
└── Order
    ├── by:
    │   ┌── n_name
    │   └── desc
    │       └──  Extract { from: o_orderdate, field: YEAR }
    ├── cost: 154822880
    └── HashAgg
        ├── aggs:sum
        │   └── -
        │       ├── lhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        │       └── rhs: * { lhs: ps_supplycost, rhs: l_quantity }
        ├── group_by: [ n_name, Extract { from: o_orderdate, field: YEAR } ]
        ├── cost: 143857090
        └── Projection
            ├── exprs:
            │   ┌── n_name
            │   ├── Extract { from: o_orderdate, field: YEAR }
            │   └── -
            │       ├── lhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
            │       └── rhs: * { lhs: ps_supplycost, rhs: l_quantity }
            ├── cost: 121525510
            └── HashJoin { type: inner, on: = { lhs: [ s_nationkey ], rhs: [ n_nationkey ] }, cost: 120125510 }
                ├── Projection
                │   ├── exprs: [ s_nationkey, ps_supplycost, o_orderdate, l_quantity, l_extendedprice, l_discount ]
                │   ├── cost: 92172010
                │   └── HashJoin { type: inner, on: = { lhs: [ l_orderkey ], rhs: [ o_orderkey ] }, cost: 91472010 }
                │       ├── Projection
                │       │   ├── exprs:
                │       │   │   ┌── s_nationkey
                │       │   │   ├── ps_supplycost
                │       │   │   ├── l_orderkey
                │       │   │   ├── l_quantity
                │       │   │   ├── l_extendedprice
                │       │   │   └── l_discount
                │       │   ├── cost: 63518504
                │       │   └── HashJoin
                │       │       ├── type: inner
                │       │       ├── on: = { lhs: [ l_suppkey, l_partkey ], rhs: [ ps_suppkey, ps_partkey ] }
                │       │       ├── cost: 62818504
                │       │       ├── Projection
                │       │       │   ├── exprs:
                │       │       │   │   ┌── s_nationkey
                │       │       │   │   ├── l_orderkey
                │       │       │   │   ├── l_partkey
                │       │       │   │   ├── l_suppkey
                │       │       │   │   ├── l_quantity
                │       │       │   │   ├── l_extendedprice
                │       │       │   │   └── l_discount
                │       │       │   ├── cost: 32864002
                │       │       │   └── HashJoin
                │       │       │       ├── type: inner
                │       │       │       ├── on: = { lhs: [ s_suppkey, p_partkey ], rhs: [ l_suppkey, l_partkey ] }
                │       │       │       ├── cost: 32064002
                │       │       │       ├── Join { type: inner, cost: 3106500 }
                │       │       │       │   ├── Projection { exprs: [ p_partkey ], cost: 4500 }
                │       │       │       │   │   └── Filter { cond: like { lhs: p_name, rhs: '%green%' }, cost: 4300 }
                │       │       │       │   │       └── Scan
                │       │       │       │   │           ├── table: part
                │       │       │       │   │           ├── list: [ p_partkey, p_name ]
                │       │       │       │   │           ├── filter: null
                │       │       │       │   │           └── cost: 2000
                │       │       │       │   └── Scan
                │       │       │       │       ├── table: supplier
                │       │       │       │       ├── list: [ s_suppkey, s_nationkey ]
                │       │       │       │       ├── filter: null
                │       │       │       │       └── cost: 2000
                │       │       │       └── Scan
                │       │       │           ├── table: lineitem
                │       │       │           ├── list:
                │       │       │           │   ┌── l_orderkey
                │       │       │           │   ├── l_partkey
                │       │       │           │   ├── l_suppkey
                │       │       │           │   ├── l_quantity
                │       │       │           │   ├── l_extendedprice
                │       │       │           │   └── l_discount
                │       │       │           ├── filter: null
                │       │       │           └── cost: 6000
                │       │       └── Scan
                │       │           ├── table: partsupp
                │       │           ├── list: [ ps_partkey, ps_suppkey, ps_supplycost ]
                │       │           ├── filter: null
                │       │           └── cost: 3000
                │       └──  Scan { table: orders, list: [ o_orderkey, o_orderdate ], filter: null, cost: 2000 }
                └──  Scan { table: nation, list: [ n_nationkey, n_name ], filter: null, cost: 2000 }
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
│   │   └──  * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
│   ├── c_acctbal
│   ├── n_name
│   ├── c_address
│   ├── c_phone
│   └── c_comment
├── cost: 119002.195
└── TopN
    ├── limit: 20
    ├── offset: 0
    ├── order_by:desc
    │   └── sum
    │       └──  * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    ├── cost: 118958.195
    └── HashAgg
        ├── aggs:sum
        │   └──  * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ]
        ├── cost: 116602.04
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
            ├── cost: 102133.375
            └── HashJoin { type: inner, on: = { lhs: [ c_nationkey ], rhs: [ n_nationkey ] }, cost: 101133.375 }
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
                │   ├── cost: 68198.92
                │   └── HashJoin { type: inner, on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }, cost: 67198.92 }
                │       ├── Projection
                │       │   ├── exprs:
                │       │   │   ┌── c_custkey
                │       │   │   ├── c_name
                │       │   │   ├── c_address
                │       │   │   ├── c_nationkey
                │       │   │   ├── c_phone
                │       │   │   ├── c_acctbal
                │       │   │   ├── c_comment
                │       │   │   └── o_orderkey
                │       │   ├── cost: 39058.25
                │       │   └── HashJoin
                │       │       ├── type: inner
                │       │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                │       │       ├── cost: 38158.25
                │       │       ├── Scan
                │       │       │   ├── table: customer
                │       │       │   ├── list:
                │       │       │   │   ┌── c_custkey
                │       │       │   │   ├── c_name
                │       │       │   │   ├── c_address
                │       │       │   │   ├── c_nationkey
                │       │       │   │   ├── c_phone
                │       │       │   │   ├── c_acctbal
                │       │       │   │   └── c_comment
                │       │       │   ├── filter: null
                │       │       │   └── cost: 7000
                │       │       └── Projection { exprs: [ o_orderkey, o_custkey ], cost: 5812 }
                │       │           └── Filter
                │       │               ├── cond:and
                │       │               │   ├── lhs: >= { lhs: o_orderdate, rhs: 1993-10-01 }
                │       │               │   └── rhs: > { lhs: 1994-01-01, rhs: o_orderdate }
                │       │               ├── cost: 5620
                │       │               └── Scan
                │       │                   ├── table: orders
                │       │                   ├── list: [ o_orderkey, o_custkey, o_orderdate ]
                │       │                   ├── filter: null
                │       │                   └── cost: 3000
                │       └── Projection { exprs: [ l_orderkey, l_extendedprice, l_discount ], cost: 5180 }
                │           └── Filter { cond: = { lhs: l_returnflag, rhs: 'R' }, cost: 5100 }
                │               └── Scan
                │                   ├── table: lineitem
                │                   ├── list: [ l_orderkey, l_extendedprice, l_discount, l_returnflag ]
                │                   ├── filter: null
                │                   └── cost: 4000
                └──  Scan { table: nation, list: [ n_nationkey, n_name ], filter: null, cost: 2000 }
*/

