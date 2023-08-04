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
│   ├── ref
│   │   └── sum
│   │       └── l_quantity
│   ├── ref
│   │   └── sum
│   │       └── l_extendedprice
│   ├── ref
│   │   └── sum
│   │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
│   ├── ref
│   │   └── sum
│   │       └── *
│   │           ├── lhs: + { lhs: l_extendedprice, rhs: * { lhs: l_tax, rhs: l_extendedprice } }
│   │           └── rhs: - { lhs: 1, rhs: l_discount }
│   ├── /
│   │   ├── lhs:ref
│   │   │   └── sum
│   │   │       └── l_quantity
│   │   ├── rhs:ref
│   │   │   └── count
│   │   │       └── l_quantity

│   ├── /
│   │   ├── lhs:ref
│   │   │   └── sum
│   │   │       └── l_extendedprice
│   │   ├── rhs:ref
│   │   │   └── count
│   │   │       └── l_extendedprice

│   ├── /
│   │   ├── lhs:ref
│   │   │   └── sum
│   │   │       └── l_discount
│   │   ├── rhs:ref
│   │   │   └── count
│   │   │       └── l_discount

│   └── ref
│       └── rowcount
├── cost: 70266880
├── rows: 100
└── Order { by: [ l_returnflag, l_linestatus ], cost: 70266840, rows: 100 }
    └── HashAgg
        ├── keys: [ l_returnflag, l_linestatus ]
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

-- tpch-q2
explain select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 15
    and p_type like '%BRASS'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
    and ps_supplycost = (
        select
            min(ps_supplycost)
        from
            partsupp,
            supplier,
            nation,
            region
        where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'EUROPE'
    )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;

/*
Projection
├── exprs: [ s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment ]
├── cost: 183232770
├── rows: 100
└── TopN
    ├── limit: 100
    ├── offset: 0
    ├── order_by:
    │   ┌── desc
    │   │   └── s_acctbal
    │   ├── n_name
    │   ├── s_name
    │   └── p_partkey
    ├── cost: 183232750
    ├── rows: 100
    └── Projection
        ├── exprs: [ p_partkey, p_mfgr, s_name, s_address, s_phone, s_acctbal, s_comment, n_name ]
        ├── cost: 179070580
        ├── rows: 625000
        └── Filter
            ├── cond:=
            │   ├── lhs: ps_supplycost
            │   ├── rhs:ref
            │   │   └── min
            │   │       └── ps_supplycost(1)

            ├── cost: 179014320
            ├── rows: 625000
            └── Projection
                ├── exprs:
                │   ┌── p_partkey
                │   ├── p_mfgr
                │   ├── s_name
                │   ├── s_address
                │   ├── s_phone
                │   ├── s_acctbal
                │   ├── s_comment
                │   ├── ps_supplycost
                │   ├── n_name
                │   └── ref
                │       └── min
                │           └── ps_supplycost(1)
                ├── cost: 172614320
                ├── rows: 1250000
                └── HashAgg
                    ├── keys:
                    │   ┌── p_partkey
                    │   ├── p_name
                    │   ├── p_mfgr
                    │   ├── p_brand
                    │   ├── p_type
                    │   ├── p_size
                    │   ├── p_container
                    │   ├── p_retailprice
                    │   ├── p_comment
                    │   ├── s_suppkey
                    │   ├── s_name
                    │   ├── s_address
                    │   ├── s_nationkey
                    │   ├── s_phone
                    │   ├── s_acctbal
                    │   ├── s_comment
                    │   ├── ps_partkey
                    │   ├── ps_suppkey
                    │   ├── ps_availqty
                    │   ├── ps_supplycost
                    │   ├── ps_comment
                    │   ├── n_nationkey
                    │   ├── n_name
                    │   ├── n_regionkey
                    │   ├── n_comment
                    │   ├── r_regionkey
                    │   ├── r_name
                    │   └── r_comment
                    ├── aggs:min
                    │   └── ps_supplycost(1)
                    ├── cost: 172476820
                    ├── rows: 1250000
                    └── Projection
                        ├── exprs:
                        │   ┌── p_partkey
                        │   ├── p_name
                        │   ├── p_mfgr
                        │   ├── p_brand
                        │   ├── p_type
                        │   ├── p_size
                        │   ├── p_container
                        │   ├── p_retailprice
                        │   ├── p_comment
                        │   ├── s_suppkey
                        │   ├── s_name
                        │   ├── s_address
                        │   ├── s_nationkey
                        │   ├── s_phone
                        │   ├── s_acctbal
                        │   ├── s_comment
                        │   ├── ps_partkey
                        │   ├── ps_suppkey
                        │   ├── ps_availqty
                        │   ├── ps_supplycost
                        │   ├── ps_comment
                        │   ├── n_nationkey
                        │   ├── n_name
                        │   ├── n_regionkey
                        │   ├── n_comment
                        │   ├── r_regionkey
                        │   ├── r_name
                        │   ├── r_comment
                        │   └── ps_supplycost(1)
                        ├── cost: 135461150
                        ├── rows: 1250000
                        └── HashJoin
                            ├── type: left_outer
                            ├── on: = { lhs: [ p_partkey ], rhs: [ ps_partkey(1) ] }
                            ├── cost: 135086160
                            ├── rows: 1250000
                            ├── Projection
                            │   ├── exprs:
                            │   │   ┌── p_partkey
                            │   │   ├── p_name
                            │   │   ├── p_mfgr
                            │   │   ├── p_brand
                            │   │   ├── p_type
                            │   │   ├── p_size
                            │   │   ├── p_container
                            │   │   ├── p_retailprice
                            │   │   ├── p_comment
                            │   │   ├── s_suppkey
                            │   │   ├── s_name
                            │   │   ├── s_address
                            │   │   ├── s_nationkey
                            │   │   ├── s_phone
                            │   │   ├── s_acctbal
                            │   │   ├── s_comment
                            │   │   ├── ps_partkey
                            │   │   ├── ps_suppkey
                            │   │   ├── ps_availqty
                            │   │   ├── ps_supplycost
                            │   │   ├── ps_comment
                            │   │   ├── n_nationkey
                            │   │   ├── n_name
                            │   │   ├── n_regionkey
                            │   │   ├── n_comment
                            │   │   ├── r_regionkey
                            │   │   ├── r_name
                            │   │   └── r_comment
                            │   ├── cost: 91331120
                            │   ├── rows: 1250000
                            │   └── HashJoin
                            │       ├── type: inner
                            │       ├── on: = { lhs: [ p_partkey, s_suppkey ], rhs: [ ps_partkey, ps_suppkey ] }
                            │       ├── cost: 90968620
                            │       ├── rows: 1250000
                            │       ├── HashJoin
                            │       │   ├── type: inner
                            │       │   ├── on: = { lhs: [ s_nationkey ], rhs: [ n_nationkey ] }
                            │       │   ├── cost: 51491930
                            │       │   ├── rows: 1250000
                            │       │   ├── Scan
                            │       │   │   ├── table: supplier
                            │       │   │   ├── list:
                            │       │   │   │   ┌── s_suppkey
                            │       │   │   │   ├── s_name
                            │       │   │   │   ├── s_address
                            │       │   │   │   ├── s_nationkey
                            │       │   │   │   ├── s_phone
                            │       │   │   │   ├── s_acctbal
                            │       │   │   │   └── s_comment
                            │       │   │   ├── filter: null
                            │       │   │   ├── cost: 70000
                            │       │   │   └── rows: 10000
                            │       │   └── Join { type: inner, cost: 22479300, rows: 1250000 }
                            │       │       ├── HashJoin
                            │       │       │   ├── type: inner
                            │       │       │   ├── on: = { lhs: [ n_regionkey ], rhs: [ r_regionkey ] }
                            │       │       │   ├── cost: 300.3926
                            │       │       │   ├── rows: 25
                            │       │       │   ├── Scan
                            │       │       │   │   ├── table: nation
                            │       │       │   │   ├── list: [ n_nationkey, n_name, n_regionkey, n_comment ]
                            │       │       │   │   ├── filter: null
                            │       │       │   │   ├── cost: 100
                            │       │       │   │   └── rows: 25
                            │       │       │   └── Filter
                            │       │       │       ├── cond: = { lhs: 'EUROPE', rhs: r_name }
                            │       │       │       ├── cost: 23.55
                            │       │       │       ├── rows: 2.5
                            │       │       │       └── Scan
                            │       │       │           ├── table: region
                            │       │       │           ├── list: [ r_regionkey, r_name, r_comment ]
                            │       │       │           ├── filter: null
                            │       │       │           ├── cost: 15
                            │       │       │           └── rows: 5
                            │       │       └── Filter
                            │       │           ├── cond:and
                            │       │           │   ├── lhs: = { lhs: p_size, rhs: 15 }
                            │       │           │   └── rhs: like { lhs: p_type, rhs: '%BRASS' }
                            │       │           ├── cost: 2354000
                            │       │           ├── rows: 50000
                            │       │           └── Scan
                            │       │               ├── table: part
                            │       │               ├── list:
                            │       │               │   ┌── p_partkey
                            │       │               │   ├── p_name
                            │       │               │   ├── p_mfgr
                            │       │               │   ├── p_brand
                            │       │               │   ├── p_type
                            │       │               │   ├── p_size
                            │       │               │   ├── p_container
                            │       │               │   ├── p_retailprice
                            │       │               │   └── p_comment
                            │       │               ├── filter: null
                            │       │               ├── cost: 1800000
                            │       │               └── rows: 200000
                            │       └── Scan
                            │           ├── table: partsupp
                            │           ├── list: [ ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment ]
                            │           ├── filter: null
                            │           ├── cost: 4000000
                            │           └── rows: 800000
                            └── Projection { exprs: [ ps_partkey(1), ps_supplycost(1) ], cost: 5798846, rows: 800000 }
                                └── HashJoin
                                    ├── type: inner
                                    ├── on: = { lhs: [ s_suppkey(1) ], rhs: [ ps_suppkey(1) ] }
                                    ├── cost: 5774846
                                    ├── rows: 800000
                                    ├── Projection { exprs: [ s_suppkey(1) ], cost: 51014.367, rows: 10000 }
                                    │   └── HashJoin
                                    │       ├── type: inner
                                    │       ├── on: = { lhs: [ n_nationkey(1) ], rhs: [ s_nationkey(1) ] }
                                    │       ├── cost: 50814.367
                                    │       ├── rows: 10000
                                    │       ├── Projection { exprs: [ n_nationkey(1) ], cost: 142.64702, rows: 25 }
                                    │       │   └── HashJoin
                                    │       │       ├── type: inner
                                    │       │       ├── on: = { lhs: [ r_regionkey(1) ], rhs: [ n_regionkey(1) ] }
                                    │       │       ├── cost: 142.14702
                                    │       │       ├── rows: 25
                                    │       │       ├── Projection
                                    │       │       │   ├── exprs: [ r_regionkey(1) ]
                                    │       │       │   ├── cost: 16.099998
                                    │       │       │   ├── rows: 2.5
                                    │       │       │   └── Filter
                                    │       │       │       ├── cond: = { lhs: r_name(1), rhs: 'EUROPE' }
                                    │       │       │       ├── cost: 16.05
                                    │       │       │       ├── rows: 2.5
                                    │       │       │       └── Scan
                                    │       │       │           ├── table: region
                                    │       │       │           ├── list: [ r_regionkey(1), r_name(1) ]
                                    │       │       │           ├── filter: null
                                    │       │       │           ├── cost: 10
                                    │       │       │           └── rows: 5
                                    │       │       └── Scan
                                    │       │           ├── table: nation
                                    │       │           ├── list: [ n_nationkey(1), n_regionkey(1) ]
                                    │       │           ├── filter: null
                                    │       │           ├── cost: 50
                                    │       │           └── rows: 25
                                    │       └── Scan
                                    │           ├── table: supplier
                                    │           ├── list: [ s_suppkey(1), s_nationkey(1) ]
                                    │           ├── filter: null
                                    │           ├── cost: 20000
                                    │           └── rows: 10000
                                    └── Scan
                                        ├── table: partsupp
                                        ├── list: [ ps_partkey(1), ps_suppkey(1), ps_supplycost(1) ]
                                        ├── filter: null
                                        ├── cost: 2400000
                                        └── rows: 800000
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
│   ├── ref
│   │   └── sum
│   │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
│   ├── o_orderdate
│   └── o_shippriority
├── cost: 71823220
├── rows: 10
└── TopN
    ├── limit: 10
    ├── offset: 0
    ├── order_by:
    │   ┌── desc
    │   │   └── ref
    │   │       └── sum
    │   │           └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    │   └── o_orderdate
    ├── cost: 71823220
    ├── rows: 10
    └── HashAgg
        ├── keys: [ l_orderkey, o_orderdate, o_shippriority ]
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── cost: 71819720
        ├── rows: 1000
        └── Projection
            ├── exprs: [ o_orderdate, o_shippriority, l_orderkey, l_extendedprice, l_discount ]
            ├── cost: 70106360
            ├── rows: 3000607.5
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                ├── cost: 69926320
                ├── rows: 3000607.5
                ├── Projection { exprs: [ o_orderkey, o_orderdate, o_shippriority ], cost: 13728106, rows: 750000 }
                │   └── HashJoin
                │       ├── type: inner
                │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                │       ├── cost: 13698106
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

-- tpch-q4
explain select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
    o_orderdate >= date '1993-07-01'
    and o_orderdate < date '1993-07-01' + interval '3' month
    and exists (
        select
            *
        from
            lineitem
        where
            l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority;

/*
Projection
├── exprs:
│   ┌── o_orderpriority
│   └── ref
│       └── rowcount
├── cost: 35815010
├── rows: 10
└── Order { by: [ o_orderpriority ], cost: 35815010, rows: 10 }
    └── HashAgg { keys: [ o_orderpriority ], aggs: [ rowcount ], cost: 35814950, rows: 10 }
        └── Projection { exprs: [ o_orderpriority ], cost: 35753210, rows: 375000 }
            └── HashJoin
                ├── type: semi
                ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                ├── cost: 35745708
                ├── rows: 375000
                ├── Projection { exprs: [ o_orderkey, o_orderpriority ], cost: 6416250, rows: 375000 }
                │   └── Filter
                │       ├── cond:and
                │       │   ├── lhs: >= { lhs: o_orderdate, rhs: 1993-07-01 }
                │       │   └── rhs: > { lhs: 1993-10-01, rhs: o_orderdate }
                │       ├── cost: 6405000
                │       ├── rows: 375000
                │       └── Scan
                │           ├── table: orders
                │           ├── list: [ o_orderkey, o_orderdate, o_orderpriority ]
                │           ├── filter: null
                │           ├── cost: 4500000
                │           └── rows: 1500000
                └── Projection { exprs: [ l_orderkey ], cost: 27785624, rows: 3000607.5 }
                    └── Filter { cond: > { lhs: l_receiptdate, rhs: l_commitdate }, cost: 27725612, rows: 3000607.5 }
                        └── Scan
                            ├── table: lineitem
                            ├── list: [ l_orderkey, l_commitdate, l_receiptdate ]
                            ├── filter: null
                            ├── cost: 18003644
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
│   └── ref
│       └── sum
│           └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
├── cost: 116165816
├── rows: 10
└── Order
    ├── by:desc
    │   └── ref
    │       └── sum
    │           └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    ├── cost: 116165816
    ├── rows: 10
    └── HashAgg
        ├── keys: [ n_name ]
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── cost: 116165760
        ├── rows: 10
        └── Projection { exprs: [ l_extendedprice, l_discount, n_name ], cost: 113257580, rows: 6001215 }
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ s_suppkey, s_nationkey ], rhs: [ l_suppkey, c_nationkey ] }
                ├── cost: 113017540
                ├── rows: 6001215
                ├── Projection { exprs: [ s_suppkey, s_nationkey, n_name ], cost: 81264.66, rows: 10000 }
                │   └── HashJoin
                │       ├── type: inner
                │       ├── on: = { lhs: [ n_nationkey ], rhs: [ s_nationkey ] }
                │       ├── cost: 80864.66
                │       ├── rows: 10000
                │       ├── HashJoin
                │       │   ├── type: inner
                │       │   ├── on: = { lhs: [ n_regionkey ], rhs: [ r_regionkey ] }
                │       │   ├── cost: 192.94263
                │       │   ├── rows: 25
                │       │   ├── Scan
                │       │   │   ├── table: nation
                │       │   │   ├── list: [ n_nationkey, n_name, n_regionkey ]
                │       │   │   ├── filter: null
                │       │   │   ├── cost: 75
                │       │   │   └── rows: 25
                │       │   └── Projection { exprs: [ r_regionkey ], cost: 16.099998, rows: 2.5 }
                │       │       └── Filter { cond: = { lhs: r_name, rhs: 'AFRICA' }, cost: 16.05, rows: 2.5 }
                │       │           └── Scan
                │       │               ├── table: region
                │       │               ├── list: [ r_regionkey, r_name ]
                │       │               ├── filter: null
                │       │               ├── cost: 10
                │       │               └── rows: 5
                │       └── Scan
                │           ├── table: supplier
                │           ├── list: [ s_suppkey, s_nationkey ]
                │           ├── filter: null
                │           ├── cost: 20000
                │           └── rows: 10000
                └── Projection
                    ├── exprs: [ c_nationkey, l_suppkey, l_extendedprice, l_discount ]
                    ├── cost: 69948664
                    ├── rows: 6001215
                    └── HashJoin
                        ├── type: inner
                        ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                        ├── cost: 69648600
                        ├── rows: 6001215
                        ├── Projection { exprs: [ c_nationkey, o_orderkey ], cost: 8328272, rows: 375000 }
                        │   └── HashJoin
                        │       ├── type: inner
                        │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                        │       ├── cost: 8317022
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
                        │               │   ├── lhs: > { lhs: 1995-01-01, rhs: o_orderdate }
                        │               │   └── rhs: >= { lhs: o_orderdate, rhs: 1994-01-01 }
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
├── exprs:ref
│   └── sum
│       └── * { lhs: l_discount, rhs: l_extendedprice }
├── cost: 32817268
├── rows: 1
└── Agg
    ├── aggs:sum
    │   └── * { lhs: l_discount, rhs: l_extendedprice }
    ├── cost: 32817268
    ├── rows: 1
    └── Filter
        ├── cond: and { lhs: >= { lhs: 0.09, rhs: l_discount }, rhs: >= { lhs: l_discount, rhs: 0.07 } }
        ├── cost: 32774134
        ├── rows: 187537.97
        └── Projection { exprs: [ l_extendedprice, l_discount ], cost: 32008980, rows: 750151.9 }
            └── Filter
                ├── cond:and
                │   ├── lhs: > { lhs: 24, rhs: l_quantity }
                │   └── rhs:and
                │       ├── lhs: > { lhs: 1995-01-01, rhs: l_shipdate }
                │       └── rhs: >= { lhs: l_shipdate, rhs: 1994-01-01 }
                ├── cost: 31986476
                ├── rows: 750151.9
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
│   ├── ref
│   │   └── Extract { from: l_shipdate, field: YEAR }
│   └── ref
│       └── sum
│           └── ref
│               └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
├── cost: 80557304
├── rows: 1000
└── Order
    ├── by:
    │   ┌── n_name
    │   ├── n_name(1)
    │   └── ref
    │       └── Extract { from: l_shipdate, field: YEAR }
    ├── cost: 80557256
    ├── rows: 1000
    └── HashAgg
        ├── keys:
        │   ┌── n_name
        │   ├── n_name(1)
        │   └── ref
        │       └── Extract { from: l_shipdate, field: YEAR }
        ├── aggs:sum
        │   └── ref
        │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── cost: 80543290
        ├── rows: 1000
        └── Projection
            ├── exprs:
            │   ┌── n_name
            │   ├── n_name(1)
            │   ├── Extract { from: l_shipdate, field: YEAR }
            │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
            ├── cost: 80149704
            ├── rows: 1500303.8
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ n_nationkey, o_orderkey ], rhs: [ s_nationkey, l_orderkey ] }
                ├── cost: 79309540
                ├── rows: 1500303.8
                ├── Projection { exprs: [ o_orderkey, n_nationkey, n_name, n_name(1) ], cost: 13629962, rows: 1500000 }
                │   └── HashJoin
                │       ├── type: inner
                │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                │       ├── cost: 13554962
                │       ├── rows: 1500000
                │       ├── Projection
                │       │   ├── exprs: [ c_custkey, n_nationkey, n_name, n_name(1) ]
                │       │   ├── cost: 1238250.6
                │       │   ├── rows: 150000
                │       │   └── HashJoin
                │       │       ├── type: inner
                │       │       ├── on: = { lhs: [ c_nationkey ], rhs: [ n_nationkey(1) ] }
                │       │       ├── cost: 1230750.6
                │       │       ├── rows: 150000
                │       │       ├── Scan
                │       │       │   ├── table: customer
                │       │       │   ├── list: [ c_custkey, c_nationkey ]
                │       │       │   ├── filter: null
                │       │       │   ├── cost: 300000
                │       │       │   └── rows: 150000
                │       │       └── Join
                │       │           ├── type: inner
                │       │           ├── on:or
                │       │           │   ├── lhs:and
                │       │           │   │   ├── lhs: = { lhs: n_name, rhs: 'GERMANY' }
                │       │           │   │   └── rhs: = { lhs: n_name(1), rhs: 'FRANCE' }
                │       │           │   └── rhs:and
                │       │           │       ├── lhs: = { lhs: n_name, rhs: 'FRANCE' }
                │       │           │       └── rhs: = { lhs: n_name(1), rhs: 'GERMANY' }
                │       │           ├── cost: 1906.25
                │       │           ├── rows: 273.4375
                │       │           ├── Scan
                │       │           │   ├── table: nation
                │       │           │   ├── list: [ n_nationkey(1), n_name(1) ]
                │       │           │   ├── filter: null
                │       │           │   ├── cost: 50
                │       │           │   └── rows: 25
                │       │           └── Scan
                │       │               ├── table: nation
                │       │               ├── list: [ n_nationkey, n_name ]
                │       │               ├── filter: null
                │       │               ├── cost: 50
                │       │               └── rows: 25
                │       └── Scan
                │           ├── table: orders
                │           ├── list: [ o_orderkey, o_custkey ]
                │           ├── filter: null
                │           ├── cost: 3000000
                │           └── rows: 1500000
                └── Projection
                    ├── exprs: [ s_nationkey, l_orderkey, l_extendedprice, l_discount, l_shipdate ]
                    ├── cost: 51471268
                    ├── rows: 1500303.8
                    └── HashJoin
                        ├── type: inner
                        ├── on: = { lhs: [ s_suppkey ], rhs: [ l_suppkey ] }
                        ├── cost: 51381250
                        ├── rows: 1500303.8
                        ├── Scan
                        │   ├── table: supplier
                        │   ├── list: [ s_suppkey, s_nationkey ]
                        │   ├── filter: null
                        │   ├── cost: 20000
                        │   └── rows: 10000
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
│   ┌── ref
│   │   └── Extract { from: o_orderdate, field: YEAR }
│   └── /
│       ├── lhs:ref
│       │   └── sum
│       │       └── If
│       │           ├── cond: = { lhs: n_name(1), rhs: 'BRAZIL' }
│       │           ├── then:ref
│       │           │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
│       │           ├── else:Cast { type: 0 }
│       │           │   └── DECIMAL(30,4)

│       ├── rhs:ref
│       │   └── sum
│       │       └── ref
│       │           └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }

├── cost: 227191060
├── rows: 10
└── Order
    ├── by:ref
    │   └── Extract { from: o_orderdate, field: YEAR }
    ├── cost: 227191060
    ├── rows: 10
    └── HashAgg
        ├── keys:ref
        │   └── Extract { from: o_orderdate, field: YEAR }
        ├── aggs:
        │   ┌── sum
        │   │   └── If
        │   │       ├── cond: = { lhs: n_name(1), rhs: 'BRAZIL' }
        │   │       ├── then:ref
        │   │       │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        │   │       ├── else:Cast { type: 0 }
        │   │       │   └── DECIMAL(30,4)

        │   └── sum
        │       └── ref
        │           └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── cost: 227190990
        ├── rows: 10
        └── Projection
            ├── exprs:
            │   ┌── Extract { from: o_orderdate, field: YEAR }
            │   ├── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
            │   └── n_name(1)
            ├── cost: 221822300
            ├── rows: 6001215
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ n_nationkey(1), n_nationkey ], rhs: [ s_nationkey, c_nationkey ] }
                ├── cost: 218521630
                ├── rows: 6001215
                ├── Join { type: inner, cost: 2130.147, rows: 625 }
                │   ├── Projection { exprs: [ n_nationkey ], cost: 142.64702, rows: 25 }
                │   │   └── HashJoin
                │   │       ├── type: inner
                │   │       ├── on: = { lhs: [ r_regionkey ], rhs: [ n_regionkey ] }
                │   │       ├── cost: 142.14702
                │   │       ├── rows: 25
                │   │       ├── Projection { exprs: [ r_regionkey ], cost: 16.099998, rows: 2.5 }
                │   │       │   └── Filter { cond: = { lhs: r_name, rhs: 'AMERICA' }, cost: 16.05, rows: 2.5 }
                │   │       │       └── Scan
                │   │       │           ├── table: region
                │   │       │           ├── list: [ r_regionkey, r_name ]
                │   │       │           ├── filter: null
                │   │       │           ├── cost: 10
                │   │       │           └── rows: 5
                │   │       └── Scan
                │   │           ├── table: nation
                │   │           ├── list: [ n_nationkey, n_regionkey ]
                │   │           ├── filter: null
                │   │           ├── cost: 50
                │   │           └── rows: 25
                │   └── Scan { table: nation, list: [ n_nationkey(1), n_name(1) ], filter: null, cost: 50, rows: 25 }
                └── Projection
                    ├── exprs: [ s_nationkey, l_extendedprice, l_discount, o_orderdate, c_nationkey ]
                    ├── cost: 169772160
                    ├── rows: 6001215
                    └── HashJoin
                        ├── type: inner
                        ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                        ├── cost: 169412080
                        ├── rows: 6001215
                        ├── Projection { exprs: [ o_orderkey, o_orderdate, c_nationkey ], cost: 8695772, rows: 375000 }
                        │   └── HashJoin
                        │       ├── type: inner
                        │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                        │       ├── cost: 8680772
                        │       ├── rows: 375000
                        │       ├── Scan
                        │       │   ├── table: customer
                        │       │   ├── list: [ c_custkey, c_nationkey ]
                        │       │   ├── filter: null
                        │       │   ├── cost: 300000
                        │       │   └── rows: 150000
                        │       └── Filter
                        │           ├── cond:and
                        │           │   ├── lhs: >= { lhs: o_orderdate, rhs: 1995-01-01 }
                        │           │   └── rhs: >= { lhs: 1996-12-31, rhs: o_orderdate }
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
                            ├── cost: 117399624
                            ├── rows: 6001215
                            └── HashJoin
                                ├── type: inner
                                ├── on: = { lhs: [ p_partkey ], rhs: [ l_partkey ] }
                                ├── cost: 117099560
                                ├── rows: 6001215
                                ├── Filter
                                │   ├── cond: = { lhs: p_type, rhs: 'ECONOMY ANODIZED STEEL' }
                                │   ├── cost: 642000
                                │   ├── rows: 100000
                                │   └── Scan
                                │       ├── table: part
                                │       ├── list: [ p_partkey, p_type ]
                                │       ├── filter: null
                                │       ├── cost: 400000
                                │       └── rows: 200000
                                └── Projection
                                    ├── exprs: [ s_nationkey, l_orderkey, l_partkey, l_extendedprice, l_discount ]
                                    ├── cost: 73313640
                                    ├── rows: 6001215
                                    └── HashJoin
                                        ├── type: inner
                                        ├── on: = { lhs: [ s_suppkey ], rhs: [ l_suppkey ] }
                                        ├── cost: 72953570
                                        ├── rows: 6001215
                                        ├── Scan
                                        │   ├── table: supplier
                                        │   ├── list: [ s_suppkey, s_nationkey ]
                                        │   ├── filter: null
                                        │   ├── cost: 20000
                                        │   └── rows: 10000
                                        └── Scan
                                            ├── table: lineitem
                                            ├── list: [ l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount ]
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
│   ├── ref
│   │   └── Extract { from: o_orderdate, field: YEAR }
│   └── ref
│       └── sum
│           └── ref
│               └── -
│                   ├── lhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
│                   └── rhs: * { lhs: ps_supplycost, rhs: l_quantity }
├── cost: 264678370
├── rows: 100
└── Order
    ├── by:
    │   ┌── n_name
    │   └── desc
    │       └── ref
    │           └── Extract { from: o_orderdate, field: YEAR }
    ├── cost: 264678370
    ├── rows: 100
    └── HashAgg
        ├── keys:
        │   ┌── n_name
        │   └── ref
        │       └── Extract { from: o_orderdate, field: YEAR }
        ├── aggs:sum
        │   └── ref
        │       └── -
        │           ├── lhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        │           └── rhs: * { lhs: ps_supplycost, rhs: l_quantity }
        ├── cost: 264677400
        ├── rows: 100
        └── Projection
            ├── exprs:
            │   ┌── n_name
            │   ├── Extract { from: o_orderdate, field: YEAR }
            │   └── -
            │       ├── lhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
            │       └── rhs: * { lhs: ps_supplycost, rhs: l_quantity }
            ├── cost: 263377360
            ├── rows: 6001215
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                ├── cost: 258756430
                ├── rows: 6001215
                ├── Scan
                │   ├── table: orders
                │   ├── list: [ o_orderkey, o_orderdate ]
                │   ├── filter: null
                │   ├── cost: 3000000
                │   └── rows: 1500000
                └── Projection
                    ├── exprs: [ l_orderkey, l_quantity, l_extendedprice, l_discount, ps_supplycost, n_name ]
                    ├── cost: 206057700
                    ├── rows: 6001215
                    └── HashJoin
                        ├── type: inner
                        ├── on: = { lhs: [ s_suppkey ], rhs: [ l_suppkey ] }
                        ├── cost: 205637620
                        ├── rows: 6001215
                        ├── Projection
                        │   ├── exprs: [ s_suppkey, s_nationkey, n_nationkey, n_name ]
                        │   ├── cost: 61221.72
                        │   ├── rows: 10000
                        │   └── HashJoin
                        │       ├── type: inner
                        │       ├── on: = { lhs: [ n_nationkey ], rhs: [ s_nationkey ] }
                        │       ├── cost: 60721.72
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
                            ├── exprs: [ l_orderkey, l_suppkey, l_quantity, l_extendedprice, l_discount, ps_supplycost ]
                            ├── cost: 144645260
                            ├── rows: 6001215
                            └── HashJoin
                                ├── type: inner
                                ├── on: = { lhs: [ p_partkey ], rhs: [ l_partkey ] }
                                ├── cost: 144225180
                                ├── rows: 6001215
                                ├── Projection { exprs: [ p_partkey ], cost: 644000, rows: 100000 }
                                │   └── Filter
                                │       ├── cond: like { lhs: p_name, rhs: '%green%' }
                                │       ├── cost: 642000
                                │       ├── rows: 100000
                                │       └── Scan
                                │           ├── table: part
                                │           ├── list: [ p_partkey, p_name ]
                                │           ├── filter: null
                                │           ├── cost: 400000
                                │           └── rows: 200000
                                └── Projection
                                    ├── exprs:
                                    │   ┌── l_orderkey
                                    │   ├── l_partkey
                                    │   ├── l_suppkey
                                    │   ├── l_quantity
                                    │   ├── l_extendedprice
                                    │   ├── l_discount
                                    │   └── ps_supplycost
                                    ├── cost: 94436050
                                    ├── rows: 6001215
                                    └── HashJoin
                                        ├── type: inner
                                        ├── on: = { lhs: [ ps_suppkey, ps_partkey ], rhs: [ l_suppkey, l_partkey ] }
                                        ├── cost: 93955950
                                        ├── rows: 6001215
                                        ├── Scan
                                        │   ├── table: partsupp
                                        │   ├── list: [ ps_partkey, ps_suppkey, ps_supplycost ]
                                        │   ├── filter: null
                                        │   ├── cost: 2400000
                                        │   └── rows: 800000
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
│   ├── ref
│   │   └── sum
│   │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
│   ├── c_acctbal
│   ├── n_name
│   ├── c_address
│   ├── c_phone
│   └── c_comment
├── cost: 123095330
├── rows: 20
└── TopN
    ├── limit: 20
    ├── offset: 0
    ├── order_by:desc
    │   └── ref
    │       └── sum
    │           └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    ├── cost: 123095330
    ├── rows: 20
    └── HashAgg
        ├── keys: [ c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ]
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── cost: 109915550
        ├── rows: 3000607.5
        └── Projection
            ├── exprs:
            │   ┌── c_custkey
            │   ├── c_name
            │   ├── c_address
            │   ├── c_phone
            │   ├── c_acctbal
            │   ├── c_comment
            │   ├── l_extendedprice
            │   ├── l_discount
            │   └── n_name
            ├── cost: 83734744
            ├── rows: 3000607.5
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                ├── cost: 83434680
                ├── rows: 3000607.5
                ├── Projection
                │   ├── exprs: [ c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, o_orderkey, n_name ]
                │   ├── cost: 12347874
                │   ├── rows: 375000
                │   └── HashJoin
                │       ├── type: inner
                │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                │       ├── cost: 12314124
                │       ├── rows: 375000
                │       ├── Projection
                │       │   ├── exprs: [ c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, n_name ]
                │       │   ├── cost: 2422102.5
                │       │   ├── rows: 150000
                │       │   └── HashJoin
                │       │       ├── type: inner
                │       │       ├── on: = { lhs: [ n_nationkey ], rhs: [ c_nationkey ] }
                │       │       ├── cost: 2410102.5
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

-- tpch-q11
explain select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
group by
    ps_partkey having
        sum(ps_supplycost * ps_availqty) > (
            select
                sum(ps_supplycost * ps_availqty) * 0.0001000000
            from
                partsupp,
                supplier,
                nation
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'GERMANY'
        )
order by
    value desc;

/*
Order
├── by:desc
│   └── ref
│       └── sum
│           └── * { lhs: ps_supplycost, rhs: ps_availqty }
├── cost: 13617143
├── rows: 5
└── Projection
    ├── exprs:
    │   ┌── ps_partkey
    │   └── ref
    │       └── sum
    │           └── * { lhs: ps_supplycost, rhs: ps_availqty }
    ├── cost: 13617120
    ├── rows: 5
    └── Filter
        ├── cond:>
        │   ├── lhs:ref
        │   │   └── sum
        │   │       └── * { lhs: ps_supplycost, rhs: ps_availqty }
        │   ├── rhs:ref
        │   │   └── *
        │   │       ├── lhs:ref
        │   │       │   └── sum
        │   │       │       └── * { lhs: ps_supplycost(1), rhs: ps_availqty(1) }
        │   │       ├── rhs: 0.0001000000


        ├── cost: 13617120
        ├── rows: 5
        └── Join { type: left_outer, cost: 13617104, rows: 10 }
            ├── HashAgg
            │   ├── keys: [ ps_partkey ]
            │   ├── aggs:sum
            │   │   └── * { lhs: ps_supplycost, rhs: ps_availqty }
            │   ├── cost: 7634384
            │   ├── rows: 10
            │   └── Projection { exprs: [ ps_partkey, ps_availqty, ps_supplycost ], cost: 7406688.5, rows: 800000 }
            │       └── HashJoin
            │           ├── type: inner
            │           ├── on: = { lhs: [ s_suppkey ], rhs: [ ps_suppkey ] }
            │           ├── cost: 7374688.5
            │           ├── rows: 800000
            │           ├── Projection { exprs: [ s_suppkey ], cost: 50856.71, rows: 10000 }
            │           │   └── HashJoin
            │           │       ├── type: inner
            │           │       ├── on: = { lhs: [ n_nationkey ], rhs: [ s_nationkey ] }
            │           │       ├── cost: 50656.71
            │           │       ├── rows: 10000
            │           │       ├── Projection { exprs: [ n_nationkey ], cost: 80.5, rows: 12.5 }
            │           │       │   └── Filter { cond: = { lhs: 'GERMANY', rhs: n_name }, cost: 80.25, rows: 12.5 }
            │           │       │       └── Scan
            │           │       │           ├── table: nation
            │           │       │           ├── list: [ n_nationkey, n_name ]
            │           │       │           ├── filter: null
            │           │       │           ├── cost: 50
            │           │       │           └── rows: 25
            │           │       └── Scan
            │           │           ├── table: supplier
            │           │           ├── list: [ s_suppkey, s_nationkey ]
            │           │           ├── filter: null
            │           │           ├── cost: 20000
            │           │           └── rows: 10000
            │           └── Scan
            │               ├── table: partsupp
            │               ├── list: [ ps_partkey, ps_suppkey, ps_availqty, ps_supplycost ]
            │               ├── filter: null
            │               ├── cost: 3200000
            │               └── rows: 800000
            └── Projection
                ├── exprs:*
                │   ├── lhs:ref
                │   │   └── sum
                │   │       └── * { lhs: ps_supplycost(1), rhs: ps_availqty(1) }
                │   ├── rhs: 0.0001000000

                ├── cost: 5982689.5
                ├── rows: 1
                └── Agg
                    ├── aggs:sum
                    │   └── * { lhs: ps_supplycost(1), rhs: ps_availqty(1) }
                    ├── cost: 5982689.5
                    ├── rows: 1
                    └── Projection { exprs: [ ps_availqty(1), ps_supplycost(1) ], cost: 5798688.5, rows: 800000 }
                        └── HashJoin
                            ├── type: inner
                            ├── on: = { lhs: [ s_suppkey(1) ], rhs: [ ps_suppkey(1) ] }
                            ├── cost: 5774688.5
                            ├── rows: 800000
                            ├── Projection { exprs: [ s_suppkey(1) ], cost: 50856.71, rows: 10000 }
                            │   └── HashJoin
                            │       ├── type: inner
                            │       ├── on: = { lhs: [ n_nationkey(1) ], rhs: [ s_nationkey(1) ] }
                            │       ├── cost: 50656.71
                            │       ├── rows: 10000
                            │       ├── Projection { exprs: [ n_nationkey(1) ], cost: 80.5, rows: 12.5 }
                            │       │   └── Filter
                            │       │       ├── cond: = { lhs: n_name(1), rhs: 'GERMANY' }
                            │       │       ├── cost: 80.25
                            │       │       ├── rows: 12.5
                            │       │       └── Scan
                            │       │           ├── table: nation
                            │       │           ├── list: [ n_nationkey(1), n_name(1) ]
                            │       │           ├── filter: null
                            │       │           ├── cost: 50
                            │       │           └── rows: 25
                            │       └── Scan
                            │           ├── table: supplier
                            │           ├── list: [ s_suppkey(1), s_nationkey(1) ]
                            │           ├── filter: null
                            │           ├── cost: 20000
                            │           └── rows: 10000
                            └── Scan
                                ├── table: partsupp
                                ├── list: [ ps_suppkey(1), ps_availqty(1), ps_supplycost(1) ]
                                ├── filter: null
                                ├── cost: 2400000
                                └── rows: 800000
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
│   ├── ref
│   │   └── sum
│   │       └── If
│   │           ├── cond:or
│   │           │   ├── lhs: = { lhs: o_orderpriority, rhs: '2-HIGH' }
│   │           │   └── rhs: = { lhs: o_orderpriority, rhs: '1-URGENT' }
│   │           ├── then: 1
│   │           └── else: 0
│   └── ref
│       └── sum
│           └── If
│               ├── cond:and
│               │   ├── lhs: <> { lhs: o_orderpriority, rhs: '2-HIGH' }
│               │   └── rhs: <> { lhs: o_orderpriority, rhs: '1-URGENT' }
│               ├── then: 1
│               └── else: 0
├── cost: 50810744
├── rows: 10
└── Order { by: [ l_shipmode ], cost: 50810744, rows: 10 }
    └── HashAgg
        ├── keys: [ l_shipmode ]
        ├── aggs:
        │   ┌── sum
        │   │   └── If
        │   │       ├── cond:or
        │   │       │   ├── lhs: = { lhs: o_orderpriority, rhs: '2-HIGH' }
        │   │       │   └── rhs: = { lhs: o_orderpriority, rhs: '1-URGENT' }
        │   │       ├── then: 1
        │   │       └── else: 0
        │   └── sum
        │       └── If
        │           ├── cond:and
        │           │   ├── lhs: <> { lhs: o_orderpriority, rhs: '2-HIGH' }
        │           │   └── rhs: <> { lhs: o_orderpriority, rhs: '1-URGENT' }
        │           ├── then: 1
        │           └── else: 0
        ├── cost: 50810680
        ├── rows: 10
        └── Projection { exprs: [ o_orderpriority, l_shipmode ], cost: 47953760, rows: 1500000 }
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ l_orderkey ], rhs: [ o_orderkey ] }
                ├── cost: 47908760
                ├── rows: 1500000
                ├── Filter
                │   ├── cond:In { in: [ 'MAIL', 'SHIP' ] }
                │   │   └── l_shipmode
                │   ├── cost: 38524052
                │   ├── rows: 375075.94
                │   └── Projection { exprs: [ l_orderkey, l_shipmode ], cost: 37653876, rows: 375075.94 }
                │       └── Filter
                │           ├── cond:and
                │           │   ├── lhs: > { lhs: 1995-01-01, rhs: l_receiptdate }
                │           │   └── rhs:and
                │           │       ├── lhs: > { lhs: l_commitdate, rhs: l_shipdate }
                │           │       └── rhs:and
                │           │           ├── lhs: > { lhs: l_receiptdate, rhs: l_commitdate }
                │           │           └── rhs: >= { lhs: l_receiptdate, rhs: 1994-01-01 }
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

-- tpch-q13
explain select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            customer left outer join orders on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc;

/*
Projection
├── exprs:
│   ┌── ref
│   │   └── count
│   │       └── o_orderkey
│   └── ref
│       └── rowcount
├── cost: 9963795
├── rows: 10
└── Order
    ├── by:
    │   ┌── desc
    │   │   └── ref
    │   │       └── rowcount
    │   └── desc
    │       └── ref
    │           └── count
    │               └── o_orderkey
    ├── cost: 9963795
    ├── rows: 10
    └── HashAgg
        ├── keys:ref
        │   └── count
        │       └── o_orderkey
        ├── aggs: [ rowcount ]
        ├── cost: 9963740
        ├── rows: 10
        └── Projection
            ├── exprs:
            │   ┌── c_custkey
            │   └── ref
            │       └── count
            │           └── o_orderkey
            ├── cost: 9963718
            ├── rows: 10
            └── HashAgg
                ├── keys: [ c_custkey ]
                ├── aggs:count
                │   └── o_orderkey
                ├── cost: 9963718
                ├── rows: 10
                └── Projection { exprs: [ c_custkey, o_orderkey ], cost: 9832752, rows: 750000 }
                    └── HashJoin
                        ├── type: left_outer
                        ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                        ├── cost: 9810252
                        ├── rows: 750000
                        ├── Scan { table: customer, list: [ c_custkey ], filter: null, cost: 150000, rows: 150000 }
                        └── Projection { exprs: [ o_orderkey, o_custkey ], cost: 7237500, rows: 750000 }
                            └── Filter
                                ├── cond:not
                                │   └── like { lhs: o_comment, rhs: '%special%requests%' }
                                ├── cost: 7215000
                                ├── rows: 750000
                                └── Scan
                                    ├── table: orders
                                    ├── list: [ o_orderkey, o_custkey, o_comment ]
                                    ├── filter: null
                                    ├── cost: 4500000
                                    └── rows: 1500000
*/

-- tpch-q14
explain select
    100.00 * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    lineitem,
    part
where
    l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month;

/*
Projection
├── exprs:/
│   ├── lhs:*
│   │   ├── lhs: 100.00
│   │   ├── rhs:ref
│   │   │   └── sum
│   │   │       └── If
│   │   │           ├── cond: like { lhs: p_type, rhs: 'PROMO%' }
│   │   │           ├── then: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
│   │   │           ├── else:Cast { type: 0 }
│   │   │           │   └── DECIMAL(30,4)


│   ├── rhs:ref
│   │   └── sum
│   │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }

├── cost: 43672120
├── rows: 1
└── Agg
    ├── aggs:
    │   ┌── sum
    │   │   └── If
    │   │       ├── cond: like { lhs: p_type, rhs: 'PROMO%' }
    │   │       ├── then: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    │   │       ├── else:Cast { type: 0 }
    │   │       │   └── DECIMAL(30,4)

    │   └── sum
    │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    ├── cost: 43672120
    ├── rows: 1
    └── Projection { exprs: [ l_extendedprice, l_discount, p_type ], cost: 41481676, rows: 1500303.8 }
        └── HashJoin { type: inner, on: = { lhs: [ p_partkey ], rhs: [ l_partkey ] }, cost: 41421664, rows: 1500303.8 }
            ├── Scan { table: part, list: [ p_partkey, p_type ], filter: null, cost: 400000, rows: 200000 }
            └── Projection { exprs: [ l_partkey, l_extendedprice, l_discount ], cost: 33186720, rows: 1500303.8 }
                └── Filter
                    ├── cond:and
                    │   ├── lhs: >= { lhs: l_shipdate, rhs: 1995-09-01 }
                    │   └── rhs: > { lhs: 1995-10-01, rhs: l_shipdate }
                    ├── cost: 33126708
                    ├── rows: 1500303.8
                    └── Scan
                        ├── table: lineitem
                        ├── list: [ l_partkey, l_extendedprice, l_discount, l_shipdate ]
                        ├── filter: null
                        ├── cost: 24004860
                        └── rows: 6001215
*/

-- tpch-q15
create view revenue0 (supplier_no, total_revenue) as
    select
        l_suppkey,
        sum(l_extendedprice * (1 - l_discount))
    from
        lineitem
    where
        l_shipdate >= date '1996-01-01'
        and l_shipdate < date '1996-01-01' + interval '3' month
    group by
        l_suppkey;

explain select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    revenue0
where
    s_suppkey = supplier_no
    and total_revenue = (
        select
            max(total_revenue)
        from
            revenue0
    )
order by
    s_suppkey;

/*
1Order { by: [ s_suppkey ], cost: 283977.4, rows: 5000 }
└── Projection { exprs: [ s_suppkey, s_name, s_address, s_phone, total_revenue ], cost: 197537.4, rows: 5000 }
    └── Filter
        ├── cond:=
        │   ├── lhs:ref
        │   │   └── max
        │   │       └── total_revenue(1)
        │   ├── rhs: total_revenue

        ├── cost: 197237.4
        ├── rows: 5000
        └── Join { type: left_outer, cost: 166037.4, rows: 10000 }
            ├── Projection
            │   ├── exprs: [ s_suppkey, s_name, s_address, s_phone, total_revenue ]
            │   ├── cost: 103916.39
            │   ├── rows: 10000
            │   └── HashJoin
            │       ├── type: inner
            │       ├── on: = { lhs: [ supplier_no ], rhs: [ s_suppkey ] }
            │       ├── cost: 103316.39
            │       ├── rows: 10000
            │       ├── Scan
            │       │   ├── table: revenue0
            │       │   ├── list: [ supplier_no, total_revenue ]
            │       │   ├── filter: null
            │       │   ├── cost: 2000
            │       │   └── rows: 1000
            │       └── Scan
            │           ├── table: supplier
            │           ├── list: [ s_suppkey, s_name, s_address, s_phone ]
            │           ├── filter: null
            │           ├── cost: 40000
            │           └── rows: 10000
            └── Projection
                ├── exprs:ref
                │   └── max
                │       └── total_revenue(1)
                ├── cost: 1121.02
                ├── rows: 1
                └── Agg
                    ├── aggs:max
                    │   └── total_revenue(1)
                    ├── cost: 1121
                    ├── rows: 1
                    └── Scan { table: revenue0, list: [ total_revenue(1) ], filter: null, cost: 1000, rows: 1000 }
*/

-- tpch-q16
explain select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
    p_partkey = ps_partkey
    and p_brand <> 'Brand#45'
    and p_type not like 'MEDIUM POLISHED%'
    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
    and ps_suppkey not in (
        select
            s_suppkey
        from
            supplier
        where
            s_comment like '%Customer%Complaints%'
    )
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size;

/*
Projection
├── exprs:
│   ┌── p_brand
│   ├── p_type
│   ├── p_size
│   └── ref
│       └── count-distinct
│           └── ps_suppkey
├── cost: 11490655
├── rows: 1000
└── Order
    ├── by:
    │   ┌── desc
    │   │   └── ref
    │   │       └── count-distinct
    │   │           └── ps_suppkey
    │   ├── p_brand
    │   ├── p_type
    │   └── p_size
    ├── cost: 11490605
    ├── rows: 1000
    └── HashAgg
        ├── keys: [ p_brand, p_type, p_size ]
        ├── aggs:count-distinct
        │   └── ps_suppkey
        ├── cost: 11476638
        ├── rows: 1000
        └── HashJoin { type: anti, on: = { lhs: [ ps_suppkey ], rhs: [ s_suppkey ] }, cost: 11264900, rows: 800000 }
            ├── Projection { exprs: [ ps_suppkey, p_brand, p_type, p_size ], cost: 7917682, rows: 800000 }
            │   └── HashJoin
            │       ├── type: inner
            │       ├── on: = { lhs: [ p_partkey ], rhs: [ ps_partkey ] }
            │       ├── cost: 7877682
            │       ├── rows: 800000
            │       ├── Filter
            │       │   ├── cond:and
            │       │   │   ├── lhs:not
            │       │   │   │   └── like { lhs: p_type, rhs: 'MEDIUM POLISHED%' }
            │       │   │   ├── rhs:and
            │       │   │   │   ├── lhs: <> { lhs: p_brand, rhs: 'Brand#45' }
            │       │   │   │   ├── rhs:In { in: [ 49, 14, 23, 45, 19, 3, 36, 9 ] }
            │       │   │   │   │   └── p_size


            │       │   ├── cost: 1328000
            │       │   ├── rows: 50000
            │       │   └── Scan
            │       │       ├── table: part
            │       │       ├── list: [ p_partkey, p_brand, p_type, p_size ]
            │       │       ├── filter: null
            │       │       ├── cost: 800000
            │       │       └── rows: 200000
            │       └── Scan
            │           ├── table: partsupp
            │           ├── list: [ ps_partkey, ps_suppkey ]
            │           ├── filter: null
            │           ├── cost: 1600000
            │           └── rows: 800000
            └── Projection { exprs: [ s_suppkey ], cost: 32200, rows: 5000 }
                └── Filter { cond: like { lhs: s_comment, rhs: '%Customer%Complaints%' }, cost: 32100, rows: 5000 }
                    └── Scan { table: supplier, list: [ s_suppkey, s_comment ], filter: null, cost: 20000, rows: 10000 }
*/

-- tpch-q17
explain select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    part
where
    p_partkey = l_partkey
    and p_brand = 'Brand#23'
    and p_container = 'MED BOX'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    );

/*
Projection
├── exprs:/
│   ├── lhs:ref
│   │   └── sum
│   │       └── l_extendedprice
│   ├── rhs: 7.0

├── cost: 608673100
├── rows: 1
└── Agg
    ├── aggs:sum
    │   └── l_extendedprice
    ├── cost: 608673100
    ├── rows: 1
    └── Projection { exprs: [ l_extendedprice ], cost: 608313000, rows: 3000607.5 }
        └── Filter
            ├── cond:>
            │   ├── lhs:ref
            │   │   └── *
            │   │       ├── lhs:/
            │   │       │   ├── lhs:ref
            │   │       │   │   └── sum
            │   │       │   │       └── l_quantity(1)
            │   │       │   ├── rhs:ref
            │   │       │   │   └── count
            │   │       │   │       └── l_quantity(1)

            │   │       ├── rhs: 0.2

            │   ├── rhs: l_quantity

            ├── cost: 608253000
            ├── rows: 3000607.5
            └── Projection
                ├── exprs:
                │   ┌── l_quantity
                │   ├── l_extendedprice
                │   └── ref
                │       └── *
                │           ├── lhs:/
                │           │   ├── lhs:ref
                │           │   │   └── sum
                │           │   │       └── l_quantity(1)
                │           │   ├── rhs:ref
                │           │   │   └── count
                │           │   │       └── l_quantity(1)

                │           ├── rhs: 0.2

                ├── cost: 598531000
                ├── rows: 6001215
                └── Projection
                    ├── exprs:
                    │   ┌── l_orderkey
                    │   ├── l_partkey
                    │   ├── l_suppkey
                    │   ├── l_linenumber
                    │   ├── l_quantity
                    │   ├── l_extendedprice
                    │   ├── l_discount
                    │   ├── l_tax
                    │   ├── l_returnflag
                    │   ├── l_linestatus
                    │   ├── l_shipdate
                    │   ├── l_commitdate
                    │   ├── l_receiptdate
                    │   ├── l_shipinstruct
                    │   ├── l_shipmode
                    │   ├── l_comment
                    │   ├── p_partkey
                    │   ├── p_name
                    │   ├── p_mfgr
                    │   ├── p_brand
                    │   ├── p_type
                    │   ├── p_size
                    │   ├── p_container
                    │   ├── p_retailprice
                    │   ├── p_comment
                    │   └── *
                    │       ├── lhs:/
                    │       │   ├── lhs:ref
                    │       │   │   └── sum
                    │       │   │       └── l_quantity(1)
                    │       │   ├── rhs:ref
                    │       │   │   └── count
                    │       │   │       └── l_quantity(1)

                    │       ├── rhs: 0.2

                    ├── cost: 598290940
                    ├── rows: 6001215
                    └── HashAgg
                        ├── keys:
                        │   ┌── l_orderkey
                        │   ├── l_partkey
                        │   ├── l_suppkey
                        │   ├── l_linenumber
                        │   ├── l_quantity
                        │   ├── l_extendedprice
                        │   ├── l_discount
                        │   ├── l_tax
                        │   ├── l_returnflag
                        │   ├── l_linestatus
                        │   ├── l_shipdate
                        │   ├── l_commitdate
                        │   ├── l_receiptdate
                        │   ├── l_shipinstruct
                        │   ├── l_shipmode
                        │   ├── l_comment
                        │   ├── p_partkey
                        │   ├── p_name
                        │   ├── p_mfgr
                        │   ├── p_brand
                        │   ├── p_type
                        │   ├── p_size
                        │   ├── p_container
                        │   ├── p_retailprice
                        │   └── p_comment
                        ├── aggs:
                        │   ┌── sum
                        │   │   └── l_quantity(1)
                        │   └── count
                        │       └── l_quantity(1)
                        ├── cost: 594810240
                        ├── rows: 6001215
                        └── Projection
                            ├── exprs:
                            │   ┌── l_orderkey
                            │   ├── l_partkey
                            │   ├── l_suppkey
                            │   ├── l_linenumber
                            │   ├── l_quantity
                            │   ├── l_extendedprice
                            │   ├── l_discount
                            │   ├── l_tax
                            │   ├── l_returnflag
                            │   ├── l_linestatus
                            │   ├── l_shipdate
                            │   ├── l_commitdate
                            │   ├── l_receiptdate
                            │   ├── l_shipinstruct
                            │   ├── l_shipmode
                            │   ├── l_comment
                            │   ├── p_partkey
                            │   ├── p_name
                            │   ├── p_mfgr
                            │   ├── p_brand
                            │   ├── p_type
                            │   ├── p_size
                            │   ├── p_container
                            │   ├── p_retailprice
                            │   ├── p_comment
                            │   └── l_quantity(1)
                            ├── cost: 428485540
                            ├── rows: 6001215
                            └── HashJoin
                                ├── type: left_outer
                                ├── on: = { lhs: [ p_partkey ], rhs: [ l_partkey(1) ] }
                                ├── cost: 426865200
                                ├── rows: 6001215
                                ├── HashJoin
                                │   ├── type: inner
                                │   ├── on: = { lhs: [ l_partkey ], rhs: [ p_partkey ] }
                                │   ├── cost: 249887380
                                │   ├── rows: 6001215
                                │   ├── Scan
                                │   │   ├── table: lineitem
                                │   │   ├── list:
                                │   │   │   ┌── l_orderkey
                                │   │   │   ├── l_partkey
                                │   │   │   ├── l_suppkey
                                │   │   │   ├── l_linenumber
                                │   │   │   ├── l_quantity
                                │   │   │   ├── l_extendedprice
                                │   │   │   ├── l_discount
                                │   │   │   ├── l_tax
                                │   │   │   ├── l_returnflag
                                │   │   │   ├── l_linestatus
                                │   │   │   ├── l_shipdate
                                │   │   │   ├── l_commitdate
                                │   │   │   ├── l_receiptdate
                                │   │   │   ├── l_shipinstruct
                                │   │   │   ├── l_shipmode
                                │   │   │   └── l_comment
                                │   │   ├── filter: null
                                │   │   ├── cost: 96019440
                                │   │   └── rows: 6001215
                                │   └── Filter
                                │       ├── cond:and
                                │       │   ├── lhs: = { lhs: p_container, rhs: 'MED BOX' }
                                │       │   └── rhs: = { lhs: p_brand, rhs: 'Brand#23' }
                                │       ├── cost: 2354000
                                │       ├── rows: 50000
                                │       └── Scan
                                │           ├── table: part
                                │           ├── list:
                                │           │   ┌── p_partkey
                                │           │   ├── p_name
                                │           │   ├── p_mfgr
                                │           │   ├── p_brand
                                │           │   ├── p_type
                                │           │   ├── p_size
                                │           │   ├── p_container
                                │           │   ├── p_retailprice
                                │           │   └── p_comment
                                │           ├── filter: null
                                │           ├── cost: 1800000
                                │           └── rows: 200000
                                └── Scan
                                    ├── table: lineitem
                                    ├── list: [ l_partkey(1), l_quantity(1) ]
                                    ├── filter: null
                                    ├── cost: 12002430
                                    └── rows: 6001215
*/

-- tpch-q18
explain select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem
where
    o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 300
    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate
limit 100;

/*
Projection
├── exprs:
│   ┌── c_name
│   ├── c_custkey
│   ├── o_orderkey
│   ├── o_orderdate
│   ├── o_totalprice
│   └── ref
│       └── sum
│           └── l_quantity
├── cost: 124502856
├── rows: 100
└── TopN
    ├── limit: 100
    ├── offset: 0
    ├── order_by:
    │   ┌── desc
    │   │   └── o_totalprice
    │   └── o_orderdate
    ├── cost: 124502850
    ├── rows: 100
    └── HashAgg
        ├── keys: [ c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ]
        ├── aggs:sum
        │   └── l_quantity
        ├── cost: 123836424
        ├── rows: 100000
        └── HashJoin
            ├── type: semi
            ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey(1) ] }
            ├── cost: 121159420
            ├── rows: 6001215
            ├── Projection
            │   ├── exprs: [ c_custkey, c_name, o_orderkey, o_totalprice, o_orderdate, l_quantity ]
            │   ├── cost: 71826744
            │   ├── rows: 6001215
            │   └── HashJoin
            │       ├── type: inner
            │       ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
            │       ├── cost: 71406660
            │       ├── rows: 6001215
            │       ├── Projection
            │       │   ├── exprs: [ c_custkey, c_name, o_orderkey, o_totalprice, o_orderdate ]
            │       │   ├── cost: 15706711
            │       │   ├── rows: 1500000
            │       │   └── HashJoin
            │       │       ├── type: inner
            │       │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
            │       │       ├── cost: 15616711
            │       │       ├── rows: 1500000
            │       │       ├── Scan
            │       │       │   ├── table: customer
            │       │       │   ├── list: [ c_custkey, c_name ]
            │       │       │   ├── filter: null
            │       │       │   ├── cost: 300000
            │       │       │   └── rows: 150000
            │       │       └── Scan
            │       │           ├── table: orders
            │       │           ├── list: [ o_orderkey, o_custkey, o_totalprice, o_orderdate ]
            │       │           ├── filter: null
            │       │           ├── cost: 6000000
            │       │           └── rows: 1500000
            │       └── Scan
            │           ├── table: lineitem
            │           ├── list: [ l_orderkey, l_quantity ]
            │           ├── filter: null
            │           ├── cost: 12002430
            │           └── rows: 6001215
            └── Projection { exprs: [ l_orderkey(1) ], cost: 13050240, rows: 5 }
                └── Filter
                    ├── cond:>
                    │   ├── lhs:ref
                    │   │   └── sum
                    │   │       └── l_quantity(1)
                    │   ├── rhs: 300

                    ├── cost: 13050240
                    ├── rows: 5
                    └── HashAgg
                        ├── keys: [ l_orderkey(1) ]
                        ├── aggs:sum
                        │   └── l_quantity(1)
                        ├── cost: 13050228
                        ├── rows: 10
                        └── Scan
                            ├── table: lineitem
                            ├── list: [ l_orderkey(1), l_quantity(1) ]
                            ├── filter: null
                            ├── cost: 12002430
                            └── rows: 6001215
*/

-- tpch-q19
explain select
    sum(l_extendedprice* (1 - l_discount)) as revenue
from
    lineitem,
    part
where
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#12'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 1 and l_quantity <= 1 + 10
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 10 and l_quantity <= 10 + 10
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#33'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 20 and l_quantity <= 20 + 10
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    );

/*
Projection
├── exprs:ref
│   └── sum
│       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
├── cost: 103831030
├── rows: 1
└── Agg
    ├── aggs:sum
    │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    ├── cost: 103831030
    ├── rows: 1
    └── Projection { exprs: [ l_extendedprice, l_discount ], cost: 103603910, rows: 528183.1 }
        └── Filter
            ├── cond:or
            │   ├── lhs:or
            │   │   ├── lhs:and
            │   │   │   ├── lhs:and
            │   │   │   │   ├── lhs:and
            │   │   │   │   │   ├── lhs: >= { lhs: l_quantity, rhs: 10 }
            │   │   │   │   │   ├── rhs:In { in: [ 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK' ] }
            │   │   │   │   │   │   └── p_container

            │   │   │   │   ├── rhs: = { lhs: p_brand, rhs: 'Brand#23' }

            │   │   │   ├── rhs: and { lhs: >= { lhs: 10, rhs: p_size }, rhs: >= { lhs: 20, rhs: l_quantity } }

            │   │   ├── rhs:and
            │   │   │   ├── lhs:and
            │   │   │   │   ├── lhs:and
            │   │   │   │   │   ├── lhs: >= { lhs: l_quantity, rhs: 1 }
            │   │   │   │   │   ├── rhs:In { in: [ 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG' ] }
            │   │   │   │   │   │   └── p_container

            │   │   │   │   ├── rhs: = { lhs: p_brand, rhs: 'Brand#12' }

            │   │   │   ├── rhs: and { lhs: >= { lhs: 5, rhs: p_size }, rhs: >= { lhs: 11, rhs: l_quantity } }


            │   ├── rhs:and
            │   │   ├── lhs: >= { lhs: 15, rhs: p_size }
            │   │   ├── rhs:and
            │   │   │   ├── lhs:and
            │   │   │   │   ├── lhs: and { lhs: >= { lhs: 30, rhs: l_quantity }, rhs: >= { lhs: l_quantity, rhs: 20 } }
            │   │   │   │   ├── rhs:In { in: [ 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG' ] }
            │   │   │   │   │   └── p_container

            │   │   │   ├── rhs: = { lhs: p_brand, rhs: 'Brand#33' }



            ├── cost: 103588060
            ├── rows: 528183.1
            └── Projection
                ├── exprs: [ l_quantity, l_extendedprice, l_discount, p_brand, p_size, p_container ]
                ├── cost: 83975640
                ├── rows: 3000607.5
                └── HashJoin
                    ├── type: inner
                    ├── on: = { lhs: [ p_partkey ], rhs: [ l_partkey ] }
                    ├── cost: 83765600
                    ├── rows: 3000607.5
                    ├── Filter { cond: >= { lhs: p_size, rhs: 1 }, cost: 1242000, rows: 100000 }
                    │   └── Scan
                    │       ├── table: part
                    │       ├── list: [ p_partkey, p_brand, p_size, p_container ]
                    │       ├── filter: null
                    │       ├── cost: 800000
                    │       └── rows: 200000
                    └── Projection
                        ├── exprs: [ l_partkey, l_quantity, l_extendedprice, l_discount ]
                        ├── cost: 57941730
                        ├── rows: 3000607.5
                        └── Filter
                            ├── cond:and
                            │   ├── lhs:In { in: [ 'AIR', 'AIR REG' ] }
                            │   │   └── l_shipmode
                            │   ├── rhs: = { lhs: l_shipinstruct, rhs: 'DELIVER IN PERSON' }

                            ├── cost: 57791696
                            ├── rows: 3000607.5
                            └── Scan
                                ├── table: lineitem
                                ├── list:
                                │   ┌── l_partkey
                                │   ├── l_quantity
                                │   ├── l_extendedprice
                                │   ├── l_discount
                                │   ├── l_shipinstruct
                                │   └── l_shipmode
                                ├── filter: null
                                ├── cost: 36007290
                                └── rows: 6001215
*/

-- tpch-q20
explain select
    s_name,
    s_address
from
    supplier,
    nation
where
    s_suppkey in (
        select
            ps_suppkey
        from
            partsupp
        where
            ps_partkey in (
                select
                    p_partkey
                from
                    part
                where
                    p_name like 'forest%'
            )
            and ps_availqty > (
                select
                    0.5 * sum(l_quantity)
                from
                    lineitem
                where
                    l_partkey = ps_partkey
                    and l_suppkey = ps_suppkey
                    and l_shipdate >= date '1994-01-01'
                    and l_shipdate < date '1994-01-01' + interval '1' year
            )
    )
    and s_nationkey = n_nationkey
    and n_name = 'CANADA'
order by
    s_name;

/*
Order { by: [ s_name ], cost: 52236732, rows: 10000 }
└── Projection { exprs: [ s_name, s_address ], cost: 52083852, rows: 10000 }
    └── HashJoin { type: semi, on: = { lhs: [ s_suppkey ], rhs: [ ps_suppkey ] }, cost: 52083550, rows: 10000 }
        ├── Projection { exprs: [ s_suppkey, s_name, s_address ], cost: 91056.7, rows: 10000 }
        │   └── HashJoin
        │       ├── type: inner
        │       ├── on: = { lhs: [ n_nationkey ], rhs: [ s_nationkey ] }
        │       ├── cost: 90656.7
        │       ├── rows: 10000
        │       ├── Projection { exprs: [ n_nationkey ], cost: 80.5, rows: 12.5 }
        │       │   └── Filter { cond: = { lhs: n_name, rhs: 'CANADA' }, cost: 80.25, rows: 12.5 }
        │       │       └── Scan { table: nation, list: [ n_nationkey, n_name ], filter: null, cost: 50, rows: 25 }
        │       └── Scan
        │           ├── table: supplier
        │           ├── list: [ s_suppkey, s_name, s_address, s_nationkey ]
        │           ├── filter: null
        │           ├── cost: 40000
        │           └── rows: 10000
        └── Projection { exprs: [ ps_suppkey ], cost: 51951930, rows: 50000 }
            └── HashJoin { type: semi, on: = { lhs: [ ps_partkey ], rhs: [ p_partkey ] }, cost: 51950930, rows: 50000 }
                ├── Projection { exprs: [ ps_partkey, ps_suppkey ], cost: 51179012, rows: 50000 }
                │   └── Filter
                │       ├── cond:>
                │       │   ├── lhs: ps_availqty
                │       │   ├── rhs:ref
                │       │   │   └── *
                │       │   │       ├── lhs: 0.5
                │       │   │       ├── rhs:ref
                │       │   │       │   └── sum
                │       │   │       │       └── l_quantity


                │       ├── cost: 51177510
                │       ├── rows: 50000
                │       └── Projection
                │           ├── exprs:
                │           │   ┌── ps_partkey
                │           │   ├── ps_suppkey
                │           │   ├── ps_availqty
                │           │   └── ref
                │           │       └── *
                │           │           ├── lhs: 0.5
                │           │           ├── rhs:ref
                │           │           │   └── sum
                │           │           │       └── l_quantity

                │           ├── cost: 50965510
                │           ├── rows: 100000
                │           └── Projection
                │               ├── exprs:
                │               │   ┌── ps_partkey
                │               │   ├── ps_suppkey
                │               │   ├── ps_availqty
                │               │   ├── ps_supplycost
                │               │   ├── ps_comment
                │               │   └── *
                │               │       ├── lhs: 0.5
                │               │       ├── rhs:ref
                │               │       │   └── sum
                │               │       │       └── l_quantity

                │               ├── cost: 50960510
                │               ├── rows: 100000
                │               └── HashAgg
                │                   ├── keys: [ ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment ]
                │                   ├── aggs:sum
                │                   │   └── l_quantity
                │                   ├── cost: 50933510
                │                   ├── rows: 100000
                │                   └── Projection
                │                       ├── exprs:
                │                       │   ┌── ps_partkey
                │                       │   ├── ps_suppkey
                │                       │   ├── ps_availqty
                │                       │   ├── ps_supplycost
                │                       │   ├── ps_comment
                │                       │   └── l_quantity
                │                       ├── cost: 49814260
                │                       ├── rows: 1500303.8
                │                       └── HashJoin
                │                           ├── type: left_outer
                │                           ├── on: = { lhs: [ ps_partkey, ps_suppkey ], rhs: [ l_partkey, l_suppkey ] }
                │                           ├── cost: 49709240
                │                           ├── rows: 1500303.8
                │                           ├── Scan
                │                           │   ├── table: partsupp
                │                           │   ├── list:
                │                           │   │   ┌── ps_partkey
                │                           │   │   ├── ps_suppkey
                │                           │   │   ├── ps_availqty
                │                           │   │   ├── ps_supplycost
                │                           │   │   └── ps_comment
                │                           │   ├── filter: null
                │                           │   ├── cost: 4000000
                │                           │   └── rows: 800000
                │                           └── Projection
                │                               ├── exprs: [ l_partkey, l_suppkey, l_quantity ]
                │                               ├── cost: 33186720
                │                               ├── rows: 1500303.8
                │                               └── Filter
                │                                   ├── cond:and
                │                                   │   ├── lhs: >= { lhs: l_shipdate, rhs: 1994-01-01 }
                │                                   │   └── rhs: > { lhs: 1995-01-01, rhs: l_shipdate }
                │                                   ├── cost: 33126708
                │                                   ├── rows: 1500303.8
                │                                   └── Scan
                │                                       ├── table: lineitem
                │                                       ├── list: [ l_partkey, l_suppkey, l_quantity, l_shipdate ]
                │                                       ├── filter: null
                │                                       ├── cost: 24004860
                │                                       └── rows: 6001215
                └── Projection { exprs: [ p_partkey ], cost: 644000, rows: 100000 }
                    └── Filter { cond: like { lhs: p_name, rhs: 'forest%' }, cost: 642000, rows: 100000 }
                        └── Scan { table: part, list: [ p_partkey, p_name ], filter: null, cost: 400000, rows: 200000 }
*/

-- tpch-q21
explain select
    s_name,
    count(*) as numwait
from
    supplier,
    lineitem l1,
    orders,
    nation
where
    s_suppkey = l1.l_suppkey
    and o_orderkey = l1.l_orderkey
    and o_orderstatus = 'F'
    and l1.l_receiptdate > l1.l_commitdate
    and exists (
        select
            *
        from
            lineitem l2
        where
            l2.l_orderkey = l1.l_orderkey
            and l2.l_suppkey <> l1.l_suppkey
    )
    and not exists (
        select
            *
        from
            lineitem l3
        where
            l3.l_orderkey = l1.l_orderkey
            and l3.l_suppkey <> l1.l_suppkey
            and l3.l_receiptdate > l3.l_commitdate
    )
    and s_nationkey = n_nationkey
    and n_name = 'SAUDI ARABIA'
group by
    s_name
order by
    numwait desc,
    s_name
limit 100;

/*
Limit { limit: 100, offset: 0, cost: 4591978000000, rows: 10 }
└── Projection
    ├── exprs:
    │   ┌── s_name
    │   └── ref
    │       └── rowcount
    ├── cost: 4591978000000
    ├── rows: 10
    └── Order
        ├── by:
        │   ┌── desc
        │   │   └── ref
        │   │       └── rowcount
        │   └── s_name
        ├── cost: 4591978000000
        ├── rows: 10
        └── HashAgg { keys: [ s_name ], aggs: [ rowcount ], cost: 4591978000000, rows: 10 }
            └── Projection { exprs: [ s_name ], cost: 4591978000000, rows: 187537.97 }
                └── Join
                    ├── type: semi
                    ├── on:and
                    │   ├── lhs: = { lhs: l_orderkey(1), rhs: l_orderkey }
                    │   └── rhs: <> { lhs: l_suppkey(1), rhs: l_suppkey }
                    ├── cost: 4591978000000
                    ├── rows: 187537.97
                    ├── Join
                    │   ├── type: anti
                    │   ├── on:and
                    │   │   ├── lhs: = { lhs: l_orderkey(2), rhs: l_orderkey }
                    │   │   └── rhs: <> { lhs: l_suppkey(2), rhs: l_suppkey }
                    │   ├── cost: 3061345700000
                    │   ├── rows: 750151.9
                    │   ├── Projection { exprs: [ s_name, l_orderkey, l_suppkey ], cost: 67230584, rows: 3000607.5 }
                    │   │   └── HashJoin
                    │   │       ├── type: inner
                    │   │       ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                    │   │       ├── cost: 67110560
                    │   │       ├── rows: 3000607.5
                    │   │       ├── Projection { exprs: [ o_orderkey ], cost: 4830000, rows: 750000 }
                    │   │       │   └── Filter
                    │   │       │       ├── cond: = { lhs: o_orderstatus, rhs: 'F' }
                    │   │       │       ├── cost: 4815000
                    │   │       │       ├── rows: 750000
                    │   │       │       └── Scan
                    │   │       │           ├── table: orders
                    │   │       │           ├── list: [ o_orderkey, o_orderstatus ]
                    │   │       │           ├── filter: null
                    │   │       │           ├── cost: 3000000
                    │   │       │           └── rows: 1500000
                    │   │       └── Projection
                    │   │           ├── exprs: [ s_name, l_orderkey, l_suppkey ]
                    │   │           ├── cost: 49471124
                    │   │           ├── rows: 3000607.5
                    │   │           └── HashJoin
                    │   │               ├── type: inner
                    │   │               ├── on: = { lhs: [ s_suppkey ], rhs: [ l_suppkey ] }
                    │   │               ├── cost: 49351100
                    │   │               ├── rows: 3000607.5
                    │   │               ├── Projection { exprs: [ s_suppkey, s_name ], cost: 70956.7, rows: 10000 }
                    │   │               │   └── HashJoin
                    │   │               │       ├── type: inner
                    │   │               │       ├── on: = { lhs: [ n_nationkey ], rhs: [ s_nationkey ] }
                    │   │               │       ├── cost: 70656.7
                    │   │               │       ├── rows: 10000
                    │   │               │       ├── Projection { exprs: [ n_nationkey ], cost: 80.5, rows: 12.5 }
                    │   │               │       │   └── Filter
                    │   │               │       │       ├── cond: = { lhs: n_name, rhs: 'SAUDI ARABIA' }
                    │   │               │       │       ├── cost: 80.25
                    │   │               │       │       ├── rows: 12.5
                    │   │               │       │       └── Scan
                    │   │               │       │           ├── table: nation
                    │   │               │       │           ├── list: [ n_nationkey, n_name ]
                    │   │               │       │           ├── filter: null
                    │   │               │       │           ├── cost: 50
                    │   │               │       │           └── rows: 25
                    │   │               │       └── Scan
                    │   │               │           ├── table: supplier
                    │   │               │           ├── list: [ s_suppkey, s_name, s_nationkey ]
                    │   │               │           ├── filter: null
                    │   │               │           ├── cost: 30000
                    │   │               │           └── rows: 10000
                    │   │               └── Projection
                    │   │                   ├── exprs: [ l_orderkey, l_suppkey ]
                    │   │                   ├── cost: 36817456
                    │   │                   ├── rows: 3000607.5
                    │   │                   └── Filter
                    │   │                       ├── cond: > { lhs: l_receiptdate, rhs: l_commitdate }
                    │   │                       ├── cost: 36727436
                    │   │                       ├── rows: 3000607.5
                    │   │                       └── Scan
                    │   │                           ├── table: lineitem
                    │   │                           ├── list: [ l_orderkey, l_suppkey, l_commitdate, l_receiptdate ]
                    │   │                           ├── filter: null
                    │   │                           ├── cost: 24004860
                    │   │                           └── rows: 6001215
                    │   └── Projection { exprs: [ l_orderkey(2), l_suppkey(2) ], cost: 36817456, rows: 3000607.5 }
                    │       └── Filter
                    │           ├── cond: > { lhs: l_receiptdate(2), rhs: l_commitdate(2) }
                    │           ├── cost: 36727436
                    │           ├── rows: 3000607.5
                    │           └── Scan
                    │               ├── table: lineitem
                    │               ├── list: [ l_orderkey(2), l_suppkey(2), l_commitdate(2), l_receiptdate(2) ]
                    │               ├── filter: null
                    │               ├── cost: 24004860
                    │               └── rows: 6001215
                    └── Scan
                        ├── table: lineitem
                        ├── list: [ l_orderkey(1), l_suppkey(1) ]
                        ├── filter: null
                        ├── cost: 12002430
                        └── rows: 6001215
*/

-- tpch-q22
explain select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            substring(c_phone from 1 for 2) as cntrycode,
            c_acctbal
        from
            customer
        where
            substring(c_phone from 1 for 2) in
                ('13', '31', '23', '29', '30', '18', '17')
            and c_acctbal > (
                select
                    avg(c_acctbal)
                from
                    customer
                where
                    c_acctbal > 0.00
                    and substring(c_phone from 1 for 2) in
                        ('13', '31', '23', '29', '30', '18', '17')
            )
            and not exists (
                select
                    *
                from
                    orders
                where
                    o_custkey = c_custkey
            )
    ) as custsale
group by
    cntrycode
order by
    cntrycode;

/*
Projection
├── exprs:
│   ┌── ref
│   │   └── Substring { str: c_phone, start: 1, length: 2 }
│   ├── ref
│   │   └── rowcount
│   └── ref
│       └── sum
│           └── c_acctbal
├── cost: 4377327
├── rows: 10
└── Order
    ├── by:ref
    │   └── Substring { str: c_phone, start: 1, length: 2 }
    ├── cost: 4377326.5
    ├── rows: 10
    └── HashAgg
        ├── keys:ref
        │   └── Substring { str: c_phone, start: 1, length: 2 }
        ├── aggs:
        │   ┌── rowcount
        │   └── sum
        │       └── c_acctbal
        ├── cost: 4377262
        ├── rows: 10
        └── Projection
            ├── exprs: [ Substring { str: c_phone, start: 1, length: 2 }, c_acctbal ]
            ├── cost: 4356637.5
            ├── rows: 75000
            └── HashJoin { type: anti, on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }, cost: 4331887.5, rows: 75000 }
                ├── Projection { exprs: [ c_custkey, c_phone, c_acctbal ], cost: 2252252, rows: 75000 }
                │   └── Filter
                │       ├── cond:and
                │       │   ├── lhs:In { in: [ '13', '31', '23', '29', '30', '18', '17' ] }
                │       │   │   └── Substring { str: c_phone, start: 1, length: 2 }
                │       │   ├── rhs:>
                │       │   │   ├── lhs: c_acctbal
                │       │   │   ├── rhs:ref
                │       │   │   │   └── /
                │       │   │   │       ├── lhs:ref
                │       │   │   │       │   └── sum
                │       │   │   │       │       └── c_acctbal(1)
                │       │   │   │       ├── rhs:ref
                │       │   │   │       │   └── count
                │       │   │   │       │       └── c_acctbal(1)



                │       ├── cost: 2249252
                │       ├── rows: 75000
                │       └── Join { type: left_outer, cost: 1748252.1, rows: 150000 }
                │           ├── Scan
                │           │   ├── table: customer
                │           │   ├── list: [ c_custkey, c_phone, c_acctbal ]
                │           │   ├── filter: null
                │           │   ├── cost: 450000
                │           │   └── rows: 150000
                │           └── Projection
                │               ├── exprs:ref
                │               │   └── /
                │               │       ├── lhs:ref
                │               │       │   └── sum
                │               │       │       └── c_acctbal(1)
                │               │       ├── rhs:ref
                │               │       │   └── count
                │               │       │       └── c_acctbal(1)

                │               ├── cost: 683252.1
                │               ├── rows: 1
                │               └── Projection
                │                   ├── exprs:/
                │                   │   ├── lhs:ref
                │                   │   │   └── sum
                │                   │   │       └── c_acctbal(1)
                │                   │   ├── rhs:ref
                │                   │   │   └── count
                │                   │   │       └── c_acctbal(1)

                │                   ├── cost: 683252.1
                │                   ├── rows: 1
                │                   └── Agg
                │                       ├── aggs:
                │                       │   ┌── sum
                │                       │   │   └── c_acctbal(1)
                │                       │   └── count
                │                       │       └── c_acctbal(1)
                │                       ├── cost: 683252
                │                       ├── rows: 1
                │                       └── Projection { exprs: [ c_acctbal(1) ], cost: 666000, rows: 75000 }
                │                           └── Filter
                │                               ├── cond:and
                │                               │   ├── lhs: > { lhs: c_acctbal(1), rhs: 0.00 }
                │                               │   ├── rhs:In { in: [ '13', '31', '23', '29', '30', '18', '17' ] }
                │                               │   │   └── Substring { str: c_phone(1), start: 1, length: 2 }

                │                               ├── cost: 664500
                │                               ├── rows: 75000
                │                               └── Scan
                │                                   ├── table: customer
                │                                   ├── list: [ c_phone(1), c_acctbal(1) ]
                │                                   ├── filter: null
                │                                   ├── cost: 300000
                │                                   └── rows: 150000
                └── Scan { table: orders, list: [ o_custkey ], filter: null, cost: 1500000, rows: 1500000 }
*/

