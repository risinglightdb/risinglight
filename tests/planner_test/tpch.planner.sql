-- prepare
CREATE TABLE NATION  (
    N_NATIONKEY  INT PRIMARY KEY,
    N_NAME       CHAR(25) NOT NULL,
    N_REGIONKEY  INT NOT NULL,
    N_COMMENT    VARCHAR(152)
);
CREATE TABLE REGION  (
    R_REGIONKEY  INT PRIMARY KEY,
    R_NAME       CHAR(25) NOT NULL,
    R_COMMENT    VARCHAR(152)
);
CREATE TABLE PART  (
    P_PARTKEY     INT PRIMARY KEY,
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
    S_SUPPKEY     INT PRIMARY KEY,
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
    -- PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
);
CREATE TABLE CUSTOMER (
    C_CUSTKEY     INT PRIMARY KEY,
    C_NAME        VARCHAR(25) NOT NULL,
    C_ADDRESS     VARCHAR(40) NOT NULL,
    C_NATIONKEY   INT NOT NULL,
    C_PHONE       CHAR(15) NOT NULL,
    C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
    C_MKTSEGMENT  CHAR(10) NOT NULL,
    C_COMMENT     VARCHAR(117) NOT NULL
);
CREATE TABLE ORDERS (
    O_ORDERKEY       INT PRIMARY KEY,
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
    -- PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
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
│   ├── #30
│   ├── #29
│   ├── #28
│   ├── #26
│   ├── (#30 / #20) as #44
│   ├── (#29 / #19) as #41
│   ├── (#18 / #17) as #38
│   └── #16
├── cost: 70266880
├── rows: 100
└── Order { by: [ l_returnflag, l_linestatus ], cost: 70266840, rows: 100 }
    └── HashAgg
        ├── keys: [ l_returnflag, l_linestatus ]
        ├── aggs:
        │   ┌── sum(l_quantity) as #30
        │   ├── sum(l_extendedprice) as #29
        │   ├── sum((l_extendedprice * (1 - l_discount))) as #28
        │   ├── sum(((l_extendedprice + (l_tax * l_extendedprice)) * (1 - l_discount))) as #26
        │   ├── count(l_quantity) as #20
        │   ├── count(l_extendedprice) as #19
        │   ├── sum(l_discount) as #18
        │   ├── count(l_discount) as #17
        │   └── count(*) as #16
        ├── cost: 70265070
        ├── rows: 100
        └── Projection
            ├── exprs: [ l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus ]
            ├── cost: 64483056
            ├── rows: 3000607.5
            └── Filter { cond: (1998-09-21 >= l_shipdate), cost: 64273012, rows: 3000607.5 }
                └── Scan
                    ├── table: lineitem
                    ├── list: [ l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate ]
                    ├── filter: true
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
├── cost: 109844270
├── rows: 100
└── TopN { limit: 100, offset: 0, order_by: [ s_acctbal desc, n_name, s_name, p_partkey ], cost: 109844264, rows: 100 }
    └── Projection
        ├── exprs: [ p_partkey, p_mfgr, s_name, s_address, s_phone, s_acctbal, s_comment, n_name ]
        ├── cost: 107180180
        ├── rows: 400000
        └── Filter { cond: (ps_supplycost = #123), cost: 107144180, rows: 400000 }
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
                │   └── #123
                ├── cost: 103048180
                ├── rows: 800000
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
                    ├── aggs: [ min(#51) as #123 ]
                    ├── cost: 102960180
                    ├── rows: 800000
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
                        │   └── #51
                        ├── cost: 79275300
                        ├── rows: 800000
                        └── HashJoin
                            ├── type: left_outer
                            ├── cond: true
                            ├── lkey: [ p_partkey ]
                            ├── rkey: [ #53 ]
                            ├── cost: 79035300
                            ├── rows: 800000
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
                            │   ├── cost: 46723376
                            │   ├── rows: 800000
                            │   └── HashJoin
                            │       ├── type: inner
                            │       ├── cond: true
                            │       ├── lkey: [ p_partkey ]
                            │       ├── rkey: [ ps_partkey ]
                            │       ├── cost: 46491376
                            │       ├── rows: 800000
                            │       ├── Filter
                            │       │   ├── cond: ((p_type like '%BRASS') and (p_size = 15))
                            │       │   ├── cost: 2354000
                            │       │   ├── rows: 50000
                            │       │   └── Scan
                            │       │       ├── table: part
                            │       │       ├── list:
                            │       │       │   ┌── p_partkey
                            │       │       │   ├── p_name
                            │       │       │   ├── p_mfgr
                            │       │       │   ├── p_brand
                            │       │       │   ├── p_type
                            │       │       │   ├── p_size
                            │       │       │   ├── p_container
                            │       │       │   ├── p_retailprice
                            │       │       │   └── p_comment
                            │       │       ├── filter: true
                            │       │       ├── cost: 1800000
                            │       │       └── rows: 200000
                            │       └── Projection
                            │           ├── exprs:
                            │           │   ┌── n_nationkey
                            │           │   ├── n_name
                            │           │   ├── n_regionkey
                            │           │   ├── n_comment
                            │           │   ├── r_regionkey
                            │           │   ├── r_name
                            │           │   ├── r_comment
                            │           │   ├── s_suppkey
                            │           │   ├── s_name
                            │           │   ├── s_address
                            │           │   ├── s_nationkey
                            │           │   ├── s_phone
                            │           │   ├── s_acctbal
                            │           │   ├── s_comment
                            │           │   ├── ps_partkey
                            │           │   ├── ps_suppkey
                            │           │   ├── ps_availqty
                            │           │   ├── ps_supplycost
                            │           │   └── ps_comment
                            │           ├── cost: 21502692
                            │           ├── rows: 800000
                            │           └── HashJoin
                            │               ├── type: inner
                            │               ├── cond: true
                            │               ├── lkey: [ ps_suppkey ]
                            │               ├── rkey: [ s_suppkey ]
                            │               ├── cost: 21342692
                            │               ├── rows: 800000
                            │               ├── Scan
                            │               │   ├── table: partsupp
                            │               │   ├── list:
                            │               │   │   ┌── ps_partkey
                            │               │   │   ├── ps_suppkey
                            │               │   │   ├── ps_availqty
                            │               │   │   ├── ps_supplycost
                            │               │   │   └── ps_comment
                            │               │   ├── filter: true
                            │               │   ├── cost: 4000000
                            │               │   └── rows: 800000
                            │               └── Join
                            │                   ├── type: inner
                            │                   ├── on: (n_nationkey = s_nationkey)
                            │                   ├── cost: 1850303.1
                            │                   ├── rows: 125000
                            │                   ├── Scan
                            │                   │   ├── table: supplier
                            │                   │   ├── list:
                            │                   │   │   ┌── s_suppkey
                            │                   │   │   ├── s_name
                            │                   │   │   ├── s_address
                            │                   │   │   ├── s_nationkey
                            │                   │   │   ├── s_phone
                            │                   │   │   ├── s_acctbal
                            │                   │   │   └── s_comment
                            │                   │   ├── filter: true
                            │                   │   ├── cost: 70000
                            │                   │   └── rows: 10000
                            │                   └── HashJoin
                            │                       ├── type: inner
                            │                       ├── cond: true
                            │                       ├── lkey: [ n_regionkey ]
                            │                       ├── rkey: [ r_regionkey ]
                            │                       ├── cost: 303.1426
                            │                       ├── rows: 25
                            │                       ├── Scan
                            │                       │   ├── table: nation
                            │                       │   ├── list: [ n_nationkey, n_name, n_regionkey, n_comment ]
                            │                       │   ├── filter: true
                            │                       │   ├── cost: 100
                            │                       │   └── rows: 25
                            │                       └── Filter { cond: (r_name = 'EUROPE'), cost: 23.55, rows: 2.5 }
                            │                           └── Scan
                            │                               ├── table: region
                            │                               ├── list: [ r_regionkey, r_name, r_comment ]
                            │                               ├── filter: true
                            │                               ├── cost: 15
                            │                               └── rows: 5
                            └── Projection { exprs: [ #53, #51 ], cost: 7806164.5, rows: 800000 }
                                └── HashJoin
                                    ├── type: inner
                                    ├── cond: true
                                    ├── lkey: [ #52 ]
                                    ├── rkey: [ #7 ]
                                    ├── cost: 7782164.5
                                    ├── rows: 800000
                                    ├── Projection
                                    │   ├── exprs: [ ps_partkey' as #53, ps_suppkey' as #52, ps_supplycost' as #51 ]
                                    │   ├── cost: 2672000
                                    │   ├── rows: 800000
                                    │   └── Scan
                                    │       ├── table: partsupp
                                    │       ├── list: [ ps_partkey, ps_suppkey, ps_supplycost ]
                                    │       ├── filter: true
                                    │       ├── cost: 2400000
                                    │       └── rows: 800000
                                    └── HashJoin
                                        ├── type: inner
                                        ├── cond: true
                                        ├── lkey: [ #16 ]
                                        ├── rkey: [ #6 ]
                                        ├── cost: 54126.516
                                        ├── rows: 10000
                                        ├── Projection { exprs: [ #16 ], cost: 152.29703, rows: 25 }
                                        │   └── HashJoin
                                        │       ├── type: inner
                                        │       ├── cond: true
                                        │       ├── lkey: [ #25 ]
                                        │       ├── rkey: [ #15 ]
                                        │       ├── cost: 151.79703
                                        │       ├── rows: 25
                                        │       ├── Projection { exprs: [ #25 ], cost: 17.25, rows: 2.5 }
                                        │       │   └── Filter { cond: (#24 = 'EUROPE'), cost: 17.2, rows: 2.5 }
                                        │       │       └── Projection
                                        │       │           ├── exprs: [ r_regionkey' as #25, r_name' as #24 ]
                                        │       │           ├── cost: 11.15
                                        │       │           ├── rows: 5
                                        │       │           └── Scan
                                        │       │               ├── table: region
                                        │       │               ├── list: [ r_regionkey, r_name ]
                                        │       │               ├── filter: true
                                        │       │               ├── cost: 10
                                        │       │               └── rows: 5
                                        │       └── Projection
                                        │           ├── exprs: [ n_nationkey' as #16, n_regionkey' as #15 ]
                                        │           ├── cost: 55.75
                                        │           ├── rows: 25
                                        │           └── Scan
                                        │               ├── table: nation
                                        │               ├── list: [ n_nationkey, n_regionkey ]
                                        │               ├── filter: true
                                        │               ├── cost: 50
                                        │               └── rows: 25
                                        └── Projection
                                            ├── exprs: [ s_suppkey' as #7, s_nationkey' as #6 ]
                                            ├── cost: 22300
                                            ├── rows: 10000
                                            └── Scan
                                                ├── table: supplier
                                                ├── list: [ s_suppkey, s_nationkey ]
                                                ├── filter: true
                                                ├── cost: 20000
                                                └── rows: 10000
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
Projection { exprs: [ l_orderkey, #45, o_orderdate, o_shippriority ], cost: 72280776, rows: 10 }
└── TopN { limit: 10, offset: 0, order_by: [ #45 desc, o_orderdate ], cost: 72280776, rows: 10 }
    └── HashAgg
        ├── keys: [ l_orderkey, o_orderdate, o_shippriority ]
        ├── aggs: [ sum((l_extendedprice * (1 - l_discount))) as #45 ]
        ├── cost: 72277280
        ├── rows: 1000
        └── Projection
            ├── exprs: [ o_orderdate, o_shippriority, l_orderkey, l_extendedprice, l_discount ]
            ├── cost: 70563920
            ├── rows: 3000607.5
            └── HashJoin
                ├── type: inner
                ├── cond: true
                ├── lkey: [ o_orderkey ]
                ├── rkey: [ l_orderkey ]
                ├── cost: 70383880
                ├── rows: 3000607.5
                ├── Projection { exprs: [ o_orderkey, o_orderdate, o_shippriority ], cost: 13810606, rows: 750000 }
                │   └── HashJoin
                │       ├── type: inner
                │       ├── cond: true
                │       ├── lkey: [ c_custkey ]
                │       ├── rkey: [ o_custkey ]
                │       ├── cost: 13780606
                │       ├── rows: 750000
                │       ├── Projection { exprs: [ c_custkey ], cost: 483000, rows: 75000 }
                │       │   └── Filter { cond: (c_mktsegment = 'BUILDING'), cost: 481500, rows: 75000 }
                │       │       └── Scan
                │       │           ├── table: customer
                │       │           ├── list: [ c_custkey, c_mktsegment ]
                │       │           ├── filter: true
                │       │           ├── cost: 300000
                │       │           └── rows: 150000
                │       └── Filter { cond: (1995-03-15 > o_orderdate), cost: 9315000, rows: 750000 }
                │           └── Scan
                │               ├── table: orders
                │               ├── list: [ o_orderkey, o_custkey, o_orderdate, o_shippriority ]
                │               ├── filter: true
                │               ├── cost: 6000000
                │               └── rows: 1500000
                └── Projection { exprs: [ l_orderkey, l_extendedprice, l_discount ], cost: 37387570, rows: 3000607.5 }
                    └── Filter { cond: (l_shipdate > 1995-03-15), cost: 37267544, rows: 3000607.5 }
                        └── Scan
                            ├── table: lineitem
                            ├── list: [ l_orderkey, l_extendedprice, l_discount, l_shipdate ]
                            ├── filter: true
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
Order { by: [ o_orderpriority ], cost: 35742960, rows: 10 }
└── HashAgg { keys: [ o_orderpriority ], aggs: [ count(*) as #30 ], cost: 35742904, rows: 10 }
    └── Projection { exprs: [ o_orderpriority ], cost: 35712024, rows: 187500 }
        └── HashJoin
            ├── type: semi
            ├── cond: true
            ├── lkey: [ o_orderkey ]
            ├── rkey: [ l_orderkey ]
            ├── cost: 35708270
            ├── rows: 187500
            ├── Projection { exprs: [ o_orderkey, o_orderpriority ], cost: 6416250, rows: 375000 }
            │   └── Filter
            │       ├── cond: ((o_orderdate >= 1993-07-01) and (1993-10-01 > o_orderdate))
            │       ├── cost: 6405000
            │       ├── rows: 375000
            │       └── Scan
            │           ├── table: orders
            │           ├── list: [ o_orderkey, o_orderdate, o_orderpriority ]
            │           ├── filter: true
            │           ├── cost: 4500000
            │           └── rows: 1500000
            └── Projection { exprs: [ l_orderkey ], cost: 27785624, rows: 3000607.5 }
                └── Filter { cond: (l_receiptdate > l_commitdate), cost: 27725612, rows: 3000607.5 }
                    └── Scan
                        ├── table: lineitem
                        ├── list: [ l_orderkey, l_commitdate, l_receiptdate ]
                        ├── filter: true
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
Order { by: [ #71 desc ], cost: 81560570, rows: 10 }
└── HashAgg { keys: [ n_name ], aggs: [ sum((l_extendedprice * (1 - l_discount))) as #71 ], cost: 81560510, rows: 10 }
    └── Projection { exprs: [ l_extendedprice, l_discount, n_name ], cost: 78652340, rows: 6001215 }
        └── HashJoin
            ├── type: inner
            ├── cond: true
            ├── lkey: [ o_orderkey, s_suppkey ]
            ├── rkey: [ l_orderkey, l_suppkey ]
            ├── cost: 78412290
            ├── rows: 6001215
            ├── Projection { exprs: [ o_orderkey, s_suppkey, n_name ], cost: 10389364, rows: 375000 }
            │   └── HashJoin
            │       ├── type: inner
            │       ├── cond: true
            │       ├── lkey: [ c_custkey ]
            │       ├── rkey: [ o_custkey ]
            │       ├── cost: 10374364
            │       ├── rows: 375000
            │       ├── HashJoin
            │       │   ├── type: inner
            │       │   ├── cond: true
            │       │   ├── lkey: [ c_nationkey ]
            │       │   ├── rkey: [ s_nationkey ]
            │       │   ├── cost: 1179842.1
            │       │   ├── rows: 150000
            │       │   ├── Scan
            │       │   │   ├── table: customer
            │       │   │   ├── list: [ c_custkey, c_nationkey ]
            │       │   │   ├── filter: true
            │       │   │   ├── cost: 300000
            │       │   │   └── rows: 150000
            │       │   └── Projection { exprs: [ s_suppkey, s_nationkey, n_name ], cost: 83130.805, rows: 10000 }
            │       │       └── HashJoin
            │       │           ├── type: inner
            │       │           ├── cond: true
            │       │           ├── lkey: [ s_nationkey ]
            │       │           ├── rkey: [ n_nationkey ]
            │       │           ├── cost: 82730.805
            │       │           ├── rows: 10000
            │       │           ├── Scan
            │       │           │   ├── table: supplier
            │       │           │   ├── list: [ s_suppkey, s_nationkey ]
            │       │           │   ├── filter: true
            │       │           │   ├── cost: 20000
            │       │           │   └── rows: 10000
            │       │           └── HashJoin
            │       │               ├── type: inner
            │       │               ├── cond: true
            │       │               ├── lkey: [ n_regionkey ]
            │       │               ├── rkey: [ r_regionkey ]
            │       │               ├── cost: 195.69263
            │       │               ├── rows: 25
            │       │               ├── Scan
            │       │               │   ├── table: nation
            │       │               │   ├── list: [ n_nationkey, n_name, n_regionkey ]
            │       │               │   ├── filter: true
            │       │               │   ├── cost: 75
            │       │               │   └── rows: 25
            │       │               └── Projection { exprs: [ r_regionkey ], cost: 16.099998, rows: 2.5 }
            │       │                   └── Filter { cond: (r_name = 'AFRICA'), cost: 16.05, rows: 2.5 }
            │       │                       └── Scan
            │       │                           ├── table: region
            │       │                           ├── list: [ r_regionkey, r_name ]
            │       │                           ├── filter: true
            │       │                           ├── cost: 10
            │       │                           └── rows: 5
            │       └── Projection { exprs: [ o_orderkey, o_custkey ], cost: 6416250, rows: 375000 }
            │           └── Filter
            │               ├── cond: ((1995-01-01 > o_orderdate) and (o_orderdate >= 1994-01-01))
            │               ├── cost: 6405000
            │               ├── rows: 375000
            │               └── Scan
            │                   ├── table: orders
            │                   ├── list: [ o_orderkey, o_custkey, o_orderdate ]
            │                   ├── filter: true
            │                   ├── cost: 4500000
            │                   └── rows: 1500000
            └── Scan
                ├── table: lineitem
                ├── list: [ l_orderkey, l_suppkey, l_extendedprice, l_discount ]
                ├── filter: true
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
Agg { aggs: [ sum((l_discount * l_extendedprice)) as #26 ], cost: 32817268, rows: 1 }
└── Filter { cond: ((0.09 >= l_discount) and (l_discount >= 0.07)), cost: 32774134, rows: 187537.97 }
    └── Projection { exprs: [ l_extendedprice, l_discount ], cost: 32008980, rows: 750151.9 }
        └── Filter
            ├── cond: ((24 > l_quantity) and ((1995-01-01 > l_shipdate) and (l_shipdate >= 1994-01-01)))
            ├── cost: 31986476
            ├── rows: 750151.9
            └── Scan
                ├── table: lineitem
                ├── list: [ l_quantity, l_extendedprice, l_discount, l_shipdate ]
                ├── filter: true
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
Order { by: [ n_name, #11, #72 ], cost: 82596740, rows: 1000 }
└── HashAgg { keys: [ n_name, #11, #72 ], aggs: [ sum(#70) as #76 ], cost: 82582770, rows: 1000 }
    └── Projection
        ├── exprs: [ n_name, #11, extract(YEAR from l_shipdate) as #72, (l_extendedprice * (1 - l_discount)) as #70 ]
        ├── cost: 82189180
        ├── rows: 1500303.8
        └── HashJoin
            ├── type: inner
            ├── cond: true
            ├── lkey: [ l_suppkey, l_orderkey ]
            ├── rkey: [ s_suppkey, o_orderkey ]
            ├── cost: 81349016
            ├── rows: 1500303.8
            ├── Filter { cond: ((1996-12-31 >= l_shipdate) and (l_shipdate >= 1995-01-01)), cost: 40628228, rows: 1500303.8 }
            │   └── Scan
            │       ├── table: lineitem
            │       ├── list: [ l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate ]
            │       ├── filter: true
            │       ├── cost: 30006076
            │       └── rows: 6001215
            └── HashJoin { type: inner, cond: true, lkey: [ s_nationkey ], rkey: [ n_nationkey ], cost: 23211842, rows: 1500000 }
                ├── Scan { table: supplier, list: [ s_suppkey, s_nationkey ], filter: true, cost: 20000, rows: 10000 }
                └── Projection { exprs: [ n_nationkey, n_name, #11, o_orderkey ], cost: 13809995, rows: 1500000 }
                    └── HashJoin { type: inner, cond: true, lkey: [ c_custkey ], rkey: [ o_custkey ], cost: 13734995, rows: 1500000 }
                        ├── Projection { exprs: [ c_custkey, n_nationkey, n_name, #11 ], cost: 1253283.8, rows: 150000 }
                        │   └── HashJoin
                        │       ├── type: inner
                        │       ├── cond: true
                        │       ├── lkey: [ c_nationkey ]
                        │       ├── rkey: [ #12 ]
                        │       ├── cost: 1245783.8
                        │       ├── rows: 150000
                        │       ├── Scan { table: customer, list: [ c_custkey, c_nationkey ], filter: true, cost: 300000, rows: 150000 }
                        │       └── Join
                        │           ├── type: inner
                        │           ├── on: (((#11 = 'GERMANY') and (n_name = 'FRANCE')) or (('FRANCE' = #11) and (n_name = 'GERMANY')))
                        │           ├── cost: 1912
                        │           ├── rows: 273.4375
                        │           ├── Projection { exprs: [ n_nationkey' as #12, n_name' as #11 ], cost: 55.75, rows: 25 }
                        │           │   └── Scan { table: nation, list: [ n_nationkey, n_name ], filter: true, cost: 50, rows: 25 }
                        │           └── Scan { table: nation, list: [ n_nationkey, n_name ], filter: true, cost: 50, rows: 25 }
                        └── Scan { table: orders, list: [ o_orderkey, o_custkey ], filter: true, cost: 3000000, rows: 1500000 }
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
Projection { exprs: [ #95, (#106 / #99) as #114 ], cost: 224687780, rows: 10 }
└── Order { by: [ #95 ], cost: 224687780, rows: 10 }
    └── HashAgg
        ├── keys: [ #95 ]
        ├── aggs: [ sum((if (#77 = 'BRAZIL') then #93 else Cast { type: 0 })) as #106, sum(#93) as #99 ]
        ├── cost: 224687710
        ├── rows: 10
        └── Projection
            ├── exprs: [ extract(YEAR from o_orderdate) as #95, (l_extendedprice * (1 - l_discount)) as #93, #77 ]
            ├── cost: 219319020
            ├── rows: 6001215
            └── Projection { exprs: [ l_extendedprice, l_discount, o_orderdate, #77 ], cost: 216018350, rows: 6001215 }
                └── HashJoin
                    ├── type: inner
                    ├── cond: true
                    ├── lkey: [ #78, n_nationkey ]
                    ├── rkey: [ s_nationkey, c_nationkey ]
                    ├── cost: 215718290
                    ├── rows: 6001215
                    ├── Projection { exprs: [ n_nationkey, #78, #77 ], cost: 3413.147, rows: 625 }
                    │   └── Join { type: inner, cost: 3388.147, rows: 625 }
                    │       ├── Projection { exprs: [ n_nationkey' as #78, n_name' as #77 ], cost: 55.75, rows: 25 }
                    │       │   └── Scan
                    │       │       ├── table: nation
                    │       │       ├── list: [ n_nationkey, n_name ]
                    │       │       ├── filter: true
                    │       │       ├── cost: 50
                    │       │       └── rows: 25
                    │       └── HashJoin
                    │           ├── type: inner
                    │           ├── cond: true
                    │           ├── lkey: [ r_regionkey ]
                    │           ├── rkey: [ n_regionkey ]
                    │           ├── cost: 144.89702
                    │           ├── rows: 25
                    │           ├── Projection { exprs: [ r_regionkey ], cost: 16.099998, rows: 2.5 }
                    │           │   └── Filter { cond: (r_name = 'AMERICA'), cost: 16.05, rows: 2.5 }
                    │           │       └── Scan
                    │           │           ├── table: region
                    │           │           ├── list: [ r_regionkey, r_name ]
                    │           │           ├── filter: true
                    │           │           ├── cost: 10
                    │           │           └── rows: 5
                    │           └── Scan
                    │               ├── table: nation
                    │               ├── list: [ n_nationkey, n_regionkey ]
                    │               ├── filter: true
                    │               ├── cost: 50
                    │               └── rows: 25
                    └── Projection
                        ├── exprs: [ s_nationkey, l_extendedprice, l_discount, o_orderdate, c_nationkey ]
                        ├── cost: 166367340
                        ├── rows: 6001215
                        └── HashJoin
                            ├── type: inner
                            ├── cond: true
                            ├── lkey: [ o_orderkey ]
                            ├── rkey: [ l_orderkey ]
                            ├── cost: 166007260
                            ├── rows: 6001215
                            ├── Projection
                            │   ├── exprs: [ o_orderkey, o_orderdate, c_nationkey ]
                            │   ├── cost: 8748272
                            │   ├── rows: 375000
                            │   └── HashJoin
                            │       ├── type: inner
                            │       ├── cond: true
                            │       ├── lkey: [ c_custkey ]
                            │       ├── rkey: [ o_custkey ]
                            │       ├── cost: 8733272
                            │       ├── rows: 375000
                            │       ├── Scan
                            │       │   ├── table: customer
                            │       │   ├── list: [ c_custkey, c_nationkey ]
                            │       │   ├── filter: true
                            │       │   ├── cost: 300000
                            │       │   └── rows: 150000
                            │       └── Filter
                            │           ├── cond: ((1996-12-31 >= o_orderdate) and (o_orderdate >= 1995-01-01))
                            │           ├── cost: 6405000
                            │           ├── rows: 375000
                            │           └── Scan
                            │               ├── table: orders
                            │               ├── list: [ o_orderkey, o_custkey, o_orderdate ]
                            │               ├── filter: true
                            │               ├── cost: 4500000
                            │               └── rows: 1500000
                            └── Projection
                                ├── exprs: [ s_nationkey, l_orderkey, l_extendedprice, l_discount ]
                                ├── cost: 113304690
                                ├── rows: 6001215
                                └── HashJoin
                                    ├── type: inner
                                    ├── cond: true
                                    ├── lkey: [ s_suppkey ]
                                    ├── rkey: [ l_suppkey ]
                                    ├── cost: 113004620
                                    ├── rows: 6001215
                                    ├── Scan
                                    │   ├── table: supplier
                                    │   ├── list: [ s_suppkey, s_nationkey ]
                                    │   ├── filter: true
                                    │   ├── cost: 20000
                                    │   └── rows: 10000
                                    └── Projection
                                        ├── exprs: [ l_orderkey, l_suppkey, l_extendedprice, l_discount ]
                                        ├── cost: 75457230
                                        ├── rows: 6001215
                                        └── HashJoin
                                            ├── type: inner
                                            ├── cond: true
                                            ├── lkey: [ l_partkey, 'ECONOMY ANODIZED STEEL' as #14 ]
                                            ├── rkey: [ p_partkey, p_type ]
                                            ├── cost: 75157170
                                            ├── rows: 6001215
                                            ├── Scan
                                            │   ├── table: lineitem
                                            │   ├── list:
                                            │   │   ┌── l_orderkey
                                            │   │   ├── l_partkey
                                            │   │   ├── l_suppkey
                                            │   │   ├── l_extendedprice
                                            │   │   └── l_discount
                                            │   ├── filter: true
                                            │   ├── cost: 30006076
                                            │   └── rows: 6001215
                                            └── Scan
                                                ├── table: part
                                                ├── list: [ p_partkey, p_type ]
                                                ├── filter: true
                                                ├── cost: 400000
                                                └── rows: 200000
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
Order { by: [ n_name, #68 desc ], cost: 267320370, rows: 100 }
└── HashAgg { keys: [ n_name, #68 ], aggs: [ sum(#66) as #72 ], cost: 267319410, rows: 100 }
    └── Projection
        ├── exprs:
        │   ┌── n_name
        │   ├── extract(YEAR from o_orderdate) as #68
        │   └── ((l_extendedprice * (1 - l_discount)) - (ps_supplycost * l_quantity)) as #66
        ├── cost: 266019360
        ├── rows: 6001215
        └── HashJoin
            ├── type: inner
            ├── cond: true
            ├── lkey: [ o_orderkey ]
            ├── rkey: [ l_orderkey ]
            ├── cost: 261398430
            ├── rows: 6001215
            ├── Scan { table: orders, list: [ o_orderkey, o_orderdate ], filter: true, cost: 3000000, rows: 1500000 }
            └── Projection
                ├── exprs: [ l_orderkey, l_quantity, l_extendedprice, l_discount, ps_supplycost, n_name ]
                ├── cost: 207949570
                ├── rows: 6001215
                └── HashJoin
                    ├── type: inner
                    ├── cond: true
                    ├── lkey: [ s_suppkey ]
                    ├── rkey: [ l_suppkey ]
                    ├── cost: 207529490
                    ├── rows: 6001215
                    ├── HashJoin
                    │   ├── type: inner
                    │   ├── cond: true
                    │   ├── lkey: [ n_nationkey ]
                    │   ├── rkey: [ s_nationkey ]
                    │   ├── cost: 61724.22
                    │   ├── rows: 10000
                    │   ├── Scan { table: nation, list: [ n_nationkey, n_name ], filter: true, cost: 50, rows: 25 }
                    │   └── Scan
                    │       ├── table: supplier
                    │       ├── list: [ s_suppkey, s_nationkey ]
                    │       ├── filter: true
                    │       ├── cost: 20000
                    │       └── rows: 10000
                    └── Projection
                        ├── exprs: [ l_orderkey, l_suppkey, l_quantity, l_extendedprice, l_discount, ps_supplycost ]
                        ├── cost: 145935500
                        ├── rows: 6001215
                        └── HashJoin
                            ├── type: inner
                            ├── cond: true
                            ├── lkey: [ p_partkey ]
                            ├── rkey: [ l_partkey ]
                            ├── cost: 145515420
                            ├── rows: 6001215
                            ├── Projection { exprs: [ p_partkey ], cost: 644000, rows: 100000 }
                            │   └── Filter { cond: (p_name like '%green%'), cost: 642000, rows: 100000 }
                            │       └── Scan
                            │           ├── table: part
                            │           ├── list: [ p_partkey, p_name ]
                            │           ├── filter: true
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
                                ├── cost: 95116180
                                ├── rows: 6001215
                                └── HashJoin
                                    ├── type: inner
                                    ├── cond: true
                                    ├── lkey: [ ps_suppkey, ps_partkey ]
                                    ├── rkey: [ l_suppkey, l_partkey ]
                                    ├── cost: 94636080
                                    ├── rows: 6001215
                                    ├── Scan
                                    │   ├── table: partsupp
                                    │   ├── list: [ ps_partkey, ps_suppkey, ps_supplycost ]
                                    │   ├── filter: true
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
                                        ├── filter: true
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
├── exprs: [ c_custkey, c_name, #61, c_acctbal, n_name, c_address, c_phone, c_comment ]
├── cost: 123507340
├── rows: 20
└── TopN { limit: 20, offset: 0, order_by: [ #61 desc ], cost: 123507340, rows: 20 }
    └── HashAgg
        ├── keys: [ c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ]
        ├── aggs: [ sum((l_extendedprice * (1 - l_discount))) as #61 ]
        ├── cost: 110327560
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
            ├── cost: 84146750
            ├── rows: 3000607.5
            └── HashJoin
                ├── type: inner
                ├── cond: true
                ├── lkey: [ o_orderkey ]
                ├── rkey: [ l_orderkey ]
                ├── cost: 83846690
                ├── rows: 3000607.5
                ├── Projection
                │   ├── exprs: [ n_name, c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, o_orderkey ]
                │   ├── cost: 12422317
                │   ├── rows: 375000
                │   └── HashJoin
                │       ├── type: inner
                │       ├── cond: true
                │       ├── lkey: [ o_custkey ]
                │       ├── rkey: [ c_custkey ]
                │       ├── cost: 12388567
                │       ├── rows: 375000
                │       ├── Projection { exprs: [ o_orderkey, o_custkey ], cost: 6416250, rows: 375000 }
                │       │   └── Filter
                │       │       ├── cond: ((1994-01-01 > o_orderdate) and (o_orderdate >= 1993-10-01))
                │       │       ├── cost: 6405000
                │       │       ├── rows: 375000
                │       │       └── Scan
                │       │           ├── table: orders
                │       │           ├── list: [ o_orderkey, o_custkey, o_orderdate ]
                │       │           ├── filter: true
                │       │           ├── cost: 4500000
                │       │           └── rows: 1500000
                │       └── Projection
                │           ├── exprs: [ c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, n_name ]
                │           ├── cost: 2437105
                │           ├── rows: 150000
                │           └── HashJoin
                │               ├── type: inner
                │               ├── cond: true
                │               ├── lkey: [ n_nationkey ]
                │               ├── rkey: [ c_nationkey ]
                │               ├── cost: 2425105
                │               ├── rows: 150000
                │               ├── Scan
                │               │   ├── table: nation
                │               │   ├── list: [ n_nationkey, n_name ]
                │               │   ├── filter: true
                │               │   ├── cost: 50
                │               │   └── rows: 25
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
                │                   ├── filter: true
                │                   ├── cost: 1050000
                │                   └── rows: 150000
                └── Projection { exprs: [ l_orderkey, l_extendedprice, l_discount ], cost: 37387570, rows: 3000607.5 }
                    └── Filter { cond: (l_returnflag = 'R'), cost: 37267544, rows: 3000607.5 }
                        └── Scan
                            ├── table: lineitem
                            ├── list: [ l_orderkey, l_extendedprice, l_discount, l_returnflag ]
                            ├── filter: true
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
Order { by: [ #66 desc ], cost: 13687827, rows: 5 }
└── Filter { cond: (#66 > #78), cost: 13687804, rows: 5 }
    └── HashAgg { keys: [ ps_partkey ], aggs: [ sum((ps_supplycost * ps_availqty)) as #66 ], cost: 13687793, rows: 10 }
        └── Projection { exprs: [ ps_partkey, ps_availqty, ps_supplycost ], cost: 13460098, rows: 800000 }
            └── HashJoin
                ├── type: inner
                ├── cond: true
                ├── lkey: [ s_suppkey ]
                ├── rkey: [ ps_suppkey ]
                ├── cost: 13428098
                ├── rows: 800000
                ├── Projection { exprs: [ s_suppkey, s_nationkey, n_nationkey ], cost: 4423267, rows: 10000 }
                │   └── HashJoin
                │       ├── type: inner
                │       ├── cond: true
                │       ├── lkey: [ n_nationkey ]
                │       ├── rkey: [ s_nationkey ]
                │       ├── cost: 4422867
                │       ├── rows: 10000
                │       ├── Join { type: inner, cost: 4371289.5, rows: 12.5 }
                │       │   ├── Agg { aggs: [], cost: 4371195.5, rows: 1 }
                │       │   │   └── Projection { exprs: [], cost: 4363195.5, rows: 800000 }
                │       │   │       └── HashJoin
                │       │   │           ├── type: inner
                │       │   │           ├── cond: true
                │       │   │           ├── lkey: [ #29 ]
                │       │   │           ├── rkey: [ #25 ]
                │       │   │           ├── cost: 4355195.5
                │       │   │           ├── rows: 800000
                │       │   │           ├── Projection { exprs: [ #29, #28, #33 ], cost: 54363.71, rows: 10000 }
                │       │   │           │   └── HashJoin
                │       │   │           │       ├── type: inner
                │       │   │           │       ├── cond: true
                │       │   │           │       ├── lkey: [ #33 ]
                │       │   │           │       ├── rkey: [ #28 ]
                │       │   │           │       ├── cost: 53963.71
                │       │   │           │       ├── rows: 10000
                │       │   │           │       ├── Projection { exprs: [ #33 ], cost: 86.25, rows: 12.5 }
                │       │   │           │       │   └── Filter { cond: (#32 = 'GERMANY'), cost: 86, rows: 12.5 }
                │       │   │           │       │       └── Projection
                │       │   │           │       │           ├── exprs: [ n_nationkey' as #33, n_name' as #32 ]
                │       │   │           │       │           ├── cost: 55.75
                │       │   │           │       │           ├── rows: 25
                │       │   │           │       │           └── Scan
                │       │   │           │       │               ├── table: nation
                │       │   │           │       │               ├── list: [ n_nationkey, n_name ]
                │       │   │           │       │               ├── filter: true
                │       │   │           │       │               ├── cost: 50
                │       │   │           │       │               └── rows: 25
                │       │   │           │       └── Projection
                │       │   │           │           ├── exprs: [ s_suppkey' as #29, s_nationkey' as #28 ]
                │       │   │           │           ├── cost: 22300
                │       │   │           │           ├── rows: 10000
                │       │   │           │           └── Scan
                │       │   │           │               ├── table: supplier
                │       │   │           │               ├── list: [ s_suppkey, s_nationkey ]
                │       │   │           │               ├── filter: true
                │       │   │           │               ├── cost: 20000
                │       │   │           │               └── rows: 10000
                │       │   │           └── Projection { exprs: [ ps_suppkey' as #25 ], cost: 896000, rows: 800000 }
                │       │   │               └── Scan
                │       │   │                   ├── table: partsupp
                │       │   │                   ├── list: [ ps_suppkey ]
                │       │   │                   ├── filter: true
                │       │   │                   ├── cost: 800000
                │       │   │                   └── rows: 800000
                │       │   └── Projection { exprs: [ n_nationkey ], cost: 80.5, rows: 12.5 }
                │       │       └── Filter { cond: (n_name = 'GERMANY'), cost: 80.25, rows: 12.5 }
                │       │           └── Scan
                │       │               ├── table: nation
                │       │               ├── list: [ n_nationkey, n_name ]
                │       │               ├── filter: true
                │       │               ├── cost: 50
                │       │               └── rows: 25
                │       └── Scan
                │           ├── table: supplier
                │           ├── list: [ s_suppkey, s_nationkey ]
                │           ├── filter: true
                │           ├── cost: 20000
                │           └── rows: 10000
                └── Scan
                    ├── table: partsupp
                    ├── list: [ ps_partkey, ps_suppkey, ps_availqty, ps_supplycost ]
                    ├── filter: true
                    ├── cost: 3200000
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
Order { by: [ l_shipmode ], cost: 50998250, rows: 10 }
└── HashAgg
    ├── keys: [ l_shipmode ]
    ├── aggs:
    │   ┌── sum((if ((o_orderpriority = '2-HIGH') or (o_orderpriority = '1-URGENT')) then 1 else 0)) as #50
    │   └── sum((if ((o_orderpriority <> '2-HIGH') and (o_orderpriority <> '1-URGENT')) then 1 else 0)) as #45
    ├── cost: 50998184
    ├── rows: 10
    └── Projection { exprs: [ o_orderpriority, l_shipmode ], cost: 48141264, rows: 1500000 }
        └── HashJoin { type: inner, cond: true, lkey: [ l_orderkey ], rkey: [ o_orderkey ], cost: 48096264, rows: 1500000 }
            ├── Filter
            │   ├── cond:In { in: [ 'MAIL' as #27, 'SHIP' as #26 ] }
            │   │   └── l_shipmode
            │   ├── cost: 38524052
            │   ├── rows: 375075.94
            │   └── Projection { exprs: [ l_orderkey, l_shipmode ], cost: 37653876, rows: 375075.94 }
            │       └── Filter
            │           ├── cond: ((1995-01-01 > l_receiptdate) and ((l_commitdate > l_shipdate) and ((l_receiptdate > l_commitdate) and (l_receiptdate >= 1994-01-01))))
            │           ├── cost: 37642624
            │           ├── rows: 375075.94
            │           └── Scan
            │               ├── table: lineitem
            │               ├── list: [ l_orderkey, l_shipdate, l_commitdate, l_receiptdate, l_shipmode ]
            │               ├── filter: true
            │               ├── cost: 30006076
            │               └── rows: 6001215
            └── Scan { table: orders, list: [ o_orderkey, o_orderpriority ], filter: true, cost: 3000000, rows: 1500000 }
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
Order { by: [ #25 desc, #22 desc ], cost: 10053795, rows: 10 }
└── HashAgg { keys: [ #22 ], aggs: [ count(*) as #25 ], cost: 10053740, rows: 10 }
    └── HashAgg { keys: [ c_custkey ], aggs: [ count(o_orderkey) as #22 ], cost: 10053718, rows: 10 }
        └── Projection { exprs: [ c_custkey, o_orderkey ], cost: 9922752, rows: 750000 }
            └── HashJoin
                ├── type: left_outer
                ├── cond: true
                ├── lkey: [ c_custkey ]
                ├── rkey: [ o_custkey ]
                ├── cost: 9900252
                ├── rows: 750000
                ├── Scan { table: customer, list: [ c_custkey ], filter: true, cost: 150000, rows: 150000 }
                └── Projection { exprs: [ o_orderkey, o_custkey ], cost: 7237500, rows: 750000 }
                    └── Filter { cond: (not (o_comment like '%special%requests%')), cost: 7215000, rows: 750000 }
                        └── Scan
                            ├── table: orders
                            ├── list: [ o_orderkey, o_custkey, o_comment ]
                            ├── filter: true
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
Projection { exprs: [ ((100.00 * #37) / #30) as #44 ], cost: 43842148, rows: 1 }
└── Agg
    ├── aggs:
    │   ┌── sum((if (p_type like 'PROMO%') then (l_extendedprice * (1 - l_discount)) else Cast { type: 0 })) as #37
    │   └── sum((l_extendedprice * (1 - l_discount))) as #30
    ├── cost: 43842148
    ├── rows: 1
    └── Projection { exprs: [ l_extendedprice, l_discount, p_type ], cost: 41651704, rows: 1500303.8 }
        └── HashJoin
            ├── type: inner
            ├── cond: true
            ├── lkey: [ p_partkey ]
            ├── rkey: [ l_partkey ]
            ├── cost: 41591692
            ├── rows: 1500303.8
            ├── Scan { table: part, list: [ p_partkey, p_type ], filter: true, cost: 400000, rows: 200000 }
            └── Projection { exprs: [ l_partkey, l_extendedprice, l_discount ], cost: 33186720, rows: 1500303.8 }
                └── Filter
                    ├── cond: ((l_shipdate >= 1995-09-01) and (1995-10-01 > l_shipdate))
                    ├── cost: 33126708
                    ├── rows: 1500303.8
                    └── Scan
                        ├── table: lineitem
                        ├── list: [ l_partkey, l_extendedprice, l_discount, l_shipdate ]
                        ├── filter: true
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
1Order { by: [ s_suppkey ], cost: 300994.8, rows: 10000 }
└── Projection { exprs: [ s_suppkey, s_name, s_address, s_phone, total_revenue ], cost: 118116.23, rows: 10000 }
    └── HashJoin { type: inner, cond: true, lkey: [ s_suppkey ], rkey: [ supplier_no ], cost: 117516.23, rows: 10000 }
        ├── Scan
        │   ├── table: supplier
        │   ├── list: [ s_suppkey, s_name, s_address, s_phone ]
        │   ├── filter: true
        │   ├── cost: 40000
        │   └── rows: 10000
        └── Join { type: inner, on: (total_revenue = #12), cost: 4861, rows: 500 }
            ├── Agg { aggs: [ max(#8) as #12 ], cost: 1241, rows: 1 }
            │   └── Projection { exprs: [ total_revenue' as #8 ], cost: 1120, rows: 1000 }
            │       └── Scan { table: revenue0, list: [ total_revenue ], filter: true, cost: 1000, rows: 1000 }
            └── Scan { table: revenue0, list: [ supplier_no, total_revenue ], filter: true, cost: 2000, rows: 1000 }
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
Order { by: [ #50 desc, p_brand, p_type, p_size ], cost: 9952236, rows: 1000 }
└── HashAgg { keys: [ p_brand, p_type, p_size ], aggs: [ count_distinct(ps_suppkey) as #50 ], cost: 9938269, rows: 1000 }
    └── HashJoin { type: anti, cond: true, lkey: [ ps_suppkey ], rkey: [ s_suppkey ], cost: 9830400, rows: 400000 }
        ├── Projection { exprs: [ ps_suppkey, p_brand, p_type, p_size ], cost: 8002682, rows: 800000 }
        │   └── HashJoin { type: inner, cond: true, lkey: [ p_partkey ], rkey: [ ps_partkey ], cost: 7962682, rows: 800000 }
        │       ├── Filter
        │       │   ├── cond: ((not (p_type like 'MEDIUM POLISHED%')) and ((p_brand <> 'Brand#45') and In { in: [ 49 as #30, 14 as #29, 23 as #28, 45 as #27, 19 as #26, 3 as #25, 36 as #24, 9 as #23 ] }))
        │       │   ├── cost: 1328000
        │       │   ├── rows: 50000
        │       │   └── Scan { table: part, list: [ p_partkey, p_brand, p_type, p_size ], filter: true, cost: 800000, rows: 200000 }
        │       └── Scan { table: partsupp, list: [ ps_partkey, ps_suppkey ], filter: true, cost: 1600000, rows: 800000 }
        └── Projection { exprs: [ s_suppkey ], cost: 32200, rows: 5000 }
            └── Filter { cond: (s_comment like '%Customer%Complaints%'), cost: 32100, rows: 5000 }
                └── Scan { table: supplier, list: [ s_suppkey, s_comment ], filter: true, cost: 20000, rows: 10000 }
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
Projection { exprs: [ (#72 / 7.0) as #77 ], cost: 610238400, rows: 1 }
└── Agg { aggs: [ sum(l_extendedprice) as #72 ], cost: 610238400, rows: 1 }
    └── Projection { exprs: [ l_extendedprice ], cost: 609878340, rows: 3000607.5 }
        └── Filter { cond: (#64 > l_quantity), cost: 609818300, rows: 3000607.5 }
            └── Projection
                ├── exprs: [ l_quantity, l_extendedprice, ((#56 / #55) * 0.2) as #64 ]
                ├── cost: 600096300
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
                    ├── aggs: [ sum(#6) as #56, count(#6) as #55 ]
                    ├── cost: 597995900
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
                        │   └── #6
                        ├── cost: 431671200
                        ├── rows: 6001215
                        └── HashJoin
                            ├── type: left_outer
                            ├── cond: true
                            ├── lkey: [ p_partkey ]
                            ├── rkey: [ #7 ]
                            ├── cost: 430050880
                            ├── rows: 6001215
                            ├── HashJoin
                            │   ├── type: inner
                            │   ├── cond: true
                            │   ├── lkey: [ l_partkey ]
                            │   ├── rkey: [ p_partkey ]
                            │   ├── cost: 250492500
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
                            │   │   ├── filter: true
                            │   │   ├── cost: 96019440
                            │   │   └── rows: 6001215
                            │   └── Filter
                            │       ├── cond: ((p_container = 'MED BOX') and (p_brand = 'Brand#23'))
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
                            │           ├── filter: true
                            │           ├── cost: 1800000
                            │           └── rows: 200000
                            └── Projection
                                ├── exprs: [ l_partkey' as #7, l_quantity' as #6 ]
                                ├── cost: 13382709
                                ├── rows: 6001215
                                └── Scan
                                    ├── table: lineitem
                                    ├── list: [ l_partkey, l_quantity ]
                                    ├── filter: true
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
TopN { limit: 100, offset: 0, order_by: [ o_totalprice desc, o_orderdate ], cost: 108356230, rows: 100 }
└── HashAgg
    ├── keys: [ c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ]
    ├── aggs: [ sum(l_quantity) as #46 ]
    ├── cost: 107689810
    ├── rows: 100000
    └── HashJoin { type: semi, cond: true, lkey: [ o_orderkey ], rkey: [ #7 ], cost: 106051310, rows: 3000607.5 }
        ├── Projection
        │   ├── exprs: [ c_custkey, c_name, o_orderkey, o_totalprice, o_orderdate, l_quantity ]
        │   ├── cost: 72741870
        │   ├── rows: 6001215
        │   └── HashJoin
        │       ├── type: inner
        │       ├── cond: true
        │       ├── lkey: [ o_orderkey ]
        │       ├── rkey: [ l_orderkey ]
        │       ├── cost: 72321784
        │       ├── rows: 6001215
        │       ├── Projection
        │       │   ├── exprs: [ c_custkey, c_name, o_orderkey, o_totalprice, o_orderdate ]
        │       │   ├── cost: 15871711
        │       │   ├── rows: 1500000
        │       │   └── HashJoin
        │       │       ├── type: inner
        │       │       ├── cond: true
        │       │       ├── lkey: [ c_custkey ]
        │       │       ├── rkey: [ o_custkey ]
        │       │       ├── cost: 15781711
        │       │       ├── rows: 1500000
        │       │       ├── Scan
        │       │       │   ├── table: customer
        │       │       │   ├── list: [ c_custkey, c_name ]
        │       │       │   ├── filter: true
        │       │       │   ├── cost: 300000
        │       │       │   └── rows: 150000
        │       │       └── Scan
        │       │           ├── table: orders
        │       │           ├── list: [ o_orderkey, o_custkey, o_totalprice, o_orderdate ]
        │       │           ├── filter: true
        │       │           ├── cost: 6000000
        │       │           └── rows: 1500000
        │       └── Scan
        │           ├── table: lineitem
        │           ├── list: [ l_orderkey, l_quantity ]
        │           ├── filter: true
        │           ├── cost: 12002430
        │           └── rows: 6001215
        └── Projection { exprs: [ #7 ], cost: 14430519, rows: 5 }
            └── Filter { cond: (#11 > 300), cost: 14430519, rows: 5 }
                └── HashAgg { keys: [ #7 ], aggs: [ sum(#6) as #11 ], cost: 14430507, rows: 10 }
                    └── Projection { exprs: [ l_orderkey' as #7, l_quantity' as #6 ], cost: 13382709, rows: 6001215 }
                        └── Scan
                            ├── table: lineitem
                            ├── list: [ l_orderkey, l_quantity ]
                            ├── filter: true
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
Agg { aggs: [ sum((l_extendedprice * (1 - l_discount))) as #94 ], cost: 104141096, rows: 1 }
└── Projection { exprs: [ l_extendedprice, l_discount ], cost: 103913976, rows: 528183.1 }
    └── Filter
        ├── cond: ((((((l_quantity >= 10) and In { in: [ 'MED BAG' as #80, 'MED BOX' as #79, 'MED PKG' as #78, 'MED PACK' as #77 ] }) and (p_brand = 'Brand#23')) and ((10 >= p_size) and (20 >= l_quantity))) or ((((l_quantity >= 1) and In { in: [ 'SM CASE' as #64, 'SM BOX' as #63, 'SM PACK' as #62, 'SM PKG' as #61 ] }) and (p_brand = 'Brand#12')) and ((5 >= p_size) and (11 >= l_quantity)))) or ((15 >= p_size) and ((((30 >= l_quantity) and (l_quantity >= 20)) and In { in: [ 'LG CASE' as #41, 'LG BOX' as #40, 'LG PACK' as #39, 'LG PKG' as #38 ] }) and (p_brand = 'Brand#33'))))
        ├── cost: 103898130
        ├── rows: 528183.1
        └── Projection { exprs: [ l_quantity, l_extendedprice, l_discount, p_brand, p_size, p_container ], cost: 84285704, rows: 3000607.5 }
            └── HashJoin { type: inner, cond: true, lkey: [ p_partkey ], rkey: [ l_partkey ], cost: 84075660, rows: 3000607.5 }
                ├── Filter { cond: (p_size >= 1), cost: 1242000, rows: 100000 }
                │   └── Scan { table: part, list: [ p_partkey, p_brand, p_size, p_container ], filter: true, cost: 800000, rows: 200000 }
                └── Projection { exprs: [ l_partkey, l_quantity, l_extendedprice, l_discount ], cost: 57941730, rows: 3000607.5 }
                    └── Filter { cond: (In { in: [ 'AIR' as #13, 'AIR REG' as #12 ] } and (l_shipinstruct = 'DELIVER IN PERSON')), cost: 57791696, rows: 3000607.5 }
                        └── Scan { table: lineitem, list: [ l_partkey, l_quantity, l_extendedprice, l_discount, l_shipinstruct, l_shipmode ], filter: true, cost: 36007290, rows: 6001215 }
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
Order { by: [ s_name ], cost: 52327424, rows: 5000 }
└── Projection { exprs: [ s_name, s_address ], cost: 52255984, rows: 5000 }
    └── HashJoin { type: semi, cond: true, lkey: [ s_suppkey ], rkey: [ ps_suppkey ], cost: 52255830, rows: 5000 }
        ├── Projection { exprs: [ s_suppkey, s_name, s_address ], cost: 92057.95, rows: 10000 }
        │   └── HashJoin
        │       ├── type: inner
        │       ├── cond: true
        │       ├── lkey: [ n_nationkey ]
        │       ├── rkey: [ s_nationkey ]
        │       ├── cost: 91657.95
        │       ├── rows: 10000
        │       ├── Projection { exprs: [ n_nationkey ], cost: 80.5, rows: 12.5 }
        │       │   └── Filter { cond: (n_name = 'CANADA'), cost: 80.25, rows: 12.5 }
        │       │       └── Scan { table: nation, list: [ n_nationkey, n_name ], filter: true, cost: 50, rows: 25 }
        │       └── Scan
        │           ├── table: supplier
        │           ├── list: [ s_suppkey, s_name, s_address, s_nationkey ]
        │           ├── filter: true
        │           ├── cost: 40000
        │           └── rows: 10000
        └── Projection { exprs: [ ps_suppkey ], cost: 52139460, rows: 25000 }
            └── HashJoin
                ├── type: semi
                ├── cond: true
                ├── lkey: [ ps_partkey ]
                ├── rkey: [ p_partkey ]
                ├── cost: 52138960
                ├── rows: 25000
                ├── Projection { exprs: [ ps_partkey, ps_suppkey ], cost: 51402044, rows: 50000 }
                │   └── Filter { cond: (ps_availqty > #45), cost: 51400544, rows: 50000 }
                │       └── Projection
                │           ├── exprs: [ ps_partkey, ps_suppkey, ps_availqty, (0.5 * #40) as #45 ]
                │           ├── cost: 51188544
                │           ├── rows: 100000
                │           └── HashAgg
                │               ├── keys: [ ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment ]
                │               ├── aggs: [ sum(l_quantity) as #40 ]
                │               ├── cost: 51163544
                │               ├── rows: 100000
                │               └── Projection
                │                   ├── exprs:
                │                   │   ┌── ps_partkey
                │                   │   ├── ps_suppkey
                │                   │   ├── ps_availqty
                │                   │   ├── ps_supplycost
                │                   │   ├── ps_comment
                │                   │   └── l_quantity
                │                   ├── cost: 50044292
                │                   ├── rows: 1500303.8
                │                   └── HashJoin
                │                       ├── type: left_outer
                │                       ├── cond: true
                │                       ├── lkey: [ ps_partkey, ps_suppkey ]
                │                       ├── rkey: [ l_partkey, l_suppkey ]
                │                       ├── cost: 49939270
                │                       ├── rows: 1500303.8
                │                       ├── Scan
                │                       │   ├── table: partsupp
                │                       │   ├── list: [ ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment ]
                │                       │   ├── filter: true
                │                       │   ├── cost: 4000000
                │                       │   └── rows: 800000
                │                       └── Projection
                │                           ├── exprs: [ l_partkey, l_suppkey, l_quantity ]
                │                           ├── cost: 33186720
                │                           ├── rows: 1500303.8
                │                           └── Filter
                │                               ├── cond: ((l_shipdate >= 1994-01-01) and (1995-01-01 > l_shipdate))
                │                               ├── cost: 33126708
                │                               ├── rows: 1500303.8
                │                               └── Scan
                │                                   ├── table: lineitem
                │                                   ├── list: [ l_partkey, l_suppkey, l_quantity, l_shipdate ]
                │                                   ├── filter: true
                │                                   ├── cost: 24004860
                │                                   └── rows: 6001215
                └── Projection { exprs: [ p_partkey ], cost: 644000, rows: 100000 }
                    └── Filter { cond: (p_name like 'forest%'), cost: 642000, rows: 100000 }
                        └── Scan { table: part, list: [ p_partkey, p_name ], filter: true, cost: 400000, rows: 200000 }
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
Error
bind error: not supported yet: multiple EXISTS are not supported yet
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
Order { by: [ #56 ], cost: 4369279.5, rows: 10 }
└── HashAgg { keys: [ #56 ], aggs: [ count(*) as #62, sum(c_acctbal) as #61 ], cost: 4369215, rows: 10 }
    └── Projection { exprs: [ substring(c_phone from 1 for 2) as #56, c_acctbal ], cost: 4358887.5, rows: 37500 }
        └── Filter
            ├── cond:In { in: [ '13' as #20, '31' as #19, '23' as #18, '29' as #17, '30' as #16, '18' as #15, '17' as #14 ] }
            │   └── substring(c_phone from 1 for 2)
            ├── cost: 4346512.5
            ├── rows: 37500
            └── Projection { exprs: [ c_phone, c_acctbal ], cost: 4229512.5, rows: 37500 }
                └── HashJoin { type: anti, cond: true, lkey: [ c_custkey ], rkey: [ o_custkey ], cost: 4228387.5, rows: 37500 }
                    ├── Projection { exprs: [ c_custkey, c_phone, c_acctbal ], cost: 2103752, rows: 75000 }
                    │   └── Filter { cond: (c_acctbal > #40), cost: 2100752, rows: 75000 }
                    │       └── Join { type: left_outer, cost: 1782752.1, rows: 150000 }
                    │           ├── Scan { table: customer, list: [ c_custkey, c_phone, c_acctbal ], filter: true, cost: 450000, rows: 150000 }
                    │           └── Projection { exprs: [ (#35 / #34) as #40 ], cost: 717752.1, rows: 1 }
                    │               └── Agg { aggs: [ sum(#10) as #35, count(#10) as #34 ], cost: 717752, rows: 1 }
                    │                   └── Projection { exprs: [ #10 ], cost: 700500, rows: 75000 }
                    │                       └── Filter
                    │                           ├── cond: ((#10 > 0.00) and In { in: [ '13' as #20, '31' as #19, '23' as #18, '29' as #17, '30' as #16, '18' as #15, '17' as #14 ] })
                    │                           ├── cost: 699000
                    │                           ├── rows: 75000
                    │                           └── Projection { exprs: [ c_phone' as #11, c_acctbal' as #10 ], cost: 334500, rows: 150000 }
                    │                               └── Scan { table: customer, list: [ c_phone, c_acctbal ], filter: true, cost: 300000, rows: 150000 }
                    └── Scan { table: orders, list: [ o_custkey ], filter: true, cost: 1500000, rows: 1500000 }
*/

