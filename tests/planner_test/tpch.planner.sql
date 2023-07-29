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
├── cost: 98955976
├── rows: 100
└── Order { by: [ l_returnflag, l_linestatus ], cost: 98955560, rows: 100 }
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
        ├── cost: 98954780
        ├── rows: 100
        └── Projection
            ├── exprs: [ l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus ]
            ├── cost: 21544362
            ├── rows: 3000607.5
            └── Filter { cond: >= { lhs: 1998-09-21, rhs: l_shipdate }, cost: 18363718, rows: 3000607.5 }
                └── Scan
                    ├── table: lineitem
                    ├── list: [ l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate ]
                    ├── filter: null
                    ├── cost: 4200850.5
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
├── cost: 163814260
├── rows: 10
└── TopN
    ├── limit: 10
    ├── offset: 0
    ├── order_by:
    │   ┌── desc
    │   │   └── sum
    │   │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    │   └── o_orderdate
    ├── cost: 163814240
    ├── rows: 10
    └── HashAgg
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ l_orderkey, o_orderdate, o_shippriority ]
        ├── cost: 163810780
        ├── rows: 1000
        └── Projection
            ├── exprs: [ o_orderdate, o_shippriority, l_orderkey, l_extendedprice, l_discount ]
            ├── cost: 115748980
            ├── rows: 3000607.5
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                ├── cost: 112598340
                ├── rows: 3000607.5
                ├── Projection { exprs: [ o_orderkey, o_orderdate, o_shippriority ], cost: 18845312, rows: 750000 }
                │   └── HashJoin
                │       ├── type: inner
                │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                │       ├── cost: 18072812
                │       ├── rows: 750000
                │       ├── Projection { exprs: [ c_custkey ], cost: 422250, rows: 75000 }
                │       │   └── Filter { cond: = { lhs: c_mktsegment, rhs: 'BUILDING' }, cost: 346500, rows: 75000 }
                │       │       └── Scan
                │       │           ├── table: customer
                │       │           ├── list: [ c_custkey, c_mktsegment ]
                │       │           ├── filter: null
                │       │           ├── cost: 30000
                │       │           └── rows: 150000
                │       └── Filter { cond: > { lhs: 1995-03-15, rhs: o_orderdate }, cost: 3915000, rows: 750000 }
                │           └── Scan
                │               ├── table: orders
                │               ├── list: [ o_orderkey, o_custkey, o_orderdate, o_shippriority ]
                │               ├── filter: null
                │               ├── cost: 600000
                │               └── rows: 1500000
                └── Projection { exprs: [ l_orderkey, l_extendedprice, l_discount ], cost: 18753796, rows: 3000607.5 }
                    └── Filter { cond: > { lhs: l_shipdate, rhs: 1995-03-15 }, cost: 15663171, rows: 3000607.5 }
                        └── Scan
                            ├── table: lineitem
                            ├── list: [ l_orderkey, l_extendedprice, l_discount, l_shipdate ]
                            ├── filter: null
                            ├── cost: 2400486
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
├── cost: 359300770
├── rows: 10
└── Order
    ├── by:desc
    │   └── sum
    │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    ├── cost: 359300770
    ├── rows: 10
    └── HashAgg
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ n_name ]
        ├── cost: 359300740
        ├── rows: 10
        └── Projection { exprs: [ n_name, l_extendedprice, l_discount ], cost: 302352600, rows: 6001215 }
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ r_regionkey ], rhs: [ n_regionkey ] }
                ├── cost: 296171360
                ├── rows: 6001215
                ├── Projection { exprs: [ r_regionkey ], cost: 14.075001, rows: 2.5 }
                │   └── Filter { cond: = { lhs: r_name, rhs: 'AFRICA' }, cost: 11.55, rows: 2.5 }
                │       └── Scan { table: region, list: [ r_regionkey, r_name ], filter: null, cost: 1, rows: 5 }
                └── Projection
                    ├── exprs: [ n_name, n_regionkey, l_extendedprice, l_discount ]
                    ├── cost: 282324400
                    ├── rows: 6001215
                    └── HashJoin
                        ├── type: inner
                        ├── on: = { lhs: [ n_nationkey ], rhs: [ s_nationkey ] }
                        ├── cost: 276083170
                        ├── rows: 6001215
                        ├── Scan
                        │   ├── table: nation
                        │   ├── list: [ n_nationkey, n_name, n_regionkey ]
                        │   ├── filter: null
                        │   ├── cost: 7.5
                        │   └── rows: 25
                        └── Projection
                            ├── exprs: [ s_nationkey, l_extendedprice, l_discount ]
                            ├── cost: 244273970
                            ├── rows: 6001215
                            └── Projection
                                ├── exprs: [ s_nationkey, c_nationkey, l_extendedprice, l_discount ]
                                ├── cost: 238092720
                                ├── rows: 6001215
                                └── HashJoin
                                    ├── type: inner
                                    ├── on: = { lhs: [ s_suppkey, s_nationkey ], rhs: [ l_suppkey, c_nationkey ] }
                                    ├── cost: 231851460
                                    ├── rows: 6001215
                                    ├── Scan
                                    │   ├── table: supplier
                                    │   ├── list: [ s_suppkey, s_nationkey ]
                                    │   ├── filter: null
                                    │   ├── cost: 2000
                                    │   └── rows: 10000
                                    └── Projection
                                        ├── exprs: [ c_nationkey, l_suppkey, l_extendedprice, l_discount ]
                                        ├── cost: 148372560
                                        ├── rows: 6001215
                                        └── HashJoin
                                            ├── type: inner
                                            ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                                            ├── cost: 142131300
                                            ├── rows: 6001215
                                            ├── Projection
                                            │   ├── exprs: [ c_nationkey, o_orderkey ]
                                            │   ├── cost: 18064672
                                            │   ├── rows: 375000
                                            │   └── HashJoin
                                            │       ├── type: inner
                                            │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                                            │       ├── cost: 17682172
                                            │       ├── rows: 375000
                                            │       ├── Scan
                                            │       │   ├── table: customer
                                            │       │   ├── list: [ c_custkey, c_nationkey ]
                                            │       │   ├── filter: null
                                            │       │   ├── cost: 30000
                                            │       │   └── rows: 150000
                                            │       └── Projection
                                            │           ├── exprs: [ o_orderkey, o_custkey ]
                                            │           ├── cost: 8475000
                                            │           ├── rows: 375000
                                            │           └── Filter
                                            │               ├── cond:and
                                            │               │   ├── lhs: >= { lhs: o_orderdate, rhs: 1994-01-01 }
                                            │               │   └── rhs: > { lhs: 1995-01-01, rhs: o_orderdate }
                                            │               ├── cost: 8092500
                                            │               ├── rows: 375000
                                            │               └── Scan
                                            │                   ├── table: orders
                                            │                   ├── list: [ o_orderkey, o_custkey, o_orderdate ]
                                            │                   ├── filter: null
                                            │                   ├── cost: 450000
                                            │                   └── rows: 1500000
                                            └── Scan
                                                ├── table: lineitem
                                                ├── list: [ l_orderkey, l_suppkey, l_extendedprice, l_discount ]
                                                ├── filter: null
                                                ├── cost: 2400486
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
├── cost: 87550220
├── rows: 1
└── Agg
    ├── aggs:sum
    │   └── * { lhs: l_discount, rhs: l_extendedprice }
    ├── cost: 87550220
    ├── rows: 1
    └── Projection { exprs: [ l_extendedprice, l_discount ], cost: 86983860, rows: 187537.97 }
        └── Filter
            ├── cond:and
            │   ├── lhs: and { lhs: >= { lhs: 0.09, rhs: l_discount }, rhs: >= { lhs: l_discount, rhs: 0.07 } }
            │   └── rhs:and
            │       ├── lhs: > { lhs: 24, rhs: l_quantity }
            │       └── rhs:and
            │           ├── lhs: > { lhs: 1995-01-01, rhs: l_shipdate }
            │           └── rhs: >= { lhs: l_shipdate, rhs: 1994-01-01 }
            ├── cost: 86792570
            ├── rows: 187537.97
            └── Scan
                ├── table: lineitem
                ├── list: [ l_quantity, l_extendedprice, l_discount, l_shipdate ]
                ├── filter: null
                ├── cost: 2400486
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
├── cost: 188877810
├── rows: 1000
└── Order { by: [ n_name, n_name(1), Extract { from: l_shipdate, field: YEAR } ], cost: 188876770, rows: 1000 }
    └── HashAgg
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ n_name, n_name(1), Extract { from: l_shipdate, field: YEAR } ]
        ├── cost: 188866400
        ├── rows: 1000
        └── Projection
            ├── exprs:
            │   ┌── n_name
            │   ├── n_name(1)
            │   ├── Extract { from: l_shipdate, field: YEAR }
            │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
            ├── cost: 180328270
            ├── rows: 656382.9
            └── Filter
                ├── cond:or
                │   ├── lhs: and { lhs: = { lhs: n_name, rhs: 'FRANCE' }, rhs: = { lhs: n_name(1), rhs: 'GERMANY' } }
                │   └── rhs: and { lhs: = { lhs: n_name, rhs: 'GERMANY' }, rhs: = { lhs: n_name(1), rhs: 'FRANCE' } }
                ├── cost: 176357150
                ├── rows: 656382.9
                └── HashJoin
                    ├── type: inner
                    ├── on: = { lhs: [ n_nationkey(1) ], rhs: [ c_nationkey ] }
                    ├── cost: 159334340
                    ├── rows: 1500303.8
                    ├── Scan { table: nation, list: [ n_nationkey(1), n_name(1) ], filter: null, cost: 5, rows: 25 }
                    └── Projection
                        ├── exprs: [ n_name, c_nationkey, l_extendedprice, l_discount, l_shipdate ]
                        ├── cost: 151231920
                        ├── rows: 1500303.8
                        └── HashJoin
                            ├── type: inner
                            ├── on: = { lhs: [ n_nationkey ], rhs: [ s_nationkey ] }
                            ├── cost: 149656600
                            ├── rows: 1500303.8
                            ├── Scan { table: nation, list: [ n_nationkey, n_name ], filter: null, cost: 5, rows: 25 }
                            └── Projection
                                ├── exprs: [ s_nationkey, c_nationkey, l_extendedprice, l_discount, l_shipdate ]
                                ├── cost: 141554190
                                ├── rows: 1500303.8
                                └── HashJoin
                                    ├── type: inner
                                    ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                                    ├── cost: 139978880
                                    ├── rows: 1500303.8
                                    ├── Scan
                                    │   ├── table: customer
                                    │   ├── list: [ c_custkey, c_nationkey ]
                                    │   ├── filter: null
                                    │   ├── cost: 30000
                                    │   └── rows: 150000
                                    └── Projection
                                        ├── exprs: [ s_nationkey, o_custkey, l_extendedprice, l_discount, l_shipdate ]
                                        ├── cost: 110522330
                                        ├── rows: 1500303.8
                                        └── HashJoin
                                            ├── type: inner
                                            ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                                            ├── cost: 108947010
                                            ├── rows: 1500303.8
                                            ├── Scan
                                            │   ├── table: orders
                                            │   ├── list: [ o_orderkey, o_custkey ]
                                            │   ├── filter: null
                                            │   ├── cost: 300000
                                            │   └── rows: 1500000
                                            └── Projection
                                                ├── exprs:
                                                │   ┌── s_nationkey
                                                │   ├── l_orderkey
                                                │   ├── l_extendedprice
                                                │   ├── l_discount
                                                │   └── l_shipdate
                                                ├── cost: 46040960
                                                ├── rows: 1500303.8
                                                └── HashJoin
                                                    ├── type: inner
                                                    ├── on: = { lhs: [ s_suppkey ], rhs: [ l_suppkey ] }
                                                    ├── cost: 44465640
                                                    ├── rows: 1500303.8
                                                    ├── Scan
                                                    │   ├── table: supplier
                                                    │   ├── list: [ s_suppkey, s_nationkey ]
                                                    │   ├── filter: null
                                                    │   ├── cost: 2000
                                                    │   └── rows: 10000
                                                    └── Filter
                                                        ├── cond: >= { lhs: 1996-12-31, rhs: l_shipdate }
                                                        ├── cost: 23344728
                                                        ├── rows: 1500303.8
                                                        └── Filter
                                                            ├── cond: >= { lhs: l_shipdate, rhs: 1995-01-01 }
                                                            ├── cost: 16563354
                                                            ├── rows: 3000607.5
                                                            └── Scan
                                                                ├── table: lineitem
                                                                ├── list:
                                                                │   ┌── l_orderkey
                                                                │   ├── l_suppkey
                                                                │   ├── l_extendedprice
                                                                │   ├── l_discount
                                                                │   └── l_shipdate
                                                                ├── filter: null
                                                                ├── cost: 3000607.8
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

├── cost: 618345150
├── rows: 10
└── Order { by: [ Extract { from: o_orderdate, field: YEAR } ], cost: 618345150, rows: 10 }
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
        ├── cost: 618345100
        ├── rows: 10
        └── Projection
            ├── exprs:
            │   ┌── Extract { from: o_orderdate, field: YEAR }
            │   ├── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
            │   └── n_name(1)
            ├── cost: 537332100
            ├── rows: 6001215
            └── Projection
                ├── exprs: [ n_nationkey, n_name(1), c_nationkey, o_orderdate, l_extendedprice, l_discount ]
                ├── cost: 501084740
                ├── rows: 6001215
                └── HashJoin
                    ├── type: inner
                    ├── on: = { lhs: [ n_nationkey, n_nationkey(1) ], rhs: [ c_nationkey, s_nationkey ] }
                    ├── cost: 494723460
                    ├── rows: 6001215
                    ├── HashJoin
                    │   ├── type: inner
                    │   ├── on: = { lhs: [ n_regionkey ], rhs: [ r_regionkey ] }
                    │   ├── cost: 547.86346
                    │   ├── rows: 62.5
                    │   ├── Scan { table: nation, list: [ n_nationkey, n_regionkey ], filter: null, cost: 5, rows: 25 }
                    │   └── Join { type: inner, cost: 100.325, rows: 62.5 }
                    │       ├── Projection { exprs: [ r_regionkey ], cost: 14.075001, rows: 2.5 }
                    │       │   └── Filter { cond: = { lhs: r_name, rhs: 'AMERICA' }, cost: 11.55, rows: 2.5 }
                    │       │       └── Scan
                    │       │           ├── table: region
                    │       │           ├── list: [ r_regionkey, r_name ]
                    │       │           ├── filter: null
                    │       │           ├── cost: 1
                    │       │           └── rows: 5
                    │       └── Scan
                    │           ├── table: nation
                    │           ├── list: [ n_nationkey(1), n_name(1) ]
                    │           ├── filter: null
                    │           ├── cost: 5
                    │           └── rows: 25
                    └── Projection
                        ├── exprs: [ s_nationkey, c_nationkey, o_orderdate, l_extendedprice, l_discount ]
                        ├── cost: 452781920
                        ├── rows: 6001215
                        └── Projection
                            ├── exprs:
                            │   ┌── s_nationkey
                            │   ├── c_custkey
                            │   ├── c_nationkey
                            │   ├── o_custkey
                            │   ├── o_orderdate
                            │   ├── l_extendedprice
                            │   └── l_discount
                            ├── cost: 446480640
                            ├── rows: 6001215
                            └── HashJoin
                                ├── type: inner
                                ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                                ├── cost: 440059330
                                ├── rows: 6001215
                                ├── Scan
                                │   ├── table: customer
                                │   ├── list: [ c_custkey, c_nationkey ]
                                │   ├── filter: null
                                │   ├── cost: 30000
                                │   └── rows: 150000
                                └── Projection
                                    ├── exprs: [ s_nationkey, o_custkey, o_orderdate, l_extendedprice, l_discount ]
                                    ├── cost: 330060700
                                    ├── rows: 6001215
                                    └── HashJoin
                                        ├── type: inner
                                        ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                                        ├── cost: 323759420
                                        ├── rows: 6001215
                                        ├── Filter
                                        │   ├── cond: >= { lhs: 1996-12-31, rhs: o_orderdate }
                                        │   ├── cost: 5310000
                                        │   ├── rows: 375000
                                        │   └── Filter
                                        │       ├── cond: >= { lhs: o_orderdate, rhs: 1995-01-01 }
                                        │       ├── cost: 3690000
                                        │       ├── rows: 750000
                                        │       └── Scan
                                        │           ├── table: orders
                                        │           ├── list: [ o_orderkey, o_custkey, o_orderdate ]
                                        │           ├── filter: null
                                        │           ├── cost: 450000
                                        │           └── rows: 1500000
                                        └── HashJoin
                                            ├── type: inner
                                            ├── on: = { lhs: [ p_partkey ], rhs: [ l_partkey ] }
                                            ├── cost: 193782670
                                            ├── rows: 6001215
                                            ├── Projection { exprs: [ p_partkey ], cost: 563000, rows: 100000 }
                                            │   └── Filter
                                            │       ├── cond: = { lhs: p_type, rhs: 'ECONOMY ANODIZED STEEL' }
                                            │       ├── cost: 462000
                                            │       ├── rows: 100000
                                            │       └── Scan
                                            │           ├── table: part
                                            │           ├── list: [ p_partkey, p_type ]
                                            │           ├── filter: null
                                            │           ├── cost: 40000
                                            │           └── rows: 200000
                                            └── HashJoin
                                                ├── type: inner
                                                ├── on: = { lhs: [ s_suppkey ], rhs: [ l_suppkey ] }
                                                ├── cost: 87079624
                                                ├── rows: 6001215
                                                ├── Scan
                                                │   ├── table: supplier
                                                │   ├── list: [ s_suppkey, s_nationkey ]
                                                │   ├── filter: null
                                                │   ├── cost: 2000
                                                │   └── rows: 10000
                                                └── Scan
                                                    ├── table: lineitem
                                                    ├── list:
                                                    │   ┌── l_orderkey
                                                    │   ├── l_partkey
                                                    │   ├── l_suppkey
                                                    │   ├── l_extendedprice
                                                    │   └── l_discount
                                                    ├── filter: null
                                                    ├── cost: 3000607.8
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
├── cost: 660241540
├── rows: 100
└── Order
    ├── by:
    │   ┌── n_name
    │   └── desc
    │       └── Extract { from: o_orderdate, field: YEAR }
    ├── cost: 660241400
    ├── rows: 100
    └── HashAgg
        ├── aggs:sum
        │   └── -
        │       ├── lhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        │       └── rhs: * { lhs: ps_supplycost, rhs: l_quantity }
        ├── group_by: [ n_name, Extract { from: o_orderdate, field: YEAR } ]
        ├── cost: 660240700
        ├── rows: 100
        └── Projection
            ├── exprs:
            │   ┌── n_name
            │   ├── Extract { from: o_orderdate, field: YEAR }
            │   └── -
            │       ├── lhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
            │       └── rhs: * { lhs: ps_supplycost, rhs: l_quantity }
            ├── cost: 602099650
            ├── rows: 6001215
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ n_nationkey ], rhs: [ s_nationkey ] }
                ├── cost: 553729860
                ├── rows: 6001215
                ├── Scan { table: nation, list: [ n_nationkey, n_name ], filter: null, cost: 5, rows: 25 }
                └── Projection
                    ├── exprs: [ s_nationkey, ps_supplycost, o_orderdate, l_quantity, l_extendedprice, l_discount ]
                    ├── cost: 520720420
                    ├── rows: 6001215
                    └── HashJoin
                        ├── type: inner
                        ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                        ├── cost: 514359140
                        ├── rows: 6001215
                        ├── Scan
                        │   ├── table: orders
                        │   ├── list: [ o_orderkey, o_orderdate ]
                        │   ├── filter: null
                        │   ├── cost: 300000
                        │   └── rows: 1500000
                        └── Projection
                            ├── exprs:
                            │   ┌── s_nationkey
                            │   ├── ps_supplycost
                            │   ├── l_orderkey
                            │   ├── l_quantity
                            │   ├── l_extendedprice
                            │   └── l_discount
                            ├── cost: 355359230
                            ├── rows: 6001215
                            └── HashJoin
                                ├── type: inner
                                ├── on: = { lhs: [ ps_suppkey, ps_partkey ], rhs: [ l_suppkey, l_partkey ] }
                                ├── cost: 348997950
                                ├── rows: 6001215
                                ├── Scan
                                │   ├── table: partsupp
                                │   ├── list: [ ps_partkey, ps_suppkey, ps_supplycost ]
                                │   ├── filter: null
                                │   ├── cost: 240000
                                │   └── rows: 800000
                                └── Projection
                                    ├── exprs:
                                    │   ┌── s_nationkey
                                    │   ├── l_orderkey
                                    │   ├── l_partkey
                                    │   ├── l_suppkey
                                    │   ├── l_quantity
                                    │   ├── l_extendedprice
                                    │   └── l_discount
                                    ├── cost: 209387340
                                    ├── rows: 6001215
                                    └── HashJoin
                                        ├── type: inner
                                        ├── on: = { lhs: [ s_suppkey ], rhs: [ l_suppkey ] }
                                        ├── cost: 202966050
                                        ├── rows: 6001215
                                        ├── Scan
                                        │   ├── table: supplier
                                        │   ├── list: [ s_suppkey, s_nationkey ]
                                        │   ├── filter: null
                                        │   ├── cost: 2000
                                        │   └── rows: 10000
                                        └── HashJoin
                                            ├── type: inner
                                            ├── on: = { lhs: [ p_partkey ], rhs: [ l_partkey ] }
                                            ├── cost: 117686780
                                            ├── rows: 6001215
                                            ├── Projection { exprs: [ p_partkey ], cost: 684000, rows: 200000 }
                                            │   └── Filter
                                            │       ├── cond: like { lhs: p_name, rhs: '%green%' }
                                            │       ├── cost: 482000
                                            │       ├── rows: 200000
                                            │       └── Scan
                                            │           ├── table: part
                                            │           ├── list: [ p_partkey, p_name ]
                                            │           ├── filter: null
                                            │           ├── cost: 40000
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
                                                ├── cost: 3600728.8
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
├── cost: 264044000
├── rows: 20
└── TopN
    ├── limit: 20
    ├── offset: 0
    ├── order_by:desc
    │   └── sum
    │       └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
    ├── cost: 264043980
    ├── rows: 20
    └── HashAgg
        ├── aggs:sum
        │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }
        ├── group_by: [ c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ]
        ├── cost: 220120800
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
            ├── cost: 124072490
            ├── rows: 3000607.5
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ n_nationkey ], rhs: [ c_nationkey ] }
                ├── cost: 120801820
                ├── rows: 3000607.5
                ├── Scan { table: nation, list: [ n_nationkey, n_name ], filter: null, cost: 5, rows: 25 }
                └── Projection
                    ├── exprs:
                    │   ┌── c_custkey
                    │   ├── c_name
                    │   ├── c_address
                    │   ├── c_nationkey
                    │   ├── c_phone
                    │   ├── c_acctbal
                    │   ├── c_comment
                    │   ├── l_extendedprice
                    │   └── l_discount
                    ├── cost: 103396856
                    ├── rows: 3000607.5
                    └── HashJoin
                        ├── type: inner
                        ├── on: = { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }
                        ├── cost: 100126190
                        ├── rows: 3000607.5
                        ├── Projection
                        │   ├── exprs:
                        │   │   ┌── c_custkey
                        │   │   ├── c_name
                        │   │   ├── c_address
                        │   │   ├── c_nationkey
                        │   │   ├── c_phone
                        │   │   ├── c_acctbal
                        │   │   ├── c_comment
                        │   │   └── o_orderkey
                        │   ├── cost: 15567172
                        │   ├── rows: 375000
                        │   └── HashJoin
                        │       ├── type: inner
                        │       ├── on: = { lhs: [ c_custkey ], rhs: [ o_custkey ] }
                        │       ├── cost: 15162172
                        │       ├── rows: 375000
                        │       ├── Scan
                        │       │   ├── table: customer
                        │       │   ├── list:
                        │       │   │   ┌── c_custkey
                        │       │   │   ├── c_name
                        │       │   │   ├── c_address
                        │       │   │   ├── c_nationkey
                        │       │   │   ├── c_phone
                        │       │   │   ├── c_acctbal
                        │       │   │   └── c_comment
                        │       │   ├── filter: null
                        │       │   ├── cost: 105000
                        │       │   └── rows: 150000
                        │       └── Projection { exprs: [ o_orderkey, o_custkey ], cost: 5692500, rows: 375000 }
                        │           └── Filter
                        │               ├── cond: >= { lhs: o_orderdate, rhs: 1993-10-01 }
                        │               ├── cost: 5310000
                        │               ├── rows: 375000
                        │               └── Filter
                        │                   ├── cond: > { lhs: 1994-01-01, rhs: o_orderdate }
                        │                   ├── cost: 3690000
                        │                   ├── rows: 750000
                        │                   └── Scan
                        │                       ├── table: orders
                        │                       ├── list: [ o_orderkey, o_custkey, o_orderdate ]
                        │                       ├── filter: null
                        │                       ├── cost: 450000
                        │                       └── rows: 1500000
                        └── Projection
                            ├── exprs: [ l_orderkey, l_extendedprice, l_discount ]
                            ├── cost: 18753796
                            ├── rows: 3000607.5
                            └── Filter { cond: = { lhs: l_returnflag, rhs: 'R' }, cost: 15663171, rows: 3000607.5 }
                                └── Scan
                                    ├── table: lineitem
                                    ├── list: [ l_orderkey, l_extendedprice, l_discount, l_returnflag ]
                                    ├── filter: null
                                    ├── cost: 2400486
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
├── cost: 95677500
├── rows: 10
└── Order { by: [ l_shipmode ], cost: 95677496, rows: 10 }
    └── HashAgg
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
        ├── group_by: [ l_shipmode ]
        ├── cost: 95677460
        ├── rows: 10
        └── Projection { exprs: [ o_orderpriority, l_shipmode ], cost: 60413308, rows: 1500000 }
            └── HashJoin
                ├── type: inner
                ├── on: = { lhs: [ l_orderkey ], rhs: [ o_orderkey ] }
                ├── cost: 58883308
                ├── rows: 1500000
                ├── Projection { exprs: [ l_orderkey, l_shipmode ], cost: 26601636, rows: 250050.61 }
                │   └── Filter
                │       ├── cond:In { in: [ 'MAIL', 'SHIP' ] }
                │       │   └── l_shipmode
                │       ├── cost: 26346584
                │       ├── rows: 250050.61
                │       └── Filter
                │           ├── cond: > { lhs: l_receiptdate, rhs: l_commitdate }
                │           ├── cost: 24717504
                │           ├── rows: 375075.94
                │           └── Filter
                │               ├── cond: >= { lhs: l_receiptdate, rhs: 1994-01-01 }
                │               ├── cost: 23764812
                │               ├── rows: 750151.9
                │               └── Filter
                │                   ├── cond: > { lhs: l_commitdate, rhs: l_shipdate }
                │                   ├── cost: 20374126
                │                   ├── rows: 1500303.8
                │                   └── Filter
                │                       ├── cond: > { lhs: 1995-01-01, rhs: l_receiptdate }
                │                       ├── cost: 16563354
                │                       ├── rows: 3000607.5
                │                       └── Scan
                │                           ├── table: lineitem
                │                           ├── list:
                │                           │   ┌── l_orderkey
                │                           │   ├── l_shipdate
                │                           │   ├── l_commitdate
                │                           │   ├── l_receiptdate
                │                           │   └── l_shipmode
                │                           ├── filter: null
                │                           ├── cost: 3000607.8
                │                           └── rows: 6001215
                └── Scan
                    ├── table: orders
                    ├── list: [ o_orderkey, o_orderpriority ]
                    ├── filter: null
                    ├── cost: 300000
                    └── rows: 1500000
*/

