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
Projection: [l_returnflag, l_linestatus, sum(l_quantity), sum(l_extendedprice), sum((l_extendedprice * (1 - l_discount))), sum(((l_tax + 1) * (l_extendedprice * (1 - l_discount)))), (sum(l_quantity) / count(l_quantity)), (sum(l_extendedprice) / count(l_extendedprice)), (sum(l_discount) / count(l_discount)), rowcount] (cost=39238.984)
  Order: [l_returnflag asc, l_linestatus asc] (cost=33158.984)
    Aggregate: [sum(l_quantity), sum(l_extendedprice), sum((l_extendedprice * (1 - l_discount))), sum(((l_tax + 1) * (l_extendedprice * (1 - l_discount)))), count(l_quantity), count(l_extendedprice), sum(l_discount), count(l_discount), rowcount], groupby=[l_returnflag, l_linestatus] (cost=25300)
      Projection: [l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus] (cost=18260)
        Filter: (1998-09-21 >= l_shipdate) (cost=12900)
          Scan: lineitem[l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate] (cost=7000)
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
Projection: [l_orderkey, sum((l_extendedprice * (1 - l_discount))), o_orderdate, o_shippriority] (cost=63701.88)
  TopN: limit=10, offset=0, orderby=[sum((l_extendedprice * (1 - l_discount))) desc, o_orderdate asc] (cost=63647.88)
    Aggregate: [sum((l_extendedprice * (1 - l_discount)))], groupby=[l_orderkey, o_orderdate, o_shippriority] (cost=62224.105)
      Projection: [o_orderdate, o_shippriority, l_orderkey, l_extendedprice, l_discount] (cost=59744.105)
        HashJoin: inner, on=([o_orderkey] = [l_orderkey]) (cost=55264.105)
          Projection: [o_orderkey, o_orderdate, o_shippriority] (cost=24811.05)
            HashJoin: inner, on=([c_custkey] = [o_custkey]) (cost=22091.05)
              Projection: [c_custkey] (cost=2940)
                Filter: (c_mktsegment = 'BUILDING') (cost=2700)
                  Scan: customer[c_custkey, c_mktsegment] (cost=2000)
              Filter: (1995-03-15 > o_orderdate) (cost=7500)
                Scan: orders[o_orderkey, o_custkey, o_orderdate, o_shippriority] (cost=4000)
          Projection: [l_orderkey, l_extendedprice, l_discount] (cost=10220)
            Filter: (l_shipdate > 1995-03-15) (cost=7500)
              Scan: lineitem[l_orderkey, l_extendedprice, l_discount, l_shipdate] (cost=4000)
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
Projection: [n_name, sum((l_extendedprice * (1 - l_discount)))] (cost=163126.61)
  Order: [sum((l_extendedprice * (1 - l_discount))) desc] (cost=161626.61)
    Aggregate: [sum((l_extendedprice * (1 - l_discount)))], groupby=[n_name] (cost=156142.28)
      Projection: [n_name, l_extendedprice, l_discount] (cost=154242.28)
        HashJoin: inner, on=([n_regionkey] = [r_regionkey]) (cost=150842.28)
          Projection: [n_name, n_regionkey, l_extendedprice, l_discount] (cost=130941.61)
            HashJoin: inner, on=([s_nationkey] = [n_nationkey]) (cost=126441.61)
              Projection: [s_nationkey, l_extendedprice, l_discount] (cost=97507.16)
                HashJoin: inner, on=([l_suppkey, c_nationkey] = [s_suppkey, s_nationkey]) (cost=94107.16)
                  Projection: [c_nationkey, l_suppkey, l_extendedprice, l_discount] (cost=66172.7)
                    HashJoin: inner, on=([o_orderkey] = [l_orderkey]) (cost=61672.703)
                      Projection: [c_nationkey, o_orderkey] (cost=31738.25)
                        HashJoin: inner, on=([c_custkey] = [o_custkey]) (cost=29438.25)
                          Scan: customer[c_custkey, c_nationkey] (cost=2000)
                          Projection: [o_orderkey, o_custkey] (cost=7092)
                            Filter: ((o_orderdate >= 1994-01-01) and (1995-01-01 > o_orderdate)) (cost=5620)
                              Scan: orders[o_orderkey, o_custkey, o_orderdate] (cost=3000)
                      Scan: lineitem[l_orderkey, l_suppkey, l_extendedprice, l_discount] (cost=4000)
                  Scan: supplier[s_suppkey, s_nationkey] (cost=2000)
              Scan: nation[n_nationkey, n_name, n_regionkey] (cost=3000)
          Projection: [r_regionkey] (cost=2940)
            Filter: (r_name = 'AFRICA') (cost=2700)
              Scan: region[r_regionkey, r_name] (cost=2000)
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
Projection: [sum((l_discount * l_extendedprice))] (cost=8163.5923)
  Aggregate: [sum((l_discount * l_extendedprice))], groupby=[] (cost=8161.992)
    Projection: [l_extendedprice, l_discount] (cost=7964.3843)
      Filter: ((24 > l_quantity) and ((l_shipdate >= 1994-01-01) and ((0.09 >= l_discount) and ((1995-01-01 > l_shipdate) and (l_discount >= 0.07))))) (cost=7210.72)
        Scan: lineitem[l_quantity, l_extendedprice, l_discount, l_shipdate] (cost=4000)
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
Projection: [c_custkey, c_name, sum((l_extendedprice * (1 - l_discount))), c_acctbal, n_name, c_address, c_phone, c_comment] (cost=138073.53)
  TopN: limit=20, offset=0, orderby=[sum((l_extendedprice * (1 - l_discount))) desc] (cost=137869.53)
    Aggregate: [sum((l_extendedprice * (1 - l_discount)))], groupby=[c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment] (cost=135513.38)
      Projection: [n_name, c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, l_extendedprice, l_discount] (cost=130013.375)
        HashJoin: inner, on=([c_nationkey] = [n_nationkey]) (cost=120013.375)
          Projection: [c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment, l_extendedprice, l_discount] (cost=87078.92)
            HashJoin: inner, on=([o_orderkey] = [l_orderkey]) (cost=77078.92)
              Projection: [c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment, o_orderkey] (cost=48338.25)
                HashJoin: inner, on=([c_custkey] = [o_custkey]) (cost=39438.25)
                  Scan: customer[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment] (cost=7000)
                  Projection: [o_orderkey, o_custkey] (cost=7092)
                    Filter: ((o_orderdate >= 1993-10-01) and (1994-01-01 > o_orderdate)) (cost=5620)
                      Scan: orders[o_orderkey, o_custkey, o_orderdate] (cost=3000)
              Projection: [l_orderkey, l_extendedprice, l_discount] (cost=5780)
                Filter: (l_returnflag = 'R') (cost=5100)
                  Scan: lineitem[l_orderkey, l_extendedprice, l_discount, l_returnflag] (cost=4000)
          Scan: nation[n_nationkey, n_name] (cost=2000)
*/

