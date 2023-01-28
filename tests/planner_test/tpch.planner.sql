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
├── cost: 39238.984                                                                                                     
├── exprs:                                                                                                              
│   ┌── l_returnflag                                                                                                    
│   ├── l_linestatus                                                                                                    
│   ├── sum {  }                                                                                                        
│   │   └── l_quantity                                                                                                  
│   ├── sum {  }                                                                                                        
│   │   └── l_extendedprice                                                                                             
│   ├── sum {  }                                                                                                        
│   │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }                                              
│   ├── sum {  }                                                                                                        
│   │   └── * { lhs: + { lhs: l_tax, rhs: 1 }, rhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } } }    
│   ├── / { lhs: sum {  }, rhs: count {  } }                                                                            
                                                                                                                        
│   ├── / { lhs: sum {  }, rhs: count {  } }                                                                            
                                                                                                                        
│   ├── / { lhs: sum {  }, rhs: count {  } }                                                                            
                                                                                                                        
│   └── rowcount                                                                                                        
└── Order { by: [ asc {  }, asc {  } ], cost: 33158.984 }                                                               
    └── Aggregate                                                                                                       
        ├── aggs:                                                                                                       
        │   ┌── sum {  }                                                                                                
        │   │   └── l_quantity                                                                                          
        │   ├── sum {  }                                                                                                
        │   │   └── l_extendedprice                                                                                     
        │   ├── sum {  }                                                                                                
        │   │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }                                      
        │   ├── sum {  }                                                                                                
        │   │   └── *                                                                                                   
        │   │       ├── lhs: + { lhs: l_tax, rhs: 1 }                                                                   
        │   │       └── rhs: * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }                             
        │   ├── count {  }                                                                                              
        │   │   └── l_quantity                                                                                          
        │   ├── count {  }                                                                                              
        │   │   └── l_extendedprice                                                                                     
        │   ├── sum {  }                                                                                                
        │   │   └── l_discount                                                                                          
        │   ├── count {  }                                                                                              
        │   │   └── l_discount                                                                                          
        │   └── rowcount                                                                                                
        ├── cost: 25300                                                                                                 
        ├── group_by: [ l_returnflag, l_linestatus ]                                                                    
        └── Projection                                                                                                  
            ├── cost: 18260                                                                                             
            ├── exprs: [ l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus ]                   
            └── Filter { cond: >= { lhs: 1998-09-21, rhs: l_shipdate }, cost: 12900 }                                   
                └── Scan                                                                                                
                    ├── cost: 7000                                                                                      
                    ├── list: [ l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate ]
                    └── table: lineitem
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

Projection { cost: 63701.88, exprs: [ l_orderkey, sum {  }, o_orderdate, o_shippriority ] }                         
└── TopN { cost: 63647.88, limit: 10, offset: 0, order_by: [ desc {  }, asc {  } ] }                                
    └── Aggregate { aggs: [ sum {  } ], cost: 62224.105, group_by: [ l_orderkey, o_orderdate, o_shippriority ] }    
        └── Projection                                                                                              
            ├── cost: 59744.105                                                                                     
            ├── exprs: [ o_orderdate, o_shippriority, l_orderkey, l_extendedprice, l_discount ]                     
            └── HashJoin { cost: 55264.105, on: Equality { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }, type: inner }
                ├── Projection { cost: 24811.05, exprs: [ o_orderkey, o_orderdate, o_shippriority ] }               
                │   └── HashJoin                                                                                    
                │       ├── cost: 22091.05                                                                          
                │       ├── on: Equality { lhs: [ c_custkey ], rhs: [ o_custkey ] }                                 
                │       ├── type: inner                                                                             
                │       ├── Projection { cost: 2940, exprs: [ c_custkey ] }                                         
                │       │   └── Filter { cond: = { lhs: c_mktsegment, rhs: 'BUILDING' }, cost: 2700 }               
                │       │       └── Scan { cost: 2000, list: [ c_custkey, c_mktsegment ], table: customer }         
                │       └── Filter { cond: > { lhs: 1995-03-15, rhs: o_orderdate }, cost: 7500 }                    
                │           └── Scan                                                                                
                │               ├── cost: 4000                                                                      
                │               ├── list: [ o_orderkey, o_custkey, o_orderdate, o_shippriority ]                    
                │               └── table: orders                                                                   
                └── Projection { cost: 10220, exprs: [ l_orderkey, l_extendedprice, l_discount ] }                  
                    └── Filter { cond: > { lhs: l_shipdate, rhs: 1995-03-15 }, cost: 7500 }                         
                        └── Scan                                                                                    
                            ├── cost: 4000                                                                          
                            ├── list: [ l_orderkey, l_extendedprice, l_discount, l_shipdate ]                       
                            └── table: lineitem
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

Projection { cost: 163126.61, exprs: [ n_name, sum {  } ] }                                                           
└── Order { by: [ desc {  } ], cost: 161626.61 }                                                                      
    └── Aggregate { aggs: [ sum {  } ], cost: 156142.28, group_by: [ n_name ] }                                       
        └── Projection { cost: 154242.28, exprs: [ n_name, l_extendedprice, l_discount ] }                            
            └── HashJoin { cost: 150842.28, on: Equality { lhs: [ n_regionkey ], rhs: [ r_regionkey ] }, type: inner }
                ├── Projection { cost: 130941.61, exprs: [ n_name, n_regionkey, l_extendedprice, l_discount ] }       
                │   └── HashJoin                                                                                      
                │       ├── cost: 126441.61                                                                           
                │       ├── on: Equality { lhs: [ s_nationkey ], rhs: [ n_nationkey ] }                               
                │       ├── type: inner                                                                               
                │       ├── Projection { cost: 97507.16, exprs: [ s_nationkey, l_extendedprice, l_discount ] }        
                │       │   └── HashJoin                                                                              
                │       │       ├── cost: 94107.16                                                                    
                │       │       ├── on: Equality { lhs: [ l_suppkey, c_nationkey ], rhs: [ s_suppkey, s_nationkey ] } 
                │       │       ├── type: inner                                                                       
                │       │       ├── Projection                                                                        
                │       │       │   ├── cost: 66172.7                                                                 
                │       │       │   ├── exprs: [ c_nationkey, l_suppkey, l_extendedprice, l_discount ]                
                │       │       │   └── HashJoin                                                                      
                │       │       │       ├── cost: 61672.703                                                           
                │       │       │       ├── on: Equality { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }                 
                │       │       │       ├── type: inner                                                               
                │       │       │       ├── Projection { cost: 31738.25, exprs: [ c_nationkey, o_orderkey ] }         
                │       │       │       │   └── HashJoin                                                              
                │       │       │       │       ├── cost: 29438.25                                                    
                │       │       │       │       ├── on: Equality { lhs: [ c_custkey ], rhs: [ o_custkey ] }           
                │       │       │       │       ├── type: inner                                                       
                │       │       │       │       ├── Scan                                                              
                │       │       │       │       │   ├── cost: 2000                                                    
                │       │       │       │       │   ├── list: [ c_custkey, c_nationkey ]                              
                │       │       │       │       │   └── table: customer                                               
                │       │       │       │       └── Projection { cost: 7092, exprs: [ o_orderkey, o_custkey ] }       
                │       │       │       │           └── Filter                                                        
                │       │       │       │               ├── cond: and                                                 
                │       │       │       │               │   ├── lhs: >= { lhs: o_orderdate, rhs: 1994-01-01 }         
                │       │       │       │               │   └── rhs: > { lhs: 1995-01-01, rhs: o_orderdate }          
                │       │       │       │               ├── cost: 5620                                                
                │       │       │       │               └── Scan                                                      
                │       │       │       │                   ├── cost: 3000                                            
                │       │       │       │                   ├── list: [ o_orderkey, o_custkey, o_orderdate ]          
                │       │       │       │                   └── table: orders                                         
                │       │       │       └── Scan                                                                      
                │       │       │           ├── cost: 4000                                                            
                │       │       │           ├── list: [ l_orderkey, l_suppkey, l_extendedprice, l_discount ]          
                │       │       │           └── table: lineitem                                                       
                │       │       └── Scan { cost: 2000, list: [ s_suppkey, s_nationkey ], table: supplier }            
                │       └── Scan { cost: 3000, list: [ n_nationkey, n_name, n_regionkey ], table: nation }            
                └── Projection { cost: 2940, exprs: [ r_regionkey ] }                                                 
                    └── Filter { cond: = { lhs: r_name, rhs: 'AFRICA' }, cost: 2700 }                                 
                        └── Scan { cost: 2000, list: [ r_regionkey, r_name ], table: region }
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

Projection { cost: 8163.5923, exprs: [ sum {  } ] }                                                                 
└── Aggregate { aggs: [ sum {  } ], cost: 8161.992, group_by: [] }                                                  
    └── Projection { cost: 7964.3843, exprs: [ l_extendedprice, l_discount ] }                                      
        └── Filter                                                                                                  
            ├── cond: and                                                                                           
            │   ├── lhs: >= { lhs: l_shipdate, rhs: 1994-01-01 }                                                    
            │   └── rhs: and                                                                                        
            │       ├── lhs: > { lhs: 1995-01-01, rhs: l_shipdate }                                                 
            │       └── rhs: and                                                                                    
            │           ├── lhs: >= { lhs: 0.09, rhs: l_discount }                                                  
            │           └── rhs: and { lhs: > { lhs: 24, rhs: l_quantity }, rhs: >= { lhs: l_discount, rhs: 0.07 } }
            ├── cost: 7210.72                                                                                       
            └── Scan { cost: 4000, list: [ l_quantity, l_extendedprice, l_discount, l_shipdate ], table: lineitem }
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
├── cost: 138073.53                                                                                                    
├── exprs:                                                                                                             
│   ┌── c_custkey                                                                                                      
│   ├── c_name                                                                                                         
│   ├── sum {  }                                                                                                       
│   │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }                                             
│   ├── c_acctbal                                                                                                      
│   ├── n_name                                                                                                         
│   ├── c_address                                                                                                      
│   ├── c_phone                                                                                                        
│   └── c_comment                                                                                                      
└── TopN { cost: 137869.53, limit: 20, offset: 0, order_by: [ desc {  } ] }                                            
    └── Aggregate                                                                                                      
        ├── aggs:                                                                                                      
        │   ┌── sum {  }                                                                                               
        │   │   └── * { lhs: l_extendedprice, rhs: - { lhs: 1, rhs: l_discount } }                                     
        ├── cost: 135513.38                                                                                            
        ├── group_by: [ c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ]                          
        └── Projection                                                                                                 
            ├── cost: 130013.375                                                                                       
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
            └── HashJoin { cost: 120013.375, on: Equality { lhs: [ c_nationkey ], rhs: [ n_nationkey ] }, type: inner }
                ├── Projection                                                                                         
                │   ├── cost: 87078.92                                                                                 
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
                │   └── HashJoin                                                                                       
                │       ├── cost: 77078.92                                                                             
                │       ├── on: Equality { lhs: [ o_orderkey ], rhs: [ l_orderkey ] }                                  
                │       ├── type: inner                                                                                
                │       ├── Projection                                                                                 
                │       │   ├── cost: 48338.25                                                                         
                │       │   ├── exprs:                                                                                 
                │       │   │   ┌── c_custkey                                                                          
                │       │   │   ├── c_name                                                                             
                │       │   │   ├── c_address                                                                          
                │       │   │   ├── c_nationkey                                                                        
                │       │   │   ├── c_phone                                                                            
                │       │   │   ├── c_acctbal                                                                          
                │       │   │   ├── c_comment                                                                          
                │       │   │   └── o_orderkey                                                                         
                │       │   └── HashJoin                                                                               
                │       │       ├── cost: 39438.25                                                                     
                │       │       ├── on: Equality { lhs: [ c_custkey ], rhs: [ o_custkey ] }                            
                │       │       ├── type: inner                                                                        
                │       │       ├── Scan                                                                               
                │       │       │   ├── cost: 7000                                                                     
                │       │       │   ├── list:                                                                          
                │       │       │   │   ┌── c_custkey                                                                  
                │       │       │   │   ├── c_name                                                                     
                │       │       │   │   ├── c_address                                                                  
                │       │       │   │   ├── c_nationkey                                                                
                │       │       │   │   ├── c_phone                                                                    
                │       │       │   │   ├── c_acctbal                                                                  
                │       │       │   │   └── c_comment                                                                  
                │       │       │   └── table: customer                                                                
                │       │       └── Projection { cost: 7092, exprs: [ o_orderkey, o_custkey ] }                        
                │       │           └── Filter                                                                         
                │       │               ├── cond: and                                                                  
                │       │               │   ├── lhs: >= { lhs: o_orderdate, rhs: 1993-10-01 }                          
                │       │               │   └── rhs: > { lhs: 1994-01-01, rhs: o_orderdate }                           
                │       │               ├── cost: 5620                                                                 
                │       │               └── Scan                                                                       
                │       │                   ├── cost: 3000                                                             
                │       │                   ├── list: [ o_orderkey, o_custkey, o_orderdate ]                           
                │       │                   └── table: orders                                                          
                │       └── Projection { cost: 5780, exprs: [ l_orderkey, l_extendedprice, l_discount ] }              
                │           └── Filter { cond: = { lhs: l_returnflag, rhs: 'R' }, cost: 5100 }                         
                │               └── Scan                                                                               
                │                   ├── cost: 4000                                                                     
                │                   ├── list: [ l_orderkey, l_extendedprice, l_discount, l_returnflag ]                
                │                   └── table: lineitem                                                                
                └── Scan { cost: 2000, list: [ n_nationkey, n_name ], table: nation }
*/

