# Importing TPC-H Data

Currently, RisingLight supports importing TPC-H data (but cannot run the TPC-H queries). You may import the TPC-H data
and run some simple queries in RisingLight.

## Generate TPC-H Data

First, you should use git to clone the tpch-dbgen repo:

```
git clone https://github.com/databricks/tpch-dbgen.git
```

This repo contains the program for generating TPC-H data. 

Then, enter the tpch-dbgen directory and type `make all`, and it will generate some executable binaries such as `dbgen` and `qgen`. We will show you how to generate TPC-H data by using one line of command in the following sections. Meanwhile, you can read this [README](https://github.com/databricks/tpch-dbgen/blob/master/README) for more details.

Finally, type the following command and wait for several seconds:

```
./dbgen -s 1 -T L
```

This command will generate the data we want, which contains a table called `LINEITEM` with a size of 700MB.

## Create Table and Import `lineitem` Data

We should run RisingLight:

```shell
cargo run --release
```

and create a table for it in order to import the data and query on it:

```sql
CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                        L_PARTKEY     INTEGER NOT NULL,
                        L_SUPPKEY     INTEGER NOT NULL,
                        L_LINENUMBER  INTEGER NOT NULL,
                        L_QUANTITY    FLOAT NOT NULL,
                        L_EXTENDEDPRICE  FLOAT NOT NULL,
                        L_DISCOUNT    FLOAT NOT NULL,
                        L_TAX         FLOAT NOT NULL,
                        L_RETURNFLAG  CHAR(1) NOT NULL,
                        L_LINESTATUS  CHAR(1) NOT NULL,
                        L_SHIPDATE    CHAR(20) NOT NULL,
                        L_COMMITDATE  CHAR(20) NOT NULL,
                        L_RECEIPTDATE CHAR(20) NOT NULL,
                        L_SHIPINSTRUCT CHAR(25) NOT NULL,
                        L_SHIPMODE     CHAR(10) NOT NULL,
                        L_COMMENT      VARCHAR(44) NOT NULL);
```

We use the copy command to import this data: 

```sql
COPY LINEITEM FROM '<path to lineitem.tbl>' ( DELIMITER '|' );
```

Generally, you can finish this process within ten seconds.

## Run Simple Queries

Now, we can run simple queries on this table.

Run `select sum(L_LINENUMBER) from LINEITEM;`, and you will get an output like this:

```
+----------+
| 18007100 |
+----------+
```

Run `select count(L_ORDERKEY), sum(L_LINENUMBER) from LINEITEM where L_ORDERKEY > 2135527;`, and you will get an output like this:

```
+---------+----------+
| 3865242 | 11597843 |
+---------+----------+
```

RisingLight supports a variety of queries. You may try whatever queries you want!
