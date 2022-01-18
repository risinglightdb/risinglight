# Running TPC-H Queries

Currently, RisingLight supports importing TPC-H data and run a subset of TPC-H queries. You may import the TPC-H data
and run some simple queries in RisingLight.

## Generate TPC-H Data

### Use Make Recipe

You may use the make recipe to download and generate TPC-H data.

```
make tpch
```

The generated data will be placed under `target/tpch-dbgen/tbl` folder.

### Manual Generation

First, you should use git to clone the tpch-dbgen repo:

```
git clone https://github.com/electrum/tpch-dbgen.git
```

This repo contains the program for generating TPC-H data. 

Then, enter the tpch-dbgen directory and type `make all`, and it will generate some executable binaries such as `dbgen` and `qgen`. We will show you how to generate TPC-H data by using one line of command in the following sections. Meanwhile, you can read this [README](https://github.com/electrum/tpch-dbgen/blob/master/README) for more details.

Finally, type the following command and wait for several seconds:

```
./dbgen -s 1
```

This command will generate the data we want, which contains a table called `LINEITEM` with a size of 700MB.

## Create Table and Import Data

You will need to build RisingLight in release mode, so as to import data faster.

```shell
cargo build --release
```

Then, use our test scripts to create tables.

```shell
cargo run --release -- -f tests/sql/tpch/create.slt
```

We can use `\dt` to ensure that all tables have been imported.

```
cargo run --release
# Inside SQL shell
\dt
+---+----------+---+----------+---+----------+
| 0 | postgres | 0 | postgres | 3 | supplier |
| 0 | postgres | 0 | postgres | 1 | region   |
| 0 | postgres | 0 | postgres | 0 | nation   |
| 0 | postgres | 0 | postgres | 4 | partsupp |
| 0 | postgres | 0 | postgres | 7 | lineitem |
| 0 | postgres | 0 | postgres | 6 | orders   |
| 0 | postgres | 0 | postgres | 2 | part     |
| 0 | postgres | 0 | postgres | 5 | customer |
+---+----------+---+----------+---+----------+
```

Then, we may use the `import.sql` to import data, which calls `COPY FROM` SQL statements internally:

```shell
cargo run --release -- -f tests/sql/tpch/import.sql
```

Generally, you can finish this process within several seconds.

## Run TPC-H

Now, we can run simple queries on this table.

```shell
cargo run --release
```

```sql
select sum(L_LINENUMBER) from LINEITEM;
select count(L_ORDERKEY), sum(L_LINENUMBER) from LINEITEM where L_ORDERKEY > 2135527;
```
