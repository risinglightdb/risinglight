# SQLLogicTest and SQLPlannerTest

RisingLight uses two test frameworks to do end-to-end tests.

## SQLLogicTest

SQLLogicTest will run a special `slt` file and compare the result from the expected output.
The test cases are stored under `tests/sql` folder.

For example, let's see `order_by.slt`:

```
statement ok
create table t(v1 int not null, v2 int not null)

statement ok
insert into t values(1, 1), (4, 2), (3, 3), (10, 12), (2, 5)

query I
select v1 from t order by v1 asc
----
1
2
3
4
10
```

The first 3 test cases of this test file are
* check whether create table works
* check whether insert table works 
* select data from table

We use `statement ok` to ensure statement successfully runs, and `query` to compare the query result.

When running `make test`, the test runner will run all files under `tests/sql` folder.

## SQLPlannerTest

SQLPlannerTest is a regression test. We will write yaml files to describe the cases we want to test.
The test cases are stored in `tests/planner_test`. Use the following command:

```
make apply_planner_test
```

to generate a sql file containing explain results for each yaml file.

Generally, we will compare the explain result before and after a commit, so as to know how the commit
affects the optimizer result. We don't really care about the correctness -- we just compare the explain
result before and after a PR.