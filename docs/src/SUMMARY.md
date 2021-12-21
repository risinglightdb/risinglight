# RisingLight Tutorial

[RisingLight Tutorial](./00-lets-build-a-database.md)

- [入门](./01-intro.md)

    - [Hello, SQL!](./01-01-hello-sql.md)
    - [Catalog](./01-02-catalog.md)
    - [创建表（CREATE TABLE）](./01-03-create-table.md)
    - [内存存储](./01-04-array.md)
    - [插入数据（INSERT）](./01-05-insert.md)
    - [执行计划](./01-06-planner.md)
    - [查询数据（SELECT）](./01-07-select.md)

- [查询](./02-query.md)

    - [类型转换（CAST）](./02-01-cast.md)
    - [算术运算（+-*/）](./02-02-operation.md)
    - [条件查询（WHERE）](./02-03-where.md)
    - [排序（ORDER BY）](./02-04-order.md)
    - [排序2：限制行数（LIMIT）](./02-07-limit.md)
    - [聚合（SUM）](./02-05-aggregation.md)
    - [聚合2：分组聚合（GROUP BY）](./02-08-group.md)
    - [聚合3：过滤分组（HAVING）](./02-09-having.md)
    - [连接（JOIN）](./02-06-join.md)
    - [连接2：Hash Join](./02-11-hash-join.md)
    - [连接3：Sort-Merge Join](./02-10-sort-merge-join.md)
    - [嵌套查询](./02-12-nested-query.md)
    - [TPC-H](./02-13-tpch.md)

- [优化](./03-optmization.md)

    - [常量折叠](./03-01-constant-folding.md)
    - [列剪裁](./03-02-column-pruning.md)
    - [谓词下推](./03-03-predicate-pushdown.md)
    - [代价估计](./03-04-cost-estimation.md)
    - [计划搜索](./03-05-cascade-optimizer.md)
    - [连接重排序](./03-06-join-reordering.md)

- [存储](./04-storage.md)

    - [Memtable](./04-01-memtable.md)
    - [编码与写入](./04-02-write.md)
    - [Manifest](./04-03-manifest.md)
    - [读取](./04-04-read.md)
    - [删除](./04-05-deletion.md)
    - [合并](./04-06-compaction.md)
    - [快照](./04-07-snapshot.md)

- [事务](./05-transaction.md)

