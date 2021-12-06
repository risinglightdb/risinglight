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
    - [连接2：Sort-Merge Join](./02-10-sort-merge-join.md)
    - [连接3：Hash Join](./02-11-hash-join.md)
    - [嵌套查询](./02-12-nested-query.md)

- [优化](./03-optmization.md)

    - [优化器框架](./03-01-optimizer.md)
    - [常量折叠](./03-02-constant-folding.md)
    - [谓词下推](./03-03-predicate-pushdown.md)
    - [表达式化简](./03-04-simplification.md)
    - [代价估计](./03-05-cost-estimation.md)
    - [连接重排序](./03-06-join-reordering.md)

- [存储](./04-storage.md)

    - [列式存储与合并树](./04-01-merge-tree.md)
    - [Memtable](./04-02-memtable.md)
    - [编码与写入](./04-03-write.md)
    - [Manifest](./04-04-manifest.md)
    - [读取](./04-05-read.md)
    - [删除](./04-06-deletion.md)
    - [合并](./04-07-compaction.md)
    - [快照](./04-08-snapshot.md)

- [事务](./05-transaction.md)

