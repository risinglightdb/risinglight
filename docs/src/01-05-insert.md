# 插入数据（`INSERT`）

在实现完内存存储系统之后，我们就可以正式向数据库中添加数据了。

用于向表中插入数据的 SQL 语句是 `INSERT`，它的一般形式为：
```sql
INSERT INTO t(a, b) VALUES (1, 10), (2, 20)
```
数据库需要首先解析表达式的值，为每一列构建 Array，然后按列排好序后插入到表中。

## 背景知识

TODO

## 任务目标

能够向表中插入数据，支持以下 SQL：

```sql
INSERT INTO t(a, b) VALUES (1, 10)
```

