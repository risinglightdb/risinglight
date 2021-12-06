# 执行计划

为了更好的实现各种复杂的数据查询操作，我们首先扩展。

## 背景知识

### 逻辑计划（Logical Plan）

### 物理计划（Physical Plan）

## 任务目标

能够使用 `EXPLAIN` 语句输出其它 SQL 语句的执行计划：

```sql
EXPLAIN INSERT INTO t(a, b) VALUES (1, 10)
```

