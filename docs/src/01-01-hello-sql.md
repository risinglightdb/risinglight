# Hello, SQL!

作为万里长征的第一步，在这一节中我们会构建起 RisingLight 数据库的基本框架，并且使它能够运行最简单的 SQL 语句。

<!-- toc -->

## 背景知识

### SQL

TODO

### SqlLogicTest

[SqlLogicTest] 是一个检验数据库执行结果正确性的测试框架，最早被用来测试 SQLite。它定义了一种脚本语言来描述 SQL 测试语句和期望输出结果。

[SqlLogicTest]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

以这个任务所定义的第一个测试为例：

```
query I         # query 表示查询，I 表示期望输出是一个整数
SELECT 1        # 输入的 SQL 语句
----            # 分隔符，接下来描述期望结果
1               # 期望输出 1，测试器会检查它和数据库输出的字符串是否一致
```

我们的 RisingLight 就使用了 sqllogictest 来做端到端测试。你可以在 [`code/sql`] 文件夹下找到每个任务对应的 sqllogictest 测试脚本。

[`code/sql`]: https://github.com/singularity-data/risinglight/tree/main/code/sql

## 任务目标

能够运行最简单的 SQL 语句：

```sql
SELECT 1
```

## 整体设计

整个项目由以下部分组成：

* DB：数据库对象。其中包含两个模块：
    * Parser：负责解析 SQL 语句并生成抽象语法树（AST）
    * Executor：负责执行解析后的 SQL 语句
* Shell：一个可交互的命令行终端
* Test：一个基于 sqllogictest 脚本的端到端测试框架

TODO

## 源码解析

TODO
<!-- 
不知 mdbook 有没有办法自动折叠这段
 -->