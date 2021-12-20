# Hello, SQL!

作为万里长征的第一步，在这一节中我们会构建起 RisingLight 数据库的基本框架，并且使它能够运行最简单的 SQL 语句。

<!-- toc -->

## 背景知识

### SQL

SQL 是数据库领域最流行的编程语言。它的全称是 Structured Query Language（结构化查询语言），用来访问和处理数据库中的数据。

SQL 语言的背后是 **关系代数** 模型，具有非常坚实的理论基础。

关于 SQL 语言的更多介绍可以参考 [CMU 15-445] Lecture 01/02 的相关内容。

[CMU 15-445]: https://15445.courses.cs.cmu.edu/fall2021/schedule.html

由于 SQL 在数据库中的广泛应用，我们的 RisingLight 也采用 SQL 作为操作数据库的唯一接口，并且后续会以 SQL 语言特性为导向进行相关功能的开发。

### DuckDB

[DuckDB] 是一个轻量级嵌入式分析型数据库，可以类比作 OLAP 领域的 SQLite。

你可以按照[这里](https://duckdb.org/docs/installation/)的说明下载安装 DuckDB，然后在其中输入 SQL 命令进行体验。

RisingLight 的功能定位和 DuckDB 很相似，并且一定程度上参考了 DuckDB 的设计与实现。
如果大家在实现数据库的过程中遇到困难，那么 DuckDB 是一个很好的学习和参考对象。

[DuckDB]: https://duckdb.org

### SqlLogicTest

[SqlLogicTest] 是一个检验数据库执行结果正确性的测试框架，最早被用来测试 SQLite。它定义了一种脚本语言来描述 SQL 测试语句和期望输出结果。

[SqlLogicTest]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

以一个最简单的测试为例：

```
query I         # query 表示查询，I 表示期望输出是一个整数
SELECT 1        # 输入的 SQL 语句
----            # 分隔符，接下来描述期望结果
1               # 期望输出 1，测试器会检查它和数据库输出的字符串是否一致
```

我们的 RisingLight 就使用了 sqllogictest 来做端到端测试。你可以在 [`code/sql`] 文件夹下找到每个任务对应的 sqllogictest 测试脚本。

[`code/sql`]: https://github.com/singularity-data/risinglight/tree/main/code/sql

### Rust

RisingLight 使用 Rust 语言编写！

[Rust] 是新时代的系统级编程语言，主要为了解决 C/C++ 中的内存安全问题而生。
Rust 在不使用垃圾回收的前提下，通过引入所有权和生命周期等机制在编译期保证程序不会非法使用内存，使得程序既运行高效又安全可靠。
这使得它成为编写数据库的很好选择。

[Rust]: https://www.rust-lang.org

然而凡事都有两面，Rust 严格的语言机制使得它具有很高的学习门槛。并且一些在其它 OOP 语言中能轻而易举完成的操作，到了 Rust 中就很难实现。
我们在编写 RisingLight 的过程中也遇到了这些问题，在后面的章节中我们也会具体介绍应该如何处理或绕过它们。

整体来讲，使用 Rust 编写数据库能为我们带来很多好处。大家在后面的 coding 过程中可以亲自体会！

## 任务目标

简单介绍完了背景知识，下面我们就可以开始动手了！

在第一个任务中你需要从零开始搭起一个最简单的数据库框架。它需要提供一个可交互的终端，能够接收用户输入的 SQL 命令并输出结果。

接下来，我们向世界庄严宣告一个伟大的数据库项目从此诞生：

```sql
> SELECT 'Hello, world!'
Hello, world!
```

这就是我们要支持的第一个 SQL 命令：`SELECT` 一个常数，然后输出它：）

除此之外，我们还要搭起一个端到端测试框架，能够运行第一个 sqllogictest 脚本：`01-01.slt`。

## 整体设计

整个项目由以下部分组成：

* DB：数据库对象。其中包含两个模块：
    * Parser：负责解析 SQL 语句并生成抽象语法树（AST）
    * Executor：负责执行解析后的 SQL 语句
* Shell：一个可交互的命令行终端
* Test：一个基于 sqllogictest 脚本的端到端测试框架

### SQL Parser

为了读懂用户输入的 SQL 命令，你首先需要一个 SQL 解析器（Parser）。对于上面这条 SQL 语句来说，自己手写一个字符串解析就足够了。
不过随着之后 SQL 语句越来越复杂，解析 SQL 的复杂度也会与日俱增。既然我们又不是编译原理课，Parser 并不是我们关注的重点，
因此在 RisingLight 中我们推荐大家使用第三方库 [sqlparser-rs] 来完成 SQL 解析的工作，具体用法可以参考它的[文档]。

[sqlparser-rs]: https://github.com/sqlparser-rs/sqlparser-rs
[文档]: https://docs.rs/sqlparser/0.13.0/sqlparser/

简单来说，我们可以创建一个 [`Parser`] 对象，然后使用 [`parse_sql`] 方法将字符串解析成一颗抽象语法树（AST）。
抽象语法树的各个节点定义在 [`ast`] 模块中，我们可以从根节点 [`Statement`] 开始，一级一级地查看里面的内容。

[`Parser`]: https://docs.rs/sqlparser/0.13.0/sqlparser/parser/struct.Parser.html
[`parse_sql`]: https://docs.rs/sqlparser/0.13.0/sqlparser/parser/struct.Parser.html#method.parse_sql
[`ast`]: https://docs.rs/sqlparser/0.13.0/sqlparser/ast/index.html
[`Statement`]: https://docs.rs/sqlparser/0.13.0/sqlparser/ast/enum.Statement.html

当我们要实现解析某种特定的 SQL 语句时，一个好办法是直接 debug 输出解析后 AST 的完整结构，观察我们想要的东西分别被解析到了哪个位置上，然后在代码中提取相应的内容。

```rust,no_run
// 01-01/src/bin/print-ast.rs
{{#include ../../code/01-01/src/bin/print-ast.rs}}
```

例如我们使用上述代码，输入 `SELECT 'Hello, world!'`，就会得到以下输出：

```
Ok(
    [
        Query(
            Query {
                with: None,
                body: Select(
                    Select {
                        distinct: false,
                        top: None,
                        projection: [
                            UnnamedExpr(
                                Value(
                                    SingleQuotedString(
                                        "Hello, world!",
                                    ),
                                ),
                            ),
                        ],
                        from: [],
                        lateral_views: [],
                        selection: None,
                        group_by: [],
                        cluster_by: [],
                        distribute_by: [],
                        sort_by: [],
                        having: None,
                    },
                ),
                order_by: [],
                limit: None,
                offset: None,
                fetch: None,
            },
        ),
    ],
)
```

### SqlLogicTest

## 源码解析

TODO
<!-- 
不知 mdbook 有没有办法自动折叠这段
 -->