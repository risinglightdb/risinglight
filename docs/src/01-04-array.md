# 内存存储

有了表之后，我们就可以向其中导入数据了！数据在被持久化存储之前，会首先写入到内存中。
因此在本节中我们会先实现一个**内存存储系统（In-memory Storage）**。

对于分析型数据库（OLAP）而言，为了读取和计算的高效，它们大多使用**列式存储**，即同一列的数据在内存中被紧密地排列在一起。
这种数据结构就是我们所熟悉的**数组（Array）**。

每一列的数据用一个数组表示，多个列的数据就组成了**数据块（DataChunk）**。和 `Array` 一样，`DataChunk` 也是数据库中重要的基础类型。在我们的内存存储系统中，数据就是以 `DataChunk` 的形式存储在表内。同时它也是未来数据库执行引擎中，各个算子之间传递数据的基本类型。

<!-- toc -->

## 任务目标

对于四种数据类型：布尔 `bool`、整数 `i32`、浮点数 `f64`、字符串 `&str`，实现它们的 `Array`，以及用来构建数组的 `ArrayBuilder`。

此外，还需实现一个简单的内存存储系统，支持插入、删除表，并支持向表中插入数据、从表中读取数据。

一种可供参考的接口设计：

```rust
pub struct InMemoryStorage {...}

impl InMemoryStorage {
    pub fn new() -> Self {...}
    pub fn add_table(&self, id: TableRefId) -> StorageResult<()> {...}
    pub fn get_table(&self, id: TableRefId) -> StorageResult<Arc<InMemoryTable>> {...}
}


pub struct InMemoryTable {...}

impl InMemoryTable {
    pub fn append(&self, chunk: DataChunk) -> StorageResult<()> {...}
    pub fn all_chunks(&self) -> StorageResult<Vec<DataChunkRef>> {...}
}
```

除此之外，本节没有新增的 SQL 测试。
