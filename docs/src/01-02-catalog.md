# Catalog

数据库通过 Catalog 描述内部对象的结构信息。

在实际存储任何数据之前，我们需要首先定义好“元数据”。

<!-- toc -->

## 背景知识

### Catalog

TODO

## 任务目标

实现 Catalog 相关数据结构，包括：Database，Schema，Table，Column 四个层级。

其中每一级都至少支持 插入、删除、查找 三种操作。

一种可供参考的接口设计：

```rust,no_run
pub struct DatabaseCatalog {...}

impl DatabaseCatalog {
    pub fn add_schema(&self, name: &str) -> SchemaId {...}
    pub fn get_schema(&self, id: SchemaId) -> Option<Arc<SchemaCatalog>> {...}
    pub fn del_schema(&self, id: SchemaId) {...}
}


pub struct SchemaCatalog {...}

impl SchemaCatalog {
    pub fn id(&self) -> SchemaId {...}
    pub fn name(&self) -> String {...}
    pub fn add_table(&self, name: &str) -> TableId {...}
    pub fn get_table(&self, id: TableId) -> Option<Arc<TableCatalog>> {...}
    pub fn del_table(&self, id: TableId) {...}
}


pub struct TableCatalog {...}

impl TableCatalog {
    pub fn id(&self) -> TableId {...}
    pub fn name(&self) -> String {...}
    pub fn add_column(&self, name: &str) -> ColumnId {...}
    pub fn get_column(&self, id: ColumnId) -> Option<Arc<ColumnCatalog>> {...}
    pub fn del_column(&self, id: ColumnId) {...}
    pub fn all_columns(&self) -> Vec<Arc<ColumnCatalog>> {...}
}


pub struct ColumnCatalog {...}

impl ColumnCatalog {
    pub fn id(&self) -> ColumnId {...}
    pub fn name(&self) -> String {...}
    pub fn datatype(&self) -> DataType {...}
}
```

除此之外，本节没有新增的 SQL 测试。
