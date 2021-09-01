# lightdb

[![CI](https://github.com/MingjiHan99/lightdb/workflows/CI/badge.svg?branch=main)](https://github.com/MingjiHan99/lightdb/actions)

LightDB is an OLAP database system for educational purpose.

### Quick Start

```
make deps

```

### Progress

- [ ] Implement basic catalog (Mingji)
- [ ] Implement a parser tree transformer, we need to transfer AST into our own statement definition. [Reference](https://github.com/singularity-data/risingwave/tree/master/cpp/src/parser/statement)
- [ ] Implement a very basic binder, which can process `create table t1 (v1 int not null, v2 int not null)`, `insert into t1 (1,2), (3, 4), (5,6)` and `select v1, v2, v1 + v2 from t1`.
