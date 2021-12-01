# RisingLight

[![CI](https://github.com/singularity-data/risinglight/workflows/CI/badge.svg?branch=main)](https://github.com/singularity-data/risinglight/actions)

RisingLight is an OLAP database system for educational purpose.

### Quick Start

Run tests:

```
cargo test
```

Run benchmarks:

```
cargo bench
```

Run benchmarks with SIMD acceleration:
```
RUSTFLAGS='-C target-cpu=native' cargo bench --features=simd
```

Run interactive shell:

```
cargo run
```
