# RisingLight

[![CI](https://github.com/singularity-data/risinglight/workflows/CI/badge.svg?branch=main)](https://github.com/singularity-data/risinglight/actions)

RisingLight is an OLAP database system for educational purpose.

### Quick Start

Run interactive shell:

```
cargo run
```

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

### Contributing

Developers are required to run unit tests and pass clippy check before submitting PRs.

```
cargo test
cargo fmt
cargo clippy --all-features --all-targets
```
