# RisingLight

[![CI](https://github.com/singularity-data/risinglight/workflows/CI/badge.svg?branch=main)](https://github.com/singularity-data/risinglight/actions)

RisingLight is an OLAP database system for educational purpose. It is still in rapid development.

## Quick Start

Currently, RisingLight only supports Linux or macOS. If you are familiar with the Rust programming language, you can
start an interactive shell with:

```
cargo run
# or start in release mode
cargo run --release
```

Otherwise, see [Install, Run, and Develop RisingLight](docs/00-develop.md) for more information. We provide
step-by-step guide on how to compile and run RisingLight from scratch.

## Documentation

All documentation can be found in [docs](docs/) folder. At the same time, dev docs are also available in `make docs`
(latest) or [crates.io](https://docs.rs/risinglight) (stable, to-be-released).

## Roadmap

We plan to release a stable version of RisingLight in the near future, as well as a tutorial in Chinese on how to build an OLAP database from scratch. See the pinned
[Roadmap](https://github.com/singularity-data/risinglight/issues/317) issue for more information.

## License

RisingLight is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.

## Contributing

If you have a bug report or feature request, welcome to open an [issue](https://github.com/singularity-data/risinglight/issues).

If you have any question or want to discuss, join our Slack channel or start a discussion on
[GitHub Discussions](https://github.com/singularity-data/risinglight/discussions).

<!-- TODO: add Slack channel -->

If you want to contribute code, see [CONTRIBUTING](CONTRIBUTING.md) for more information. Generally, you will need to
pass necessary checks for your changes and sign DCO before submitting PRs.

Welcome to the RisingLight community!
