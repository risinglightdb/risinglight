# RisingLight

[![CI](https://github.com/risinglightdb/risinglight/workflows/CI/badge.svg?branch=main)](https://github.com/risinglightdb/risinglight/actions)

RisingLight is an OLAP database system for educational purpose. It is still in rapid development, and should not be used in production.

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

After successfully building RisingLight, you may import some data and run SQL queries. See [Running TPC-H Queries](docs/01-tpch.md).

## Documentation

All documentation can be found in [docs](docs/) folder. At the same time, dev docs are also available in `make docs`
(latest) or [crates.io](https://docs.rs/risinglight) (stable, to-be-released).

## Roadmap

We plan to release a stable version of RisingLight in the near future, as well as a tutorial in Chinese on how to build an OLAP database from scratch. See the pinned
[Roadmap](https://github.com/risinglightdb/risinglight/issues/317) issue for more information.

## License

RisingLight is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.

## Community

RisingLight developers are active in a variety of places:

### Slack Channel

The RisingLight project is currently part of the RisingWave community. You may join RisingWave community with the [invitation link](https://join.slack.com/t/risingwave-community/shared_invite/zt-120rft0mr-d8uGk3d~NZiZAQWPnElOfw).

After that, you may join either `risinglight-chinese` or `risinglight-english` channel.

* [risinglight-chinese](https://risingwave-community.slack.com/archives/C02UZDEE4AC)
* [risinglight-english](https://risingwave-community.slack.com/archives/C030SJRDT4J)

### WeChat Group

If you want to join our WeChat group, please send an email to `hezi at singularity-data.com` with your WeChat ID. We will invite you to the WeChat group.

## Contributing

If you have a bug report or feature request, welcome to open an [issue](https://github.com/risinglightdb/risinglight/issues).

If you have any question or want to discuss, join our WeChat group, Slack channel, or start a discussion on
[GitHub Discussions](https://github.com/risinglightdb/risinglight/discussions).

If you want to contribute code, see [CONTRIBUTING](CONTRIBUTING.md) for more information. Generally, you will need to
pass necessary checks for your changes and sign DCO before submitting PRs. We have plenty of [good first issues](https://github.com/risinglightdb/risinglight/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22). Feel free to ask questions either on GitHub or in our chat groups if you meet any difficulty.

Welcome to the RisingLight community!
