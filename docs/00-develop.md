# Install, Run, and Develop RisingLight

To run RisingLight, you will need Linux or macOS operating systems and the Rust toolchain.

## Install the Rust toolchain

The recommended way to set up the Rust toolchain is using [rustup](https://rustup.rs). The following command will set
up everything for you:

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Upon running the above command, the setup wizard will ask for a toolchain version. You may go ahead and use the default
toolchain.

For Chinese users, you may configure rustup to use a mirror to speed up download.

* [SJTUG Mirror](https://mirrors.sjtug.sjtu.edu.cn/docs/rust-static)
* [TUNA Mirror](https://mirrors.tuna.tsinghua.edu.cn/help/rustup/)

Rust toolchain has multiple release channels, e.g., stable channel and nightly channel. RisingLight requires a specific
version of nightly toolchain, as it is using some experimental features in the Rust compiler. However, some mirror
sites only retain recent versions of nightly releases. If you encountered errors when downloading Rust toolchains, you
may switch to a mirror site with full Rust toolchain or use the official rustup source.


## Compile RisingLight

After installing the Rust toolchain, you may download and compile RisingLight.

```shell
git clone https://github.com/singularity-data/risinglight
cd risinglight
cargo build # Or cargo build --release
```

For Chinese users, you may configure a mirror for cargo to speed up downloading dependencies:

* [SJTUG Mirror](https://mirrors.sjtug.sjtu.edu.cn/docs/crates.io)
* [TUNA Mirror](https://mirrors.tuna.tsinghua.edu.cn/help/crates.io-index.git/)
* [USTC Mirror](https://mirrors.ustc.edu.cn/help/crates.io-index.html)

## Run RisingLight

RisingLight provides a SQL interactive shell. Simply use `cargo run` to start the shell.

```shell
cargo run
```

You may refer to [Importing TPC-H Data](01-tpch.md) for supported query types.

## Development

It is recommended to use VSCode with [rust-analyzer][rust-analyzer] extension to develop RisingLight. Simply install
`rust-analyzer` extension in VSCode, and everything will be set for you. Note that `rust-analyzer` conflicts with
the official Rust extension. You will need to uninstall "The Rust Programming Language" extension before proceeding.

To enable logs for RisingLight, export the following environment variable to your shell environment.

```
export RUST_LOG=info
```

If you want to contribute to the RisingLight project, refer to [Contributing to RisingLight](../CONTRIBUTING.md) docs
for more information.

[rust-analyzer]: https://marketplace.visualstudio.com/items?itemName=matklad.rust-analyzer
