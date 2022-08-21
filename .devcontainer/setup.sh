## update and install some things we should probably have
apt-get update
apt-get install -y \
  curl \
  git \
  gnupg2 \
  jq \
  vim \
  build-essential \
  openssl \
  cmake \
  protobuf-compiler

## Install rustup and common components
curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"
toolchain=$(cat rust-toolchain)
rustup install $toolchain
rustup component add rustfmt --toolchain $toolchain
rustup component add clippy --toolchain $toolchain

cargo install cargo-expand
cargo install cargo-edit
cargo install cargo-nextest
