#!/bin/bash

set -xe

rm -rf risinglight.secondary.db
make tpch

cargo build --release
cargo run --release -- -f tests/sql/tpch/create.sql
cargo run --release -- -f tests/sql/tpch/import.sql
cargo run --release -- -f tests/sql/tpch-full/_tpch_full.slt
