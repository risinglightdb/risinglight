#!/bin/bash

set -e

rm -rf risinglight.secondary.db
make tpch
cargo run --release -- -f tests/sql/tpch/create.sql
cargo run --release -- -f tests/sql/tpch/import.sql
cargo run --release -- -f tests/sql/tpch-full/_tpch_full.slt
