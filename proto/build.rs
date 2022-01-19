// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

extern crate prost_build;

fn main() {
    prost_build::compile_protos(&["src/proto/rowset.proto"], &["src/proto"]).unwrap();
}
