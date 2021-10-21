extern crate prost_build;

fn main() {
    prost_build::compile_protos(&["src/proto/rowset.proto"], &["src/proto"]).unwrap();
}
