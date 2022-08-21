// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

#![allow(clippy::derive_partial_eq_without_eq)]

pub mod rowset {
    include!(concat!(env!("OUT_DIR"), "/risinglight.rowset.rs"));
}
