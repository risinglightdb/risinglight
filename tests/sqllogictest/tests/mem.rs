// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! RisingLight sqllogictest

use risinglight_sqllogictest::{run, Engine};

fn main() {
    const MEM_BLOCKLIST: &[&str] = &["statistics.slt"];
    run(MEM_BLOCKLIST, Engine::Mem)
}
