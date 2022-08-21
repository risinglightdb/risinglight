// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! RisingLight sqllogictest

use risinglight_sqllogictest::{run, Engine};

fn main() {
    const DISK_BLOCKLIST: &[&str] = &[];
    run(DISK_BLOCKLIST, Engine::Disk)
}
