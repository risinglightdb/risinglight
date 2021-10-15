#![deny(unused_must_use)]

#[macro_use]
extern crate log;

pub mod array;
pub mod binder;
pub mod catalog;
mod db;
pub mod executor;
pub mod expr;
pub mod logical_planner;
pub mod parser;
pub mod physical_planner;
pub mod storage;
pub mod types;

pub use self::db::{Database, Error};

use jemallocator::Jemalloc;

/// Jemalloc can significantly improve performance compared to the default system allocator.
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
