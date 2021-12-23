//! RisingLight -- an educational OLAP database.

#![deny(clippy::explicit_into_iter_loop)]
#![deny(clippy::explicit_iter_loop)]
#![deny(unused_must_use)]
#![feature(portable_simd)]
#![feature(generators)]

// Enable macros for logging.
#[macro_use]
extern crate log;

/// Top-level structure of the database.
pub mod db;

/// Stage 1: Parse the SQL string into an Abstract Syntax Tree (AST).
pub mod parser;

/// Stage 2: Resolve all expressions referring with their names.
pub mod binder;

/// Stage 3: Transform the parse tree into a logical operations tree.
pub mod logical_planner;

/// Stage 4: Do query optimization.
pub mod optimizer;

/// Stage 5: Execute the queries.
pub mod executor;

/// In-memory representations of a column values.
pub mod array;
/// Metadata of database objects.
pub mod catalog;
/// Persistent storage engine.
pub mod storage;
/// Basic type definitions.
pub mod types;

use jemallocator::Jemalloc;

pub use self::db::{Database, Error};

/// Jemalloc can significantly improve performance compared to the default system allocator.
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
