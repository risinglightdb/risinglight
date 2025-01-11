// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! RisingLight -- an educational OLAP database.

#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::map_flatten)]
#![deny(unused_must_use)]
#![feature(array_chunks)]
#![feature(portable_simd)]
#![feature(error_generic_member_access)]
#![feature(stmt_expr_attributes)]
#![feature(proc_macro_hygiene)]
#![feature(trusted_len)]
#![feature(adt_const_params)]
#![feature(iterator_try_collect)]
#![feature(let_chains)]
#![feature(impl_trait_in_assoc_type)]
#![feature(coroutines)]
#![allow(incomplete_features)]

/// Top-level structure of the database.
pub mod db;

/// Parse the SQL string into an Abstract Syntax Tree (AST).
pub mod parser;

/// Convert the parser AST to planner AST.
pub mod binder;

/// Egg-based planner and optimizer.
pub mod planner;

/// Execute the queries.
pub mod executor;

/// In-memory representations of a column values.
pub mod array;
/// Metadata of database objects.
pub mod catalog;
/// Python Extension
#[cfg(feature = "python")]
pub mod python;
/// Postgres wire protocol.
pub mod server;
/// Persistent storage engine.
pub mod storage;
/// Basic type definitions.
pub mod types;
/// Utilities.
pub mod utils;

#[cfg(feature = "jemalloc")]
use tikv_jemallocator::Jemalloc;

pub use self::db::{Database, Error};

/// Jemalloc can significantly improve performance compared to the default system allocator.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
