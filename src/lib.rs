// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! RisingLight -- an educational OLAP database.

#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::map_flatten)]
#![deny(unused_must_use)]
#![feature(portable_simd)]
#![feature(generators)]
#![feature(error_generic_member_access)]
#![feature(provide_any)]
#![feature(type_alias_impl_trait)]
#![feature(stmt_expr_attributes)]
#![feature(proc_macro_hygiene)]
#![feature(core_intrinsics)]
#![feature(trusted_len)]
#![feature(once_cell)]
#![feature(array_methods)]
#![feature(iterator_try_collect)]
#![feature(async_fn_in_trait)]
#![feature(return_position_impl_trait_in_trait)]
#![allow(incomplete_features)]

/// Top-level structure of the database.
pub mod db;

/// Parse the SQL string into an Abstract Syntax Tree (AST).
pub mod parser;

/// Convert the parser AST to planner AST.
pub mod binder_v2;

/// Egg-based planner and optimizer.
pub mod planner;

/// Execute the queries.
pub mod executor_v2;

/// The legacy query engine.
pub mod v1 {
    /// Resolve all expressions referring with their names.
    pub mod binder;

    /// Transform the parse tree into a logical operations tree.
    pub mod logical_planner;

    /// Do query optimization.
    pub mod optimizer;

    /// Execute the queries.
    pub mod executor;

    /// Functions
    pub mod function;
}

/// In-memory representations of a column values.
pub mod array;
/// Metadata of database objects.
pub mod catalog;
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
