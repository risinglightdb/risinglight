//! RisingLight -- an educational OLAP database.

#![deny(unused_must_use)]

// Enable macros for logging.
#[macro_use]
extern crate log;

#[cfg(test)]
mod test;

// Top-level structure of the database.
pub mod db;

// Stage 1: Parse the SQL string into an Abstract Syntax Tree (AST).
pub mod parser;

// Stage 2: Resolve all expressions referring with their names.
pub mod binder;

// Stage 3: Transform the parse tree into a logical operations tree.
pub mod logical_planner;

// Stage 4: Transform the logical plan into the physical plan.
pub mod physical_planner;

// Stage 5: Execute the plans.
pub mod executor;

pub mod array;
pub mod catalog;
pub mod storage;
pub mod types;

pub use self::db::{Database, Error};
