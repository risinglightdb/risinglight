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

// Stage 2: Execute the queries.
pub mod executor;

pub use self::db::{Database, Error};
