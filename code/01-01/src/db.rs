//! Top-level structure of the database.

use crate::executor::{execute, ExecuteError};
use crate::parser::{parse, ParserError};

/// The database instance.
#[derive(Default)]
pub struct Database {}

impl Database {
    /// Create a new database instance.
    pub fn new() -> Self {
        Database {}
    }

    /// Run SQL queries and return the outputs.
    pub fn run(&self, sql: &str) -> Result<Vec<String>, Error> {
        // parse
        let stmts = parse(sql)?;

        let mut outputs = vec![];
        for stmt in stmts {
            debug!("execute: {:#?}", stmt);
            let output = execute(&stmt);
            outputs.extend(output);
        }
        Ok(outputs)
    }
}

/// The error type of database operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("parse error: {0}")]
    Parse(#[from] ParserError),
    #[error("execute error: {0}")]
    Execute(#[from] ExecuteError),
}
