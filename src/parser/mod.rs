mod sql_parser;
mod statement;

pub(crate) use sql_parser::*;

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("unexpected statement, expected: {0}")]
    NotFound(&'static str),
    #[error("invalid argument: {0}")]
    InvalidInput(&'static str),
}

impl ParseError {}
