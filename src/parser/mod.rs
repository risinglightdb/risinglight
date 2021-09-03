macro_rules! try_match {
    ($e:expr, $pat:pat => $ok:expr, $desc:literal) => {
        match &$e {
            $pat => $ok,
            _ => return Err(ParseError::NotFound($desc)),
        }
    };
}

mod expression;
mod sql_parser;
mod statement;
mod table_ref;

pub(crate) use sql_parser::*;
pub(crate) use statement::*;

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("unexpected statement, expected: {0}")]
    NotFound(&'static str),
    #[error("invalid argument: {0}")]
    InvalidInput(&'static str),
    #[error("duplicate {0}")]
    Duplicate(&'static str),
}

impl ParseError {}
