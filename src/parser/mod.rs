macro_rules! try_match {
    ($e:expr, $pat:pat => $ok:expr, $desc:literal) => {
        match &$e {
            $pat => $ok,
            _ => return Err(ParseError::NotFound($desc)),
        }
    };
}

mod expression;
mod statement;
mod table_ref;

pub(crate) use self::statement::*;
pub use expression::*;
pub use postgres_parser::PgParserError;
pub use table_ref::*;

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("unexpected statement, expected: {0}")]
    NotFound(&'static str),
    #[error("invalid argument: {0}")]
    InvalidInput(&'static str),
    #[error("duplicate {0}")]
    Duplicate(&'static str),
    #[error("postgres parser error: {0:?}")]
    Pg(PgParserError),
}

impl From<PgParserError> for ParseError {
    fn from(pg: PgParserError) -> Self {
        Self::Pg(pg)
    }
}
