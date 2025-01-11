//! The error type of bind operations.
//!
//! To raise an error in binder, construct an `ErrorKind` and attach a span if possible:
//!
//! ```ignore
//! return Err(ErrorKind::InvalidTable("table".into()).into());
//! return Err(ErrorKind::InvalidTable("table".into()).with_span(ident.span));
//! return Err(ErrorKind::InvalidTable("table".into()).with_spanned(object_name));
//! ```

use sqlparser::ast::{Ident, ObjectType, Spanned};
use sqlparser::tokenizer::Span;

use crate::planner::TypeError;

/// The error type of bind operations.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub struct BindError(#[from] Box<Inner>);

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
struct Inner {
    #[source]
    kind: ErrorKind,
    span: Option<Span>,
    sql: Option<String>,
}

impl std::fmt::Display for BindError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)?;
        if let Some(sql) = &self.sql
            && let Some(span) = self.span
        {
            write!(f, "\n\n{}", highlight_sql(sql, span))?;
        } else if let Some(span) = self.span {
            // " at Line: {}, Column: {}"
            write!(f, "{}", span.start)?;
        }
        Ok(())
    }
}

/// The error type of bind operations.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    #[error("invalid schema {0:?}")]
    InvalidSchema(String),
    #[error("invalid table {0:?}")]
    InvalidTable(String),
    #[error("invalid index {0:?}")]
    InvalidIndex(String),
    #[error("invalid column {0:?}")]
    InvalidColumn(String),
    #[error("table {0:?} already exists")]
    TableExists(String),
    #[error("column {0:?} already exists")]
    ColumnExists(String),
    #[error("duplicated alias {0:?}")]
    DuplicatedAlias(String),
    #[error("duplicate CTE name {0:?}")]
    DuplicatedCteName(String),
    #[error("table {0:?} has {1} columns available but {2} columns specified")]
    ColumnCountMismatch(String, usize, usize),
    #[error("invalid expression {0}")]
    InvalidExpression(String),
    #[error("not nullable column {0:?}")]
    NotNullableColumn(String),
    #[error("ambiguous column {0:?} (use {1})")]
    AmbiguousColumn(String, String),
    #[error("invalid table name {0:?}")]
    InvalidTableName(Vec<Ident>),
    #[error("SQL not supported")]
    NotSupportedTSQL,
    #[error("invalid SQL")]
    InvalidSQL,
    #[error("cannot cast {0:?} to {1:?}")]
    CastError(crate::types::DataValue, crate::types::DataType),
    #[error("{0}")]
    BindFunctionError(String),
    #[error("type error: {0}")]
    TypeError(TypeError),
    #[error("aggregate function calls cannot be nested")]
    NestedAgg,
    #[error("WHERE clause cannot contain aggregates")]
    AggInWhere,
    #[error("GROUP BY clause cannot contain aggregates")]
    AggInGroupBy,
    #[error("window function calls cannot be nested")]
    NestedWindow,
    #[error("WHERE clause cannot contain window functions")]
    WindowInWhere,
    #[error("HAVING clause cannot contain window functions")]
    WindowInHaving,
    #[error("column {0:?} must appear in the GROUP BY clause or be used in an aggregate function")]
    ColumnNotInAgg(String),
    #[error("ORDER BY items must appear in the select list if DISTINCT is specified")]
    OrderKeyNotInDistinct,
    #[error("{0:?} is not an aggregate function")]
    NotAgg(String),
    #[error("unsupported object name: {0:?}")]
    UnsupportedObjectName(ObjectType),
    #[error("not supported yet: {0}")]
    Todo(String),
    #[error("can not copy to {0}")]
    CopyTo(String),
    #[error("can only insert into table")]
    CanNotInsert,
    #[error("can only delete from table")]
    CanNotDelete,
    #[error("VIEW aliases mismatch query result")]
    ViewAliasesMismatch,
    #[error("pragma does not exist: {0}")]
    NoPragma(String),
}

impl ErrorKind {
    /// Create a `BindError` with a span.
    pub fn with_span(self, span: Span) -> BindError {
        BindError(Box::new(Inner {
            kind: self,
            span: Some(span),
            sql: None,
        }))
    }

    /// Create a `BindError` with a span from a `Spanned` object.
    pub fn with_spanned(self, span: &impl Spanned) -> BindError {
        self.with_span(span.span())
    }
}

impl BindError {
    /// Set the SQL string for the error.
    pub fn with_sql(mut self, sql: &str) -> BindError {
        self.0.sql = Some(sql.to_string());
        self
    }
}

impl From<ErrorKind> for BindError {
    fn from(kind: ErrorKind) -> Self {
        BindError(Box::new(Inner {
            kind,
            span: None,
            sql: None,
        }))
    }
}

impl From<TypeError> for BindError {
    fn from(kind: TypeError) -> Self {
        BindError(Box::new(Inner {
            kind: ErrorKind::TypeError(kind),
            span: None,
            sql: None,
        }))
    }
}

/// Highlight the SQL string at the given span.
fn highlight_sql(sql: &str, span: Span) -> String {
    let lines: Vec<&str> = sql.lines().collect();
    if span.start.line == 0 || span.start.line as usize > lines.len() {
        return String::new();
    }

    let error_line = lines[span.start.line as usize - 1];
    let prefix = format!("LINE {}: ", span.start.line);
    let mut indicator = " ".repeat(prefix.len()).to_string();

    if span.start.column > 0 && span.start.column as usize <= error_line.len() {
        for _ in 1..span.start.column {
            indicator.push(' ');
        }
        let caret_count = if span.end.column > span.start.column {
            span.end.column - span.start.column
        } else {
            1
        };
        for _ in 0..caret_count {
            indicator.push('^');
        }
    }

    format!("{}{}\n{}", prefix, error_line, indicator)
}

#[cfg(test)]
mod tests {
    use sqlparser::tokenizer::Location;

    use super::*;

    #[test]
    fn test_bind_error_size() {
        assert_eq!(
            std::mem::size_of::<BindError>(),
            std::mem::size_of::<usize>(),
            "the size of BindError should be one pointer"
        );
    }

    #[test]
    fn test_highlight_sql() {
        let sql = "SELECT * FROM table WHERE id = 1";
        let span = Span::new(Location::new(1, 15), Location::new(1, 20));
        assert_eq!(
            highlight_sql(sql, span),
            "
LINE 1: SELECT * FROM table WHERE id = 1
                      ^^^^^
            "
            .trim()
        );
    }
}
