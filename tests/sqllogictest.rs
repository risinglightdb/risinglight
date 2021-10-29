//! [Sqllogictest][Sqllogictest] parser and runner.
//!
//! [Sqllogictest]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

use log::*;
use std::path::Path;
use test_case::test_case;

#[test_case("basic_test.slt")]
#[test_case("operator.slt")]
#[test_case("nullable_and_or_eval.slt")]
#[test_case("filter.slt")]
#[test_case("order_by.slt")]
#[test_case("create.test")]
#[test_case("insert.test")]
#[test_case("select.test")]
#[test_case("join.slt")]
fn sqllogictest(name: &str) {
    init_logger();
    let script = std::fs::read_to_string(Path::new("tests/sql").join(name)).unwrap();
    let records = parse(&script).expect("failed to parse sqllogictest");
    let mut tester = SqlLogicTester::new(Database::new_in_memory());

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(tester.test_multi(records));
}

fn init_logger() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(env_logger::init);
}

#[derive(Debug, PartialEq, Clone)]
pub enum Record {
    Statement {
        conditions: Vec<Condition>,
        /// The SQL command is expected to fail instead of to succeed.
        error: bool,
        /// The SQL command.
        sql: String,
    },
    Query {
        conditions: Vec<Condition>,
        // type_string: String,
        sort_mode: SortMode,
        label: Option<String>,
        sql: String,
        expected_results: Vec<ColumnValues>,
    },
    Halt,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Condition {
    OnlyIf { db_name: String },
    SkipIf { db_name: String },
}

#[derive(Debug, PartialEq, Clone)]
pub enum ColumnValues {
    Int(Vec<Option<i32>>),
    Float(Vec<Option<f64>>),
    Text(Vec<Option<String>>),
    Bool(Vec<Option<bool>>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum SortMode {
    NoSort,
    RowSort,
    ValueSort,
}

#[derive(thiserror::Error, Debug, PartialEq, Clone)]
pub enum ParseError {
    #[error("unexpected token: {0:?}")]
    UnexpectedToken(String),
    #[error("unexpected EOF")]
    UnexpectedEOF,
    #[error("invalid sort mode: {0:?}")]
    InvalidSortMode(String),
    #[error("invalid line: {0:?}")]
    InvalidLine(String),
    #[error("invalid type string: {0:?}")]
    InvalidType(String),
    #[error("value length mismatch: {0:?}")]
    LengthMismatch(String),
    #[error("{0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("{0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("{0}")]
    ParseBool(#[from] std::str::ParseBoolError),
}

impl ColumnValues {
    #[allow(dead_code)]
    fn len(&self) -> usize {
        match self {
            ColumnValues::Int(c) => c.len(),
            ColumnValues::Float(c) => c.len(),
            ColumnValues::Text(c) => c.len(),
            ColumnValues::Bool(c) => c.len(),
        }
    }
}

pub fn parse(script: &str) -> Result<Vec<Record>, ParseError> {
    let mut lines = script.split('\n');
    let mut records = vec![];
    let mut conditions = vec![];
    while let Some(line) = lines.next() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let tokens: Vec<&str> = line.split_whitespace().collect();
        match tokens.as_slice() {
            [] => continue,
            ["halt"] => {
                records.push(Record::Halt);
                break;
            }
            ["skipif", db_name] => {
                conditions.push(Condition::SkipIf {
                    db_name: db_name.to_string(),
                });
            }
            ["onlyif", db_name] => {
                conditions.push(Condition::OnlyIf {
                    db_name: db_name.to_string(),
                });
            }
            &["statement", res] => {
                let error = match res {
                    "ok" => false,
                    "error" => true,
                    _ => return Err(ParseError::UnexpectedToken(res.into())),
                };
                let mut sql = lines.next().ok_or(ParseError::UnexpectedEOF)?.into();
                for line in &mut lines {
                    if line.is_empty() {
                        break;
                    }
                    sql += line;
                }
                records.push(Record::Statement {
                    conditions: std::mem::take(&mut conditions),
                    error,
                    sql,
                });
            }
            ["query", type_string, res @ ..] => {
                let mut values = vec![];
                for c in type_string.chars() {
                    match c {
                        'T' => values.push(ColumnValues::Text(vec![])),
                        'I' => values.push(ColumnValues::Int(vec![])),
                        'R' => values.push(ColumnValues::Float(vec![])),
                        'B' => values.push(ColumnValues::Bool(vec![])),
                        _ => return Err(ParseError::InvalidType(type_string.to_string())),
                    }
                }
                let sort_mode = match res.get(0) {
                    None | Some(&"nosort") => SortMode::NoSort,
                    Some(&"rowsort") => SortMode::RowSort,
                    Some(&"valuesort") => SortMode::ValueSort,
                    Some(mode) => return Err(ParseError::InvalidSortMode(mode.to_string())),
                };
                let label = res.get(1).map(|s| s.to_string());
                // The SQL for the query is found on second an subsequent lines of the record
                // up to first line of the form "----" or until the end of the record.
                let mut sql = lines.next().ok_or(ParseError::UnexpectedEOF)?.into();
                let mut has_result = false;
                for line in &mut lines {
                    if line.is_empty() || line == "----" {
                        has_result = line == "----";
                        break;
                    }
                    sql += line;
                }
                // Lines following the "----" are expected results of the query, one value per line.
                if has_result {
                    for line in &mut lines {
                        if line.is_empty() {
                            break;
                        }
                        if line.split_whitespace().count() != values.len() {
                            return Err(ParseError::LengthMismatch(line.into()));
                        }
                        for (v, col) in line.split_whitespace().zip(values.iter_mut()) {
                            match col {
                                ColumnValues::Int(c) if v == "NULL" => c.push(None),
                                ColumnValues::Float(c) if v == "NULL" => c.push(None),
                                ColumnValues::Text(c) if v == "NULL" => c.push(None),
                                ColumnValues::Text(c) if v == "(empty)" => c.push(Some("".into())),
                                ColumnValues::Bool(c) if v == "NULL" => c.push(None),
                                ColumnValues::Int(c) => c.push(Some(v.parse()?)),
                                ColumnValues::Float(c) => c.push(Some(v.parse()?)),
                                ColumnValues::Text(c) => c.push(Some(v.into())),
                                ColumnValues::Bool(c) => c.push(Some(v.parse()?)),
                            }
                        }
                    }
                }
                records.push(Record::Query {
                    conditions: std::mem::take(&mut conditions),
                    sort_mode,
                    label,
                    sql,
                    expected_results: values,
                });
            }
            _ => return Err(ParseError::InvalidLine(line.into())),
        }
    }
    Ok(records)
}

use risinglight::{array::*, Database};

impl From<ColumnValues> for ArrayImpl {
    fn from(col: ColumnValues) -> Self {
        match col {
            ColumnValues::Int(c) => c.into_iter().collect::<PrimitiveArray<i32>>().into(),
            ColumnValues::Float(c) => c.into_iter().collect::<PrimitiveArray<f64>>().into(),
            ColumnValues::Bool(c) => c.into_iter().collect::<PrimitiveArray<bool>>().into(),
            ColumnValues::Text(c) => c
                .iter()
                .map(|o| o.as_ref().map(|s| s.as_str()))
                .collect::<UTF8Array>()
                .into(),
        }
    }
}

struct SqlLogicTester {
    db: Database,
}

impl SqlLogicTester {
    pub fn new(db: Database) -> Self {
        SqlLogicTester { db }
    }

    pub async fn test(&mut self, record: Record) {
        info!("test: {:?}", record);
        match record {
            Record::Statement { error, sql, .. } => {
                let ret = self.db.run(&sql).await;
                match ret {
                    Ok(_) if error => panic!(
                        "statement is expected to fail, but actually succeed: {:?}",
                        sql
                    ),
                    Err(e) if !error => panic!("statement failed: {}\n\tSQL: {:?}", e, sql),
                    _ => {}
                }
            }
            Record::Query {
                sql,
                expected_results,
                ..
            } => {
                let output = self.db.run(&sql).await.expect("query failed");
                let actual = output
                    .get(0)
                    .expect("expect result from query, but no output");
                let expected = expected_results.into_iter().map(ArrayImpl::from).collect();
                if *actual != expected {
                    panic!(
                        "query result mismatch:\nSQL:\n{}\n\nExpected:\n{}\nActual:\n{}",
                        sql, expected, actual
                    );
                }
            }
            Record::Halt => {}
        }
    }

    pub async fn test_multi(&mut self, records: impl IntoIterator<Item = Record>) {
        for record in records.into_iter() {
            if let Record::Halt = record {
                return;
            }
            self.test(record).await;
        }
    }
}
