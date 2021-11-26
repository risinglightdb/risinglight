//! [Sqllogictest][Sqllogictest] parser and runner.
//!
//! [Sqllogictest]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

use itertools::Itertools;
use log::*;
use risinglight::{array::*, storage::SecondaryStorageOptions, types::DataValue, Database};
use std::path::Path;
use tempfile::{tempdir, TempDir};
use test_case::test_case;

#[test_case("basic_test.slt")]
#[test_case("operator.slt")]
#[test_case("nullable_and_or_eval.slt")]
#[test_case("filter.slt")]
#[test_case("order_by.slt")]
#[test_case("create.test")]
// #[test_case("insert.test")]
#[test_case("select.test")]
#[test_case("join.slt")]
#[test_case("limit.slt")]
#[test_case("type.slt")]
#[test_case("aggregation.slt")]
#[test_case("delete.slt")]
#[test_case("copy/csv.slt")]
#[test_case("where.slt")]
// #[test_case("select.slt")]
// #[test_case("issue_347.slt")]
fn sqllogictest(name: &str) {
    init_logger();
    let script = std::fs::read_to_string(Path::new("tests/sql").join(name)).unwrap();
    let records = parse(&script).expect("failed to parse sqllogictest");
    let mut tester = SqlLogicTester::new(Database::new_in_memory());

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(tester.test_multi(records));
}

#[test_case("basic_test.slt")]
#[test_case("operator.slt")]
#[test_case("nullable_and_or_eval.slt")]
#[test_case("filter.slt")]
#[test_case("order_by.slt")]
#[test_case("create.test")]
// #[test_case("insert.test")]
#[test_case("select.test")]
#[test_case("join.slt")]
#[test_case("limit.slt")]
#[test_case("type.slt")]
#[test_case("aggregation.slt")]
#[test_case("delete.slt")]
// #[test_case("copy/csv.slt")]
// #[test_case("where.slt")]
// #[test_case("select.slt")]
// #[test_case("issue_347.slt")]
fn sqllogictest_disk(name: &str) {
    init_logger();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        let temp_dir = tempdir().unwrap();
        let script = std::fs::read_to_string(Path::new("tests/sql").join(name)).unwrap();
        let records = parse(&script).expect("failed to parse sqllogictest");
        let mut tester = SqlLogicTester::new(
            Database::new_on_disk(SecondaryStorageOptions::default_for_test(
                temp_dir.path().to_path_buf(),
            ))
            .await,
        );
        tester.test_multi(records).await;
        tester.db.shutdown().await.unwrap();
    });
}

fn init_logger() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(env_logger::init);
}

#[derive(Debug, PartialEq, Clone)]
pub enum Record {
    /// A statement is an SQL command that is to be evaluated but from which we do not expect to
    /// get results (other than success or failure).
    Statement {
        line: u32,
        conditions: Vec<Condition>,
        /// The SQL command is expected to fail instead of to succeed.
        error: bool,
        /// The SQL command.
        sql: String,
    },
    /// A query is an SQL command from which we expect to receive results. The result set might be
    /// empty.
    Query {
        line: u32,
        conditions: Vec<Condition>,
        type_string: String,
        sort_mode: SortMode,
        label: Option<String>,
        /// The SQL command.
        sql: String,
        /// The expected results.
        expected_results: Vec<String>,
    },
    /// Subtest.
    Subtest { name: String },
    /// A halt record merely causes sqllogictest to ignore the rest of the test script.
    /// For debugging use only.
    Halt,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Condition {
    /// The statement or query is skipped if an `onlyif` record for a different database engine is
    /// seen.
    OnlyIf { db_name: String },
    /// The statement or query is not evaluated if a `skipif` record for the target database engine
    /// is seen in the prefix.
    SkipIf { db_name: String },
}

#[derive(Debug, PartialEq, Clone)]
pub enum SortMode {
    /// The default option. The results appear in exactly the order in which they were received
    /// from the database engine.
    NoSort,
    /// Gathers all output from the database engine then sorts it by rows.
    RowSort,
    /// It works like rowsort except that it does not honor row groupings. Each individual result
    /// value is sorted on its own.
    ValueSort,
}

/// The error type for parsing sqllogictest.
#[derive(thiserror::Error, Debug, PartialEq, Clone)]
#[error("parse error at line {line}: {kind}")]
pub struct Error {
    kind: ErrorKind,
    line: u32,
}

impl Error {
    /// Returns the corresponding [`ErrorKind`] for this error.
    pub fn kind(&self) -> ErrorKind {
        self.kind.clone()
    }

    /// Returns the line number from which the error originated.
    pub fn line(&self) -> u32 {
        self.line
    }
}

/// The error type for parsing sqllogictest.
#[derive(thiserror::Error, Debug, PartialEq, Clone)]
pub enum ErrorKind {
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
}

impl ErrorKind {
    fn at(self, line: usize) -> Error {
        Error {
            kind: self,
            line: line as u32,
        }
    }
}

/// Parse a sqllogictest script into a list of records.
pub fn parse(script: &str) -> Result<Vec<Record>, Error> {
    let mut lines = script.split('\n').enumerate();
    let mut records = vec![];
    let mut conditions = vec![];
    while let Some((num, line)) = lines.next() {
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
            ["subtest", name] => {
                records.push(Record::Subtest {
                    name: name.to_string(),
                });
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
                let line = num as u32;
                let error = match res {
                    "ok" => false,
                    "error" => true,
                    _ => return Err(ErrorKind::UnexpectedToken(res.into()).at(num)),
                };
                let mut sql = lines
                    .next()
                    .ok_or_else(|| ErrorKind::UnexpectedEOF.at(num + 1))?
                    .1
                    .into();
                for (_, line) in &mut lines {
                    if line.is_empty() {
                        break;
                    }
                    sql += line;
                }
                records.push(Record::Statement {
                    line,
                    conditions: std::mem::take(&mut conditions),
                    error,
                    sql,
                });
            }
            ["query", type_string, res @ ..] => {
                let line = num as u32;
                let sort_mode = match res.get(0) {
                    None | Some(&"nosort") => SortMode::NoSort,
                    Some(&"rowsort") => SortMode::RowSort,
                    Some(&"valuesort") => SortMode::ValueSort,
                    Some(mode) => return Err(ErrorKind::InvalidSortMode(mode.to_string()).at(num)),
                };
                let label = res.get(1).map(|s| s.to_string());
                // The SQL for the query is found on second an subsequent lines of the record
                // up to first line of the form "----" or until the end of the record.
                let mut sql = lines
                    .next()
                    .ok_or_else(|| ErrorKind::UnexpectedEOF.at(num + 1))?
                    .1
                    .into();
                let mut has_result = false;
                for (_, line) in &mut lines {
                    if line.is_empty() || line == "----" {
                        has_result = line == "----";
                        break;
                    }
                    sql += line;
                }
                // Lines following the "----" are expected results of the query, one value per line.
                let mut expected_results = vec![];
                if has_result {
                    for (_, line) in &mut lines {
                        if line.is_empty() {
                            break;
                        }
                        let normalized_line = line.split_ascii_whitespace().join(" ");
                        expected_results.push(normalized_line);
                    }
                }
                records.push(Record::Query {
                    line,
                    conditions: std::mem::take(&mut conditions),
                    type_string: type_string.to_string(),
                    sort_mode,
                    label,
                    sql,
                    expected_results,
                });
            }
            _ => return Err(ErrorKind::InvalidLine(line.into()).at(num)),
        }
    }
    Ok(records)
}

struct SqlLogicTester {
    db: Database,
    testdir: TempDir,
}

impl SqlLogicTester {
    pub fn new(db: Database) -> Self {
        SqlLogicTester {
            db,
            testdir: tempdir().unwrap(),
        }
    }

    pub async fn test(&mut self, record: Record) {
        info!("test: {:?}", record);
        match record {
            Record::Statement {
                error, sql, line, ..
            } => {
                let sql = self.replace_keywords(sql);
                let ret = self.db.run(&sql).await;
                match ret {
                    Ok(_) if error => panic!(
                        "line {}: statement is expected to fail, but actually succeed: {:?}",
                        line, sql
                    ),
                    Err(e) if !error => {
                        panic!("line {}: statement failed: {}\n\tSQL: {:?}", line, e, sql)
                    }
                    _ => {}
                }
            }
            Record::Query {
                line,
                sql,
                mut expected_results,
                sort_mode,
                type_string,
                ..
            } => {
                let sql = self.replace_keywords(sql);
                let chunks = self.db.run(&sql).await.expect("query failed");
                // check types
                for chunk in &chunks {
                    if chunk.arrays().len() != type_string.len() {
                        panic!(
                            "line {}: column length mismatch: expected: {}, actual: {}",
                            line,
                            type_string.len(),
                            chunk.arrays().len()
                        );
                    }
                    for (array, type_char) in chunk.arrays().iter().zip(type_string.chars()) {
                        match (type_char, array) {
                            ('B', ArrayImpl::Bool(_)) => {}
                            ('T', ArrayImpl::UTF8(_)) => {}
                            ('I', ArrayImpl::Int32(_) | ArrayImpl::Int64(_)) => {}
                            ('R', ArrayImpl::Float64(_)) => {}
                            _ => panic!(
                                "line {}: column type mismatch: expected: '{}'",
                                line, type_char
                            ),
                        }
                    }
                }
                let mut output = chunks
                    .iter()
                    .map(datachunk_to_strings)
                    .flatten()
                    .collect_vec();
                match sort_mode {
                    SortMode::NoSort => {}
                    SortMode::RowSort => {
                        output.sort();
                        expected_results.sort();
                    }
                    SortMode::ValueSort => todo!(),
                };
                if output != expected_results {
                    panic!(
                        "line {}: query result mismatch:\nSQL:\n{}\n\nExpected:\n{}\nActual:\n{}",
                        line,
                        sql,
                        expected_results.join("\n"),
                        output.join("\n")
                    );
                }
            }
            Record::Halt => {}
            Record::Subtest { .. } => {}
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

    fn replace_keywords(&self, sql: String) -> String {
        sql.replace("__TEST_DIR__", self.testdir.path().to_str().unwrap())
    }
}

fn datachunk_to_strings(chunk: &DataChunk) -> Vec<String> {
    let mut lines = vec![];
    for row in 0..chunk.cardinality() {
        let mut line = String::new();
        for (col, array) in chunk.arrays().iter().enumerate() {
            use std::fmt::Write;
            if col != 0 {
                write!(line, " ").unwrap();
            }
            match array.get(row) {
                DataValue::Null => write!(line, "NULL"),
                DataValue::Bool(v) => write!(line, "{}", v),
                DataValue::Int32(v) => write!(line, "{}", v),
                DataValue::Int64(v) => write!(line, "{}", v),
                DataValue::Float64(v) => write!(line, "{}", v),
                DataValue::String(s) if s.is_empty() => write!(line, "(empty)"),
                DataValue::String(s) => write!(line, "{}", s),
            }
            .unwrap();
        }
        lines.push(line);
    }
    lines
}
