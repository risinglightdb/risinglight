//! [Sqllogictest][Sqllogictest] parser and runner.
//!
//! [Sqllogictest]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

use itertools::Itertools;
use log::*;
use risinglight::types::DataValue;
use risinglight::{array::*, storage::SecondaryStorageOptions, Database};
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
// #[test_case("where.slt")]
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
    testdir: TempDir,
}

fn consolidate_datachunk(chunks: Vec<DataChunk>) -> DataChunk {
    let mut builders = chunks[0]
        .arrays()
        .iter()
        .map(ArrayBuilderImpl::from_type_of_array)
        .collect_vec();
    for chunk in chunks {
        for (a, b) in chunk.arrays().iter().zip(builders.iter_mut()) {
            b.append(a);
        }
    }
    builders.into_iter().map(|b| b.finish()).collect()
}

#[derive(PartialEq, Eq)]
struct CmpDataValue(DataValue);

impl PartialOrd for CmpDataValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for CmpDataValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

fn sort_datachunk(chunk: DataChunk) -> DataChunk {
    let mut rows = vec![];
    for idx in 0..chunk.cardinality() {
        rows.push(
            chunk
                .get_row_by_idx(idx)
                .into_iter()
                .map(CmpDataValue)
                .collect_vec(),
        );
    }
    rows.sort();
    let mut builders = chunk
        .arrays()
        .iter()
        .map(ArrayBuilderImpl::from_type_of_array)
        .collect_vec();
    for row in rows {
        for (d, b) in row.iter().zip(builders.iter_mut()) {
            b.push(&d.0);
        }
    }
    builders.into_iter().map(|b| b.finish()).collect()
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
            Record::Statement { error, sql, .. } => {
                let sql = self.replace_keywords(sql);
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
                sort_mode,
                ..
            } => {
                let sql = self.replace_keywords(sql);
                let output = self.db.run(&sql).await.expect("query failed");
                let expected: DataChunk =
                    expected_results.into_iter().map(ArrayImpl::from).collect();
                if output.is_empty() && expected.cardinality() == 0 {
                    return;
                }
                let (output, expected) = match sort_mode {
                    SortMode::NoSort => (consolidate_datachunk(output), expected),
                    SortMode::RowSort => (
                        sort_datachunk(consolidate_datachunk(output)),
                        sort_datachunk(expected),
                    ),
                    SortMode::ValueSort => todo!(),
                };
                if output != expected {
                    panic!(
                        "query result mismatch:\nSQL:\n{}\n\nExpected:\n{}\nActual:\n{}",
                        sql, expected, output
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

    fn replace_keywords(&self, sql: String) -> String {
        sql.replace("__TEST_DIR__", self.testdir.path().to_str().unwrap())
    }
}
