use risinglight::{Database, Error};

#[test]
#[ignore] // FIXME
fn create_database() {
    let db = Database::new();
    db.run("create database mydatabase").unwrap();
}

#[test]
#[ignore] // FIXME
fn create_duplicated_database() {
    let db = Database::new();
    db.run("create database mydatabase").unwrap();
    let ret = db.run("create database mydatabase");
    assert!(matches!(ret, Err(Error::Bind(_))));
}

#[test]
#[ignore] // FIXME
fn create_schema() {
    let db = Database::new();
    db.run("create schema myschema").unwrap();
}

#[test]
fn create_table() {
    let db = Database::new();
    db.run("create table t(v1 int, v2 int, v3 int)").unwrap();
}
