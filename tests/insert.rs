use risinglight::Database;

#[test]
fn simple_insert1() {
    let db = Database::new();
    db.run("create table t(v1 int, v2 int, v3 int)").unwrap();
    db.run("insert into t values (1,10,100)").unwrap();
}

#[test]
fn simple_insert2() {
    let db = Database::new();
    db.run("create table t(v1 int, v2 int, v3 int)").unwrap();
    db.run("insert into t values (1,10,100), (2,20,200), (3,30,300), (4,40,400)")
        .unwrap();
}

#[test]
fn simple_insert3() {
    let db = Database::new();
    db.run("create table t(v1 int, v2 int, v3 int)").unwrap();
    db.run("insert into t(v1, v2, v3) values (1,10,100), (2,20,200), (3,30,300)")
        .unwrap();
}

#[test]
fn simple_insert4() {
    let db = Database::new();
    db.run("create table t(v1 int, v2 int, v3 int)").unwrap();
    db.run("insert into t(v1, v2) values (1,10), (2,20), (3,30), (4,40)")
        .unwrap();
}

#[test]
fn simple_insert5() {
    let db = Database::new();
    db.run("create table t(v1 int, v2 int, v3 int)").unwrap();
    db.run("insert into t(v2, v1) values (1,10), (2,20), (3,30), (4,40)")
        .unwrap();
}

#[test]
fn insert_null() {
    let db = Database::new();
    db.run("create table t(v1 int, v2 int, v3 int)").unwrap();
    db.run("insert into t values (NULL,NULL,NULL)").unwrap();
}
