use risinglight::Database;

#[test]
fn simple_select1() {
    let db = Database::new();
    db.run("create table t(v1 int, v2 int, v3 int)").unwrap();
    db.run("insert into t values (1,10,100)").unwrap();
    db.run("select v1, v2 from t").unwrap();
}
