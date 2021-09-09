use super::*;
use postgres_parser as pg;
use std::convert::TryFrom;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct CreateDatabaseStmt {
    /// The database name of the entry.
    pub database_name: String,
}

impl TryFrom<&pg::Node> for CreateDatabaseStmt {
    type Error = ParseError;

    fn try_from(node: &pg::Node) -> Result<Self, Self::Error> {
        let stmt = try_match!(node, pg::Node::CreatedbStmt(s) => s, "create");
        let database_name = try_match!(stmt.dbname, Some(s) => s.to_lowercase(), "database name");
        Ok(CreateDatabaseStmt { database_name })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_database() {
        let sql = "create database mydatabase;";
        let nodes = pg::parse_query(sql).unwrap();
        let stmt = CreateDatabaseStmt::try_from(&nodes[0]).unwrap();
        assert_eq!(
            stmt,
            CreateDatabaseStmt {
                database_name: "mydatabase".into(),
            }
        );
    }
}
