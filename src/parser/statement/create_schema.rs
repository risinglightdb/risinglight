use super::*;
use crate::types::DatabaseId;
use postgres_parser as pg;
use std::convert::TryFrom;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct CreateSchemaStmt {
    /// The database name of the entry.
    pub database_name: Option<String>,
    /// The schema name of the entry.
    pub schema_name: String,
    // Binder will fill the following values
    pub database_id: Option<DatabaseId>,
}

impl TryFrom<&pg::Node> for CreateSchemaStmt {
    type Error = ParseError;

    fn try_from(node: &pg::Node) -> Result<Self, Self::Error> {
        let stmt = try_match!(node, pg::Node::CreateSchemaStmt(s) => s, "create");
        let schema_name = try_match!(stmt.schemaname, Some(s) => s.to_lowercase(), "schema name");
        Ok(CreateSchemaStmt {
            database_name: None,
            schema_name,
            database_id: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_schema() {
        let sql = "create schema myschema;";
        let nodes = pg::parse_query(sql).unwrap();
        let stmt = CreateSchemaStmt::try_from(&nodes[0]).unwrap();
        assert_eq!(
            stmt,
            CreateSchemaStmt {
                database_name: None,
                schema_name: "myschema".into(),
                database_id: None,
            }
        );
    }
}
