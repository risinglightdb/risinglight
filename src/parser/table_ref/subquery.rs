use super::*;
use crate::parser::SelectStmt;
use postgres_parser as pg;
use std::convert::TryFrom;

#[derive(Debug, PartialEq)]
pub struct SubqueryRef {
    pub subquery: Box<SelectStmt>,
    /// Column alias provided by the user. This variable is set at the
    /// transformation time. Please note that the number of elements in
    /// `column_alias` do not need to be the same as that returned by the
    /// subquery.
    pub column_alias: Vec<String>,
    pub alias: Option<String>,
}

impl TryFrom<&pg::nodes::RangeSubselect> for SubqueryRef {
    type Error = ParseError;

    fn try_from(root: &pg::nodes::RangeSubselect) -> Result<Self, Self::Error> {
        let subquery = Box::new(SelectStmt::try_from(
            root.subquery.as_ref().unwrap().as_ref(),
        )?);
        let mut alias = None;
        let mut column_alias = vec![];
        if let Some(a) = &root.alias {
            fn node_to_string(node: &pg::Node) -> String {
                match node {
                    pg::Node::Value(v) => v.string.as_ref().unwrap().to_lowercase(),
                    _ => panic!("invalid type"),
                }
            }
            alias = Some(a.aliasname.as_ref().unwrap().to_lowercase());
            column_alias = a.colnames.iter().flatten().map(node_to_string).collect();
        }
        Ok(SubqueryRef {
            subquery,
            column_alias,
            alias,
        })
    }
}
