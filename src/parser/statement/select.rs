use super::*;
use crate::parser::{expression::Expression, table_ref::TableRef};
use postgres_parser as pg;
use std::convert::TryFrom;

#[derive(Debug, PartialEq)]
pub struct SelectStmt {
    select_list: Vec<Expression>,
    // TODO: aggregates: Vec<Expression>,
    from_table: Option<TableRef>,
    where_clause: Option<Expression>,
    select_distinct: bool,
    // TODO: groupby
    // TODO: orderby
    limit: Option<Expression>,
    offset: Option<Expression>,
}

impl TryFrom<&pg::Node> for SelectStmt {
    type Error = ParseError;

    fn try_from(node: &pg::Node) -> Result<Self, Self::Error> {
        let stmt = try_match!(node, pg::Node::SelectStmt(s) => s, "select");
        assert_eq!(stmt.op, pg::sys::SetOperation::SETOP_NONE, "todo");

        let select_list = match &stmt.targetList {
            Some(list) => get_target_list(list)?,
            None => vec![],
        };
        let from_table = match &stmt.fromClause {
            Some(list) => Some(get_from_list(list)?),
            None => None,
        };
        let where_clause = parse_expr(&stmt.whereClause)?;
        let select_distinct = stmt.distinctClause.is_some();
        let limit = parse_expr(&stmt.limitCount)?;
        let offset = parse_expr(&stmt.limitOffset)?;

        Ok(SelectStmt {
            select_list,
            from_table,
            where_clause,
            select_distinct,
            limit,
            offset,
        })
    }
}

fn parse_expr(node: &Option<Box<pg::Node>>) -> Result<Option<Expression>, ParseError> {
    Ok(match node {
        Some(node) => Some(Expression::try_from(node.as_ref())?),
        None => None,
    })
}

/// Transform target list. Return a list of expressions.
fn get_target_list(list: &[pg::Node]) -> Result<Vec<Expression>, ParseError> {
    let mut targets = vec![];
    targets.reserve(list.len());
    for value in list {
        let target = try_match!(value, pg::Node::ResTarget(t) => t, "target");
        let mut expr = Expression::try_from(target.val.as_ref().unwrap().as_ref())?;
        // Set table alias.
        if let Some(name) = &target.name {
            expr.alias = Some(name.to_lowercase());
        }
        targets.push(expr);
    }
    Ok(targets)
}

/// Transform the FROM clause.
fn get_from_list(list: &[pg::Node]) -> Result<TableRef, ParseError> {
    assert!(list.len() == 1, "TODO: cross product not supported");
    TableRef::try_from(&list[0])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> Result<SelectStmt, ParseError> {
        let nodes = pg::parse_query(sql).unwrap();
        SelectStmt::try_from(&nodes[0])
    }

    #[test]
    fn parse_select() {
        assert_eq!(
            parse("select v1, t1.v2, * from t1").unwrap(),
            SelectStmt {
                select_list: vec![
                    Expression::column_ref("v1".into(), None),
                    Expression::column_ref("v2".into(), Some("t1".into())),
                    Expression::star(),
                ],
                from_table: Some(TableRef::base("t1".into())),
                where_clause: None,
                select_distinct: false,
                limit: None,
                offset: None,
            }
        );
    }
}
