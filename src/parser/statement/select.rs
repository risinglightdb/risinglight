use super::*;
use crate::parser::{expression::Expression, table_ref::TableRef};
use crate::types::DataType;
use postgres_parser as pg;
use std::convert::TryFrom;

#[derive(Debug, PartialEq)]
pub struct SelectStmt {
    pub select_list: Vec<Expression>,
    // TODO: aggregates: Vec<Expression>,
    pub from_table: Option<TableRef>,
    pub where_clause: Option<Expression>,
    pub select_distinct: bool,
    pub return_names: Vec<String>,
    pub return_types: Vec<Option<DataType>>,
    // TODO: groupby
    // TODO: orderby
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
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
        let return_types = vec![];
        let return_names = vec![];
        let limit = parse_expr(&stmt.limitCount)?;
        let offset = parse_expr(&stmt.limitOffset)?;

        Ok(SelectStmt {
            select_list,
            from_table,
            where_clause,
            return_names,
            return_types,
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
    use crate::types::{DataTypeKind, DataValue};

    fn parse(sql: &str) -> Result<SelectStmt, ParseError> {
        let nodes = pg::parse_query(sql).unwrap();
        SelectStmt::try_from(&nodes[0])
    }

    #[test]
    fn column_ref() {
        assert_eq!(
            parse("select v1, t.v2, * from t").unwrap(),
            SelectStmt {
                select_list: vec![
                    Expression::column_ref("v1".into(), None),
                    Expression::column_ref("v2".into(), Some("t".into())),
                    Expression::star(),
                ],
                from_table: Some(TableRef::base("t".into())),
                where_clause: None,
                return_types: vec![],
                return_names: vec![],
                select_distinct: false,
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn constant() {
        assert_eq!(
            parse("select 1, 1.1 from t").unwrap(),
            SelectStmt {
                select_list: vec![
                    Expression::constant(DataValue::Int32(1)),
                    Expression::constant(DataValue::Float64(1.1)),
                ],
                from_table: Some(TableRef::base("t".into())),
                where_clause: None,
                return_types: vec![],
                return_names: vec![],
                select_distinct: false,
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn no_from() {
        let stmt = parse("select 1").unwrap();
        assert!(stmt.from_table.is_none());
    }

    #[test]
    fn no_select_list() {
        let stmt = parse("select from t").unwrap();
        assert!(stmt.select_list.is_empty());

        let stmt = parse("select").unwrap();
        assert!(stmt.select_list.is_empty());
    }

    #[test]
    fn from_subquery() {
        // The query should fail at binding time. The transformer is unaware of the error.
        assert_eq!(
            parse("select v1, v2 from (select v1 from t) as foo(a, b)").unwrap(),
            SelectStmt {
                select_list: vec![
                    Expression::column_ref("v1".into(), None),
                    Expression::column_ref("v2".into(), None),
                ],
                from_table: Some(TableRef::Subquery(SubqueryRef {
                    subquery: Box::new(SelectStmt {
                        select_list: vec![Expression::column_ref("v1".into(), None)],
                        from_table: Some(TableRef::base("t".into())),
                        where_clause: None,
                        select_distinct: false,
                        return_names: vec![],
                        return_types: vec![],
                        limit: None,
                        offset: None,
                    }),
                    alias: Some("foo".into()),
                    column_alias: vec!["a".into(), "b".into()],
                })),
                return_names: vec![],
                return_types: vec![],
                where_clause: None,
                select_distinct: false,
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn where_clause() {
        assert_eq!(
            parse("select v1, v2 from s where v3 = 1").unwrap(),
            SelectStmt {
                select_list: vec![
                    Expression::column_ref("v1".into(), None),
                    Expression::column_ref("v2".into(), None),
                ],
                from_table: Some(TableRef::base("s".into())),
                where_clause: Some(Expression::comparison(
                    ComparisonKind::Equal,
                    Expression::column_ref("v3".into(), None),
                    Expression::constant(DataValue::Int32(1)),
                )),
                return_names: vec![],
                return_types: vec![],
                select_distinct: false,
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn type_cast() {
        assert_eq!(
            parse("select v1::DOUBLE, cast(v2 as INTEGER) from s").unwrap(),
            SelectStmt {
                select_list: vec![
                    Expression::typecast(
                        DataTypeKind::Float64,
                        Expression::column_ref("v1".into(), None)
                    ),
                    Expression::typecast(
                        DataTypeKind::Int32,
                        Expression::column_ref("v2".into(), None)
                    ),
                ],
                from_table: Some(TableRef::base("s".into())),
                return_names: vec![],
                return_types: vec![],
                where_clause: None,
                select_distinct: false,
                limit: None,
                offset: None,
            }
        );
    }
}
