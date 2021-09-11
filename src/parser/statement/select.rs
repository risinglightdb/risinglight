use super::*;
use crate::parser::{expression::Expression, table_ref::TableRef};
use crate::types::DataType;
use postgres_parser as pg;
use std::convert::TryFrom;

#[derive(Debug, Default, PartialEq)]
pub struct SelectStmt {
    pub select_list: Vec<Expression>,
    // TODO: aggregates: Vec<Expression>,
    pub from_table: Option<TableRef>,
    pub where_clause: Option<Expression>,
    pub select_distinct: bool,
    pub groupby: Option<GroupBy>,
    pub orderby: Option<OrderBy>,
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
    pub return_names: Vec<String>,
    pub return_types: Vec<Option<DataType>>,
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
        let groupby = match &stmt.groupClause {
            Some(list) => Some(GroupBy::parse(list, &stmt.havingClause)?),
            None => None,
        };
        let orderby = match &stmt.sortClause {
            Some(list) => Some(OrderBy::try_from(list.as_slice())?),
            None => None,
        };
        let limit = parse_expr(&stmt.limitCount)?;
        let offset = parse_expr(&stmt.limitOffset)?;

        Ok(SelectStmt {
            select_list,
            from_table,
            where_clause,
            select_distinct,
            groupby,
            orderby,
            limit,
            offset,
            return_names: vec![],
            return_types: vec![],
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

#[derive(Debug, PartialEq)]
pub struct GroupBy {
    pub groups: Vec<Expression>,
    pub having: Option<Expression>,
}

impl GroupBy {
    fn parse(list: &[pg::Node], having: &Option<Box<pg::Node>>) -> Result<GroupBy, ParseError> {
        let groups = list
            .iter()
            .map(Expression::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let having = parse_expr(having)?;
        Ok(GroupBy { groups, having })
    }
}

#[derive(Debug, PartialEq)]
pub struct OrderBy {
    pub list: Vec<(OrderByKind, Expression)>,
}

#[derive(Debug, PartialEq)]
pub enum OrderByKind {
    Ascending,
    Descending,
}

impl From<pg::sys::SortByDir> for OrderByKind {
    fn from(sort: pg::sys::SortByDir) -> Self {
        use pg::sys::SortByDir as Sort;
        match sort {
            Sort::SORTBY_DESC => Self::Descending,
            Sort::SORTBY_ASC | Sort::SORTBY_DEFAULT => Self::Ascending,
            _ => todo!("unsupported order by"),
        }
    }
}

impl TryFrom<&[pg::Node]> for OrderBy {
    type Error = ParseError;

    fn try_from(list: &[pg::Node]) -> Result<Self, Self::Error> {
        let mut ret = vec![];
        ret.reserve(list.len());
        for node in list {
            let sort = try_match!(node, pg::Node::SortBy(s) => s, "sort by");
            let kind = OrderByKind::from(sort.sortby_dir);
            let expr = Expression::try_from(sort.node.as_ref().unwrap().as_ref())?;
            ret.push((kind, expr));
        }
        Ok(OrderBy { list: ret })
    }
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
                    Expression::column_ref("v1".into()),
                    Expression::column_ref2("v2".into(), "t".into()),
                    Expression::star(),
                ],
                from_table: Some(TableRef::base("t".into())),
                ..Default::default()
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
                ..Default::default()
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
                    Expression::column_ref("v1".into()),
                    Expression::column_ref("v2".into()),
                ],
                from_table: Some(TableRef::Subquery(SubqueryRef {
                    subquery: Box::new(SelectStmt {
                        select_list: vec![Expression::column_ref("v1".into())],
                        from_table: Some(TableRef::base("t".into())),
                        ..Default::default()
                    }),
                    alias: Some("foo".into()),
                    column_alias: vec!["a".into(), "b".into()],
                })),
                ..Default::default()
            }
        );
    }

    #[test]
    fn where_clause() {
        assert_eq!(
            parse("select v1, v2 from s where v3 = 1").unwrap(),
            SelectStmt {
                select_list: vec![
                    Expression::column_ref("v1".into()),
                    Expression::column_ref("v2".into()),
                ],
                from_table: Some(TableRef::base("s".into())),
                where_clause: Some(Expression::comparison(
                    CmpKind::Equal,
                    Expression::column_ref("v3".into()),
                    Expression::constant(DataValue::Int32(1)),
                )),
                ..Default::default()
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
                        Expression::column_ref("v1".into())
                    ),
                    Expression::typecast(DataTypeKind::Int32, Expression::column_ref("v2".into())),
                ],
                from_table: Some(TableRef::base("s".into())),
                ..Default::default()
            }
        );
    }

    #[test]
    fn aggregate() {
        assert_eq!(
            parse("select min(v1) from s").unwrap(),
            SelectStmt {
                select_list: vec![Expression::aggregate(
                    AggregateKind::Min,
                    Expression::column_ref("v1".into()),
                )],
                from_table: Some(TableRef::base("s".into())),
                ..Default::default()
            }
        );
        assert_eq!(
            parse("select count(*) from s").unwrap(),
            SelectStmt {
                select_list: vec![Expression::aggregate(
                    AggregateKind::Count,
                    Expression::star(),
                )],
                from_table: Some(TableRef::base("s".into())),
                ..Default::default()
            }
        );
    }

    #[test]
    fn groupby() {
        assert_eq!(
            parse("select v1 from s group by v1").unwrap(),
            SelectStmt {
                select_list: vec![Expression::column_ref("v1".into())],
                from_table: Some(TableRef::base("s".into())),
                groupby: Some(GroupBy {
                    groups: vec![Expression::column_ref("v1".into())],
                    having: None,
                }),
                ..Default::default()
            }
        );
    }

    #[test]
    fn orderby() {
        assert_eq!(
            parse("select v1 from s order by v1").unwrap(),
            SelectStmt {
                select_list: vec![Expression::column_ref("v1".into())],
                from_table: Some(TableRef::base("s".into())),
                orderby: Some(OrderBy {
                    list: vec![(OrderByKind::Ascending, Expression::column_ref("v1".into()))],
                }),
                ..Default::default()
            }
        );
    }

    #[test]
    fn between() {
        assert_eq!(
            parse("select v from t where v between 3 and 10").unwrap(),
            SelectStmt {
                select_list: vec![Expression::column_ref("v".into())],
                from_table: Some(TableRef::base("t".into())),
                where_clause: Some(Expression::and(
                    Expression::comparison(
                        CmpKind::GreaterThanOrEqual,
                        Expression::column_ref("v".into()),
                        Expression::constant(DataValue::Int32(3)),
                    ),
                    Expression::comparison(
                        CmpKind::LessThanOrEqual,
                        Expression::column_ref("v".into()),
                        Expression::constant(DataValue::Int32(10)),
                    ),
                )),
                ..Default::default()
            }
        );
        assert_eq!(
            parse("select v from t where v not between 3 and 10").unwrap(),
            SelectStmt {
                select_list: vec![Expression::column_ref("v".into())],
                from_table: Some(TableRef::base("t".into())),
                where_clause: Some(Expression::not(Expression::and(
                    Expression::comparison(
                        CmpKind::GreaterThanOrEqual,
                        Expression::column_ref("v".into()),
                        Expression::constant(DataValue::Int32(3)),
                    ),
                    Expression::comparison(
                        CmpKind::LessThanOrEqual,
                        Expression::column_ref("v".into()),
                        Expression::constant(DataValue::Int32(10)),
                    ),
                ))),
                ..Default::default()
            }
        );
    }
}
