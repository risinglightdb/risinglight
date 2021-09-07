use super::*;
use crate::catalog::TableRefId;
use crate::parser::{expression::Expression, table_ref::BaseTableRef};
use crate::types::{ColumnId, DataType};
use postgres_parser as pg;
use std::convert::TryFrom;

#[derive(Debug, Default, PartialEq)]
pub struct InsertStmt {
    /// The database name of the entry.
    pub database_name: Option<String>,
    /// The schema name of the entry.
    pub schema_name: Option<String>,
    /// Name of the table we want to insert.
    pub table_name: String,
    /// The name of the columns to insert.
    pub column_names: Vec<String>,
    /// List of values to insert.
    pub values: Vec<Vec<Expression>>,

    /// The following values will be set by binder
    pub column_ids: Vec<ColumnId>,
    pub column_types: Vec<DataType>,
    pub table_ref_id: Option<TableRefId>,
}

impl TryFrom<&pg::Node> for InsertStmt {
    type Error = ParseError;

    fn try_from(node: &pg::Node) -> Result<Self, Self::Error> {
        let stmt = try_match!(node, pg::Node::InsertStmt(s) => s, "insert");
        let column_names = get_columns(stmt)?;
        // TODO: handle select stmt
        let select_stmt = try_match!(**stmt.selectStmt.as_ref().unwrap(), pg::Node::SelectStmt(s) => s, "select stmt");
        let mut values = vec![];
        if let Some(list) = &select_stmt.valuesLists {
            values = get_values_list(list)?;
            if stmt.cols.is_some() && column_names.len() != values[0].len() {
                return Err(ParseError::InvalidInput(
                    "Number of column names does not equal to number of values.",
                ));
            }
        } else {
            todo!("transform select");
        }
        let ref_ = BaseTableRef::from(stmt.relation.as_ref().unwrap().as_ref());
        assert!(ref_.alias.is_none());
        Ok(InsertStmt {
            database_name: ref_.database_name,
            schema_name: ref_.schema_name,
            table_name: ref_.table_name,
            column_names,
            values,
            column_ids: Vec::new(),
            column_types: Vec::new(),
            table_ref_id: None,
        })
    }
}

/// Get column names from statement.
fn get_columns(stmt: &pg::nodes::InsertStmt) -> Result<Vec<String>, ParseError> {
    let mut column_names = vec![];
    if let Some(cols) = &stmt.cols {
        column_names.reserve(cols.len());
    }
    for col in stmt.cols.iter().flatten() {
        let target = try_match!(col, pg::Node::ResTarget(r) => r, "columns");
        let name = target.name.clone().unwrap();
        if column_names.contains(&name) {
            return Err(ParseError::Duplicate("column names"));
        }
        column_names.push(name);
    }
    Ok(column_names)
}

/// Transform the value list.
fn get_values_list(list: &[pg::Node]) -> Result<Vec<Vec<Expression>>, ParseError> {
    let mut values = vec![];
    values.reserve(list.len());
    let mut len: Option<usize> = None;
    for value in list {
        let target = try_match!(value, pg::Node::List(l) => l, "value list");
        // check length
        if let Some(len) = len {
            if target.len() != len {
                return Err(ParseError::InvalidInput("number of values mismatch"));
            }
        } else {
            len = Some(target.len());
        }
        let value = target
            .iter()
            .map(Expression::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        values.push(value);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataValue;

    fn parse(sql: &str) -> Result<InsertStmt, ParseError> {
        let nodes = pg::parse_query(sql).unwrap();
        InsertStmt::try_from(&nodes[0])
    }

    #[test]
    fn parse_insert() {
        assert_eq!(
            parse("insert into t1 (col1, col2) values (1,2), (3,4), (5,6)").unwrap(),
            InsertStmt {
                database_name: None,
                schema_name: None,
                table_name: "t1".into(),
                column_names: vec!["col1".into(), "col2".into()],
                values: vec![
                    vec![
                        Expression::constant(DataValue::Int32(1)),
                        Expression::constant(DataValue::Int32(2)),
                    ],
                    vec![
                        Expression::constant(DataValue::Int32(3)),
                        Expression::constant(DataValue::Int32(4)),
                    ],
                    vec![
                        Expression::constant(DataValue::Int32(5)),
                        Expression::constant(DataValue::Int32(6)),
                    ],
                ],
                column_ids: Vec::new(),
                column_types: Vec::new(),
                table_ref_id: None
            }
        );
    }

    #[test]
    fn parse_insert_length_mismatch() {
        let ret = parse("insert into t1 values (1,2), (3,4,5)");
        assert!(matches!(ret, Err(ParseError::InvalidInput(_))));

        let ret = parse("insert into t1 (col1) values (1,2)");
        assert!(matches!(ret, Err(ParseError::InvalidInput(_))));
    }
}
