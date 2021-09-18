use super::*;
use crate::parser::{Query, SelectItem, SetExpr};

#[derive(Debug, PartialEq, Clone)]
pub struct BoundSelect {
    pub select_list: Vec<BoundExpr>,
    // TODO: aggregates: Vec<BoundExpr>,
    pub from_table: Vec<BoundTableRef>,
    pub where_clause: Option<BoundExpr>,
    pub select_distinct: bool,
    // pub groupby: Option<BoundGroupBy>,
    // pub orderby: Option<BoundOrderBy>,
    pub limit: Option<BoundExpr>,
    pub offset: Option<BoundExpr>,
    // pub return_names: Vec<String>,
}

impl Binder {
    pub fn bind_select(&mut self, query: &Query) -> Result<Box<BoundSelect>, BindError> {
        self.push_context();
        let ret = self.bind_select_internal(query);
        self.pop_context();
        ret
    }

    fn bind_select_internal(&mut self, query: &Query) -> Result<Box<BoundSelect>, BindError> {
        let select = match &query.body {
            SetExpr::Select(select) => &**select,
            _ => todo!("not select"),
        };
        // Bind table ref
        let mut from_table = vec![];
        for table_ref in select.from.iter() {
            let table_ref = self.bind_table_ref(&table_ref.relation)?;
            from_table.push(table_ref);
        }
        // TODO: process where, order by, group-by
        let where_clause = match &select.selection {
            Some(expr) => Some(self.bind_expr(expr)?),
            None => None,
        };
        let limit = match &query.limit {
            Some(expr) => Some(self.bind_expr(expr)?),
            None => None,
        };
        let offset = match &query.offset {
            Some(offset) => Some(self.bind_expr(&offset.value)?),
            None => None,
        };

        // Bind the select list.
        // we only support column reference now

        let mut select_list = vec![];
        // let mut return_names = vec![];
        for item in select.projection.iter() {
            let expr = match item {
                SelectItem::UnnamedExpr(expr) => self.bind_expr(expr)?,
                SelectItem::ExprWithAlias { expr, .. } => self.bind_expr(expr)?,
                _ => todo!("bind select list"),
            };
            // return_names.push(expr.get_name());
            select_list.push(expr);
        }

        // Add referred columns for base table reference
        for table_ref in from_table.iter_mut() {
            table_ref.column_ids = self
                .context
                .column_ids
                .get(&table_ref.table_name)
                .unwrap()
                .clone();
        }

        Ok(Box::new(BoundSelect {
            select_list,
            from_table,
            where_clause,
            select_distinct: select.distinct,
            limit,
            offset,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnRefId, RootCatalog};
    use crate::parser::{parse, Statement};
    use crate::types::{DataTypeExt, DataTypeKind};
    use std::sync::Arc;

    #[test]
    fn bind_select() {
        let catalog = Arc::new(RootCatalog::new());
        let mut binder = Binder::new(catalog.clone());

        let database = catalog.get_database_by_id(0).unwrap();
        let schema = database.get_schema_by_id(0).unwrap();
        schema
            .add_table(
                "t".into(),
                vec![
                    ColumnCatalog::new(0, "a".into(), DataTypeKind::Int.not_null().to_column()),
                    ColumnCatalog::new(1, "b".into(), DataTypeKind::Int.not_null().to_column()),
                ],
                false,
            )
            .unwrap();

        let sql = "select b, a from t;  select c from t;";
        let stmts = parse(sql).unwrap();

        let query = match &stmts[0] {
            Statement::Query(q) => &*q,
            _ => panic!("type mismatch"),
        };
        assert_eq!(
            *binder.bind_select(query).unwrap(),
            BoundSelect {
                select_list: vec![
                    BoundExpr {
                        kind: BoundExprKind::ColumnRef(BoundColumnRef {
                            column_ref_id: ColumnRefId::new(0, 0, 0, 1),
                            column_index: 0,
                        }),
                        return_type: Some(DataTypeKind::Int.not_null()),
                    },
                    BoundExpr {
                        kind: BoundExprKind::ColumnRef(BoundColumnRef {
                            column_ref_id: ColumnRefId::new(0, 0, 0, 0),
                            column_index: 1,
                        }),
                        return_type: Some(DataTypeKind::Int.not_null()),
                    },
                ],
                from_table: vec![BoundTableRef {
                    ref_id: TableRefId::new(0, 0, 0),
                    table_name: "t".into(),
                    column_ids: vec![1, 0],
                }],
                where_clause: None,
                select_distinct: false,
                limit: None,
                offset: None,
            }
        );

        let query = match &stmts[1] {
            Statement::Query(q) => &*q,
            _ => panic!("type mismatch"),
        };
        assert_eq!(
            binder.bind_select(query),
            Err(BindError::InvalidColumn("c".into()))
        );
    }
}
