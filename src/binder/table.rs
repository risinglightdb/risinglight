// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::vec::Vec;

use super::*;
use crate::catalog::{ColumnRefId, RootCatalog};

impl Binder {
    /// Binds the FROM clause. Returns a nested [`Join`](Node::Join) plan of tables.
    ///
    /// If there is no FROM clause, returns `(values (0))`.
    pub(super) fn bind_from(&mut self, tables: Vec<TableWithJoins>) -> Result {
        let mut node = None;
        for table in tables {
            let table_node = self.bind_table_with_joins(table)?;
            node = Some(if let Some(node) = node {
                let ty = self.egraph.add(Node::Inner);
                let expr = self.egraph.add(Node::true_());
                self.egraph.add(Node::Join([ty, expr, node, table_node]))
            } else {
                table_node
            });
        }
        if let Some(node) = node {
            Ok(node)
        } else {
            let zero = self.egraph.add(Node::zero());
            let row = self.egraph.add(Node::List([zero].into()));
            Ok(self.egraph.add(Node::Values([row].into())))
        }
    }

    /// Returns a nested [`Join`](Node::Join) plan of tables.
    ///
    /// # Example
    /// ```ignore
    /// (join inner true
    ///     (join inner (= $1.1 $2.1)
    ///        (scan $1 (list $1.1 $1.2) null)
    ///        (scan $2 (list $2.1) null)
    ///     )
    ///     (scan $3 (list $3.1 $3.2) null)
    /// )
    /// ```
    fn bind_table_with_joins(&mut self, tables: TableWithJoins) -> Result {
        let mut node = self.bind_table_factor(tables.relation)?;
        for join in tables.joins {
            let table = self.bind_table_factor(join.relation)?;
            let (ty, condition) = self.bind_join_op(join.join_operator)?;
            node = self.egraph.add(Node::Join([ty, condition, node, table]));
        }
        Ok(node)
    }

    /// Returns a `Scan` plan of table or a plan of subquery.
    ///
    /// # Example
    /// - `bind_table_factor(t)` => `(scan $1 (list $1.1 $1.2 $1.3) true)`
    /// - `bind_table_factor(select 1)` => `(values (1))`
    fn bind_table_factor(&mut self, table: TableFactor) -> Result {
        match table {
            TableFactor::Table { name, alias, .. } => self.bind_table_def(&name, alias, false),
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let (id, ctx) = self.bind_query(*subquery)?;
                if let Some(alias) = &alias
                    && !alias.columns.is_empty()
                {
                    // 'as t(a, b, ..)'
                    let table_name = &alias.name.value;
                    for (column, id) in alias.columns.iter().zip(self.schema(id)) {
                        self.add_alias(column.name.value.to_lowercase(), table_name.clone(), id);
                    }
                } else {
                    // move `output_aliases` to current context
                    let table_name = alias.map_or("".into(), |alias| alias.name.value);
                    for (name, mut id) in ctx.output_aliases {
                        id = self.wrap_ref(id);
                        self.add_alias(name, table_name.clone(), id);
                    }
                }
                Ok(id)
            }
            _ => panic!("bind table ref"),
        }
    }

    fn bind_join_op(&mut self, op: JoinOperator) -> Result<(Id, Id)> {
        use JoinOperator::*;
        match op {
            Inner(constraint) => {
                let ty = self.egraph.add(Node::Inner);
                let condition = self.bind_join_constraint(constraint)?;
                Ok((ty, condition))
            }
            LeftOuter(constraint) => {
                let ty = self.egraph.add(Node::LeftOuter);
                let condition = self.bind_join_constraint(constraint)?;
                Ok((ty, condition))
            }
            RightOuter(constraint) => {
                let ty = self.egraph.add(Node::RightOuter);
                let condition = self.bind_join_constraint(constraint)?;
                Ok((ty, condition))
            }
            FullOuter(constraint) => {
                let ty = self.egraph.add(Node::FullOuter);
                let condition = self.bind_join_constraint(constraint)?;
                Ok((ty, condition))
            }
            CrossJoin => {
                let ty = self.egraph.add(Node::Inner);
                let condition = self.egraph.add(Node::true_());
                Ok((ty, condition))
            }
            LeftSemi(constraint) => {
                let ty = self.egraph.add(Node::Semi);
                let condition = self.bind_join_constraint(constraint)?;
                Ok((ty, condition))
            }
            LeftAnti(constraint) => {
                let ty = self.egraph.add(Node::Anti);
                let condition = self.bind_join_constraint(constraint)?;
                Ok((ty, condition))
            }
            op => todo!("unsupported join operator: {op:?}"),
        }
    }

    fn bind_join_constraint(&mut self, constraint: JoinConstraint) -> Result {
        match constraint {
            JoinConstraint::On(expr) => self.bind_expr(expr),
            JoinConstraint::None => Ok(self.egraph.add(Node::true_())),
            _ => todo!("Support more join constraints"),
        }
    }

    /// Defines the table name so that it can be referred later.
    /// Returns a `Scan` node if the table is a base table, or a subquery if it is a CTE.
    ///
    /// This function defines the table name so that it can be referred later.
    ///
    /// # Example
    /// - `bind_table_def(t)` => `(scan $1 (list $1.1 $1.2) true)`
    pub(super) fn bind_table_def(
        &mut self,
        name: &ObjectName,
        alias: Option<TableAlias>,
        with_rowid: bool,
    ) -> Result {
        let name = lower_case_name(name);
        let (schema_name, table_name) = split_name(&name)?;

        // check duplicated alias
        let table_alias = match &alias {
            Some(alias) => &alias.name.value,
            None => table_name,
        };
        self.add_table_alias(table_alias)?;

        // find cte
        if let Some((query, columns)) = self.find_cte(table_name).cloned() {
            // add column aliases
            for (column_name, id) in columns {
                self.add_alias(column_name, table_alias.into(), id);
            }
            return Ok(query);
        }

        // find table in catalog
        let ref_id = self
            .catalog
            .get_table_id_by_name(schema_name, table_name)
            .ok_or_else(|| ErrorKind::InvalidTable(table_name.into()))?;

        let table = self.catalog.get_table(&ref_id).unwrap();
        let table_occurence = {
            let count = self.table_occurrences.entry(ref_id).or_default();
            std::mem::replace(count, *count + 1)
        };
        let mut ids = vec![];
        for (cid, column) in if with_rowid {
            table.all_columns_with_rowid()
        } else {
            table.all_columns()
        } {
            let column_ref_id = ColumnRefId::from_table(ref_id, table_occurence, cid);
            let id = self.egraph.add(Node::Column(column_ref_id));
            // TODO: handle column aliases
            self.add_alias(column.name().into(), table_alias.into(), id);
            ids.push(id);
        }

        // return a Scan node
        let table = self.egraph.add(Node::Table(ref_id));
        let cols = self.egraph.add(Node::List(ids.into()));
        let true_ = self.egraph.add(Node::true_());
        let scan = self.egraph.add(Node::Scan([table, cols, true_]));
        Ok(scan)
    }

    /// Returns a list of given columns in the table.
    ///
    /// If `columns` is empty, returns all columns in the table.
    /// If `table_name` is undefined or any column name is not exist, returns an error.
    /// (note: )
    ///
    /// # Example
    /// - `bind_table_columns(t, [c, a])` => `(list $1.3 $1.1)`
    /// - `bind_table_columns(t, [])` => `(list $1.1 $1.2 $1.3)`
    pub(super) fn bind_table_columns(
        &mut self,
        table_name: &ObjectName,
        columns: &[Ident],
    ) -> Result {
        let name = lower_case_name(table_name);
        let (schema_name, table_name) = split_name(&name)?;

        let table_ref_id = self
            .catalog
            .get_table_id_by_name(schema_name, table_name)
            .ok_or_else(|| ErrorKind::InvalidTable(table_name.into()).with_spanned(&name))?;

        let table = self.catalog.get_table(&table_ref_id).unwrap();

        let column_ids = if columns.is_empty() {
            table.all_columns().keys().cloned().collect_vec()
        } else {
            let mut ids = vec![];
            for col in columns {
                let col_name = col.value.to_lowercase();
                let col = table.get_column_by_name(&col_name).ok_or_else(|| {
                    ErrorKind::InvalidColumn(col_name.clone()).with_span(col.span)
                })?;
                ids.push(col.id());
            }
            ids
        };
        let ids = column_ids
            .into_iter()
            .map(|id| {
                let column_ref_id = ColumnRefId::from_table(table_ref_id, 0, id);
                self.egraph.add(Node::Column(column_ref_id))
            })
            .collect();
        let id = self.egraph.add(Node::List(ids));
        Ok(id)
    }

    /// Returns a [`Table`](Node::Table) node, `is_system` flag, and `is_view` flag.
    ///
    /// # Example
    /// - `bind_table_id(t)` => `$1`
    pub(super) fn bind_table_id(&mut self, table_name: &ObjectName) -> Result<(Id, bool, bool)> {
        let name = lower_case_name(table_name);
        let (schema_name, table_name) = split_name(&name)?;

        let table_ref_id = self
            .catalog
            .get_table_id_by_name(schema_name, table_name)
            .ok_or_else(|| ErrorKind::InvalidTable(table_name.into()).with_spanned(&name))?;
        let table = self.catalog.get_table(&table_ref_id).unwrap();
        let id = self.egraph.add(Node::Table(table_ref_id));
        Ok((
            id,
            table_ref_id.schema_id == RootCatalog::SYSTEM_SCHEMA_ID,
            table.is_view(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, RootCatalog};
    use crate::parser::parse;
    use crate::types::DataType;

    #[test]
    fn bind_test_subquery() {
        let catalog = Arc::new(RootCatalog::new());
        let col_catalog = ColumnCatalog::new(0, ColumnDesc::new("a", DataType::Int32, false));
        catalog
            .add_table(1, "t".into(), vec![col_catalog], vec![])
            .unwrap();

        let stmts = parse("select x.b from (select a as b from t) as x").unwrap();
        let mut binder = Binder::new(catalog);
        for stmt in stmts {
            let plan = binder.bind(stmt).unwrap();
            println!("{}", plan.pretty(10));
        }
    }
}
