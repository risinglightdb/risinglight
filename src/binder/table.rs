// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::vec::Vec;

use super::*;
use crate::catalog::RootCatalog;

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
    /// ```text
    /// (join inner true
    ///     (join inner (= $1.1 $2.1)
    ///        (scan $1 (list $1.1 $1.2) null)
    ///        (scan $2 (list $2.1) null)
    ///     )
    ///     (scan $3 (list $3.1 $3.2) null)
    /// )
    /// ```
    fn bind_table_with_joins(&mut self, tables: TableWithJoins) -> Result {
        let mut node = self.bind_table_factor(tables.relation, false)?;
        for join in tables.joins {
            let table = self.bind_table_factor(join.relation, false)?;
            let (ty, join_or_apply) = self.bind_join_op(join.join_operator)?;
            node = match join_or_apply {
                JoinOrApply::Join(condition) => {
                    self.egraph.add(Node::Join([ty, condition, node, table]))
                }
                JoinOrApply::Apply => self.egraph.add(Node::Apply([ty, node, table])),
            };
        }
        Ok(node)
    }

    /// Returns a `Scan` plan of table or a plan of subquery.
    ///
    /// # Example
    /// - `bind_table_factor(t)` => `(scan $1 (list $1.1 $1.2 $1.3) true)`
    /// - `bind_table_factor(select 1)` => `(values (1))`
    pub(super) fn bind_table_factor(&mut self, table: TableFactor, with_rowid: bool) -> Result {
        let (id, column_aliases, table_alias) = match table {
            TableFactor::Table { name, alias, .. } => {
                let name = lower_case_name(&name);
                let (_, table_name) = split_name(&name)?;

                // check duplicated alias
                let table_alias = match &alias {
                    Some(alias) => &alias.name.value, // t as [t1]
                    None => table_name,               // [t]
                };
                self.add_table_alias(table_alias)?;

                let (id, column_aliases) = self.bind_table_def(&name, with_rowid)?;
                (id, column_aliases, table_alias.to_string())
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let (id, ctx) = self.bind_query(*subquery)?;
                let table_alias = match &alias {
                    Some(alias) => {
                        self.add_table_alias(&alias.name.value)?;
                        &alias.name.value
                    }
                    None => "",
                };
                let column_aliases = if let Some(alias) = &alias
                    && !alias.columns.is_empty()
                {
                    // 'as t(a, b, ..)'
                    alias
                        .columns
                        .iter()
                        .map(|c| Some(c.value.to_lowercase()))
                        .collect()
                } else {
                    ctx.output_aliases
                };
                (id, column_aliases, table_alias.to_string())
            }
            _ => return Err(BindError::Todo("bind table factor".into())),
        };
        // resolve column conflicts
        let id = self.add_proj_if_conflict(id);
        for (alias, mut id) in column_aliases.into_iter().zip(self.schema(id)) {
            if let Some(alias) = alias {
                id = self.wrap_ref(id);
                self.add_alias(alias, table_alias.clone(), id);
            }
        }
        Ok(id)
    }

    fn bind_join_op(&mut self, op: JoinOperator) -> Result<(Id, JoinOrApply)> {
        use JoinOperator::*;
        match op {
            Inner(constraint) => {
                let ty = self.egraph.add(Node::Inner);
                let condition = self.bind_join_constraint(constraint)?;
                Ok((ty, JoinOrApply::Join(condition)))
            }
            LeftOuter(constraint) => {
                let ty = self.egraph.add(Node::LeftOuter);
                let condition = self.bind_join_constraint(constraint)?;
                Ok((ty, JoinOrApply::Join(condition)))
            }
            RightOuter(constraint) => {
                let ty = self.egraph.add(Node::RightOuter);
                let condition = self.bind_join_constraint(constraint)?;
                Ok((ty, JoinOrApply::Join(condition)))
            }
            FullOuter(constraint) => {
                let ty = self.egraph.add(Node::FullOuter);
                let condition = self.bind_join_constraint(constraint)?;
                Ok((ty, JoinOrApply::Join(condition)))
            }
            CrossJoin => {
                let ty = self.egraph.add(Node::Inner);
                let condition = self.egraph.add(Node::true_());
                Ok((ty, JoinOrApply::Join(condition)))
            }
            LeftSemi(constraint) => {
                let ty = self.egraph.add(Node::Semi);
                let condition = self.bind_join_constraint(constraint)?;
                Ok((ty, JoinOrApply::Join(condition)))
            }
            LeftAnti(constraint) => {
                let ty = self.egraph.add(Node::Anti);
                let condition = self.bind_join_constraint(constraint)?;
                Ok((ty, JoinOrApply::Join(condition)))
            }
            CrossApply => {
                let ty = self.egraph.add(Node::Inner);
                Ok((ty, JoinOrApply::Apply))
            }
            OuterApply => {
                let ty = self.egraph.add(Node::LeftOuter);
                Ok((ty, JoinOrApply::Apply))
            }
            op => {
                return Err(BindError::Todo(format!(
                    "unsupported join operator: {op:?}"
                )))
            }
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
    /// Returns 2 values:
    /// - the plan node: `Scan` if the table is a base table, or a subquery if it is a CTE.
    /// - the column aliases.
    ///
    /// # Example
    /// - `bind_table_def(t)` => `(scan $1 (list $1.1 $1.2) true)`
    fn bind_table_def(
        &mut self,
        name: &ObjectName,
        with_rowid: bool,
    ) -> Result<(Id, Vec<Option<String>>)> {
        let name = lower_case_name(name);
        let (schema_name, table_name) = split_name(&name)?;

        // find cte
        if let Some((query, aliases)) = self.find_cte(table_name).cloned() {
            return Ok((query, aliases));
        }

        // find table in catalog
        let table_ref_id = self
            .catalog
            .get_table_id_by_name(schema_name, table_name)
            .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;

        let table = self.catalog.get_table(&table_ref_id).unwrap();
        let mut column_ids = vec![];
        let mut aliases = vec![];
        for (cid, column) in if with_rowid {
            table.all_columns_with_rowid()
        } else {
            table.all_columns()
        } {
            let id = self.egraph.add(Node::Column(table_ref_id.with_column(cid)));
            column_ids.push(id);
            // TODO: handle column aliases
            aliases.push(Some(column.name().to_owned()));
        }

        // return a Scan node
        let table = self.egraph.add(Node::Table(table_ref_id));
        let cols = self.egraph.add(Node::List(column_ids.into()));
        let true_ = self.egraph.add(Node::true_());
        let scan = self.egraph.add(Node::Scan([table, cols, true_]));
        Ok((scan, aliases))
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
            .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;

        let table = self.catalog.get_table(&table_ref_id).unwrap();

        let column_ids = if columns.is_empty() {
            table.all_columns().keys().cloned().collect_vec()
        } else {
            let mut ids = vec![];
            for col in columns {
                let col_name = col.value.to_lowercase();
                let col = table
                    .get_column_by_name(&col_name)
                    .ok_or_else(|| BindError::InvalidColumn(col_name.clone()))?;
                ids.push(col.id());
            }
            ids
        };
        let ids = column_ids
            .into_iter()
            .map(|cid| self.egraph.add(Node::Column(table_ref_id.with_column(cid))))
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
            .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;
        let table = self.catalog.get_table(&table_ref_id).unwrap();
        let id = self.egraph.add(Node::Table(table_ref_id));
        Ok((
            id,
            table_ref_id.schema_id == RootCatalog::SYSTEM_SCHEMA_ID,
            table.is_view(),
        ))
    }

    /// If any column from `table` has conflicted with existing columns,
    /// add a [`Proj`](Node::Proj) node to the table to distinguish columns.
    ///
    /// # Example
    /// ```text
    /// column_aliases_ids: [$1.1]
    /// table:  (scan $1 (list $1.1 $1.2) true)   # $1.1 is conflicted with existing columns
    /// return: (proj (list (' $1.1) $1.2)        # wrap it with '
    ///             (scan $1 (list $1.1 $1.2) true))
    /// ```
    pub(super) fn add_proj_if_conflict(&mut self, table: Id) -> Id {
        let mut schema = self.schema(table);
        let mut need_proj = false;
        for id in &mut schema {
            *id = self.wrap_ref(*id);
            while self.context().all_variable_ids.contains(id) {
                *id = self.egraph.add(Node::Prime(*id));
                need_proj = true;
            }
        }
        if need_proj {
            let projs = self.egraph.add(Node::List(schema.into()));
            self.egraph.add(Node::Proj([projs, table]))
        } else {
            table
        }
    }
}

enum JoinOrApply {
    Join(Id), // id of condition
    Apply,
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
