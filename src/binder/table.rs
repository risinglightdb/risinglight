// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::vec::Vec;

use super::*;
use crate::catalog::{ColumnRefId, INTERNAL_SCHEMA_NAME};

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
    /// - `bind_table_factor(t)` => `(scan $1 (list $1.1 $1.2 $1.3) null)`
    /// - `bind_table_factor(select 1)` => `(values (1))`
    fn bind_table_factor(&mut self, table: TableFactor) -> Result {
        match table {
            TableFactor::Table { name, alias, .. } => {
                let (table_id, is_internal) = self.bind_table_id(&name)?;
                let cols = self.bind_table_def(&name, alias, false)?;
                let id = if is_internal {
                    self.egraph.add(Node::Internal([table_id, cols]))
                } else {
                    let null = self.egraph.add(Node::null());
                    self.egraph.add(Node::Scan([table_id, cols, null]))
                };
                Ok(id)
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let (id, ctx) = self.bind_query(*subquery)?;
                // move `output_aliases` to current context
                let table_name = alias.map_or("".into(), |alias| alias.name.value);
                for (name, mut id) in ctx.output_aliases {
                    // wrap with `Ref` if the node is not a column unit.
                    if !matches!(self.node(id), Node::Column(_) | Node::Ref(_)) {
                        id = self.egraph.add(Node::Ref(id));
                    }
                    self.add_alias(name, table_name.clone(), id);
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
            _ => todo!("Support more join types"),
        }
    }

    fn bind_join_constraint(&mut self, constraint: JoinConstraint) -> Result {
        match constraint {
            JoinConstraint::On(expr) => self.bind_expr(expr),
            JoinConstraint::None => Ok(self.egraph.add(Node::true_())),
            _ => todo!("Support more join constraints"),
        }
    }

    /// Returns a list of all columns in the table.
    ///
    /// This function defines the table name so that it can be referred later.
    ///
    /// # Example
    /// - `bind_table_def(t)` => `(list $1.1 $1.2)`
    pub(super) fn bind_table_def(
        &mut self,
        name: &ObjectName,
        alias: Option<TableAlias>,
        with_rowid: bool,
    ) -> Result {
        let name = lower_case_name(name);
        let (schema_name, table_name) = split_name(&name)?;
        let ref_id = self
            .catalog
            .get_table_id_by_name(schema_name, table_name)
            .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;

        let table_alias = match &alias {
            Some(alias) => &alias.name.value,
            None => table_name,
        };
        if !self
            .current_ctx_mut()
            .table_aliases
            .insert(table_alias.into())
        {
            return Err(BindError::DuplicatedAlias(table_alias.into()));
        }

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
        let id = self.egraph.add(Node::List(ids.into()));
        Ok(id)
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
            for col in columns.iter() {
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
            .map(|id| {
                let column_ref_id = ColumnRefId::from_table(table_ref_id, 0, id);
                self.egraph.add(Node::Column(column_ref_id))
            })
            .collect();
        let id = self.egraph.add(Node::List(ids));
        Ok(id)
    }

    /// Returns a [`Table`](Node::Table) node.
    ///
    /// # Example
    /// - `bind_table_id(t)` => `$1`
    pub(super) fn bind_table_id(&mut self, table_name: &ObjectName) -> Result<(Id, bool)> {
        let name = lower_case_name(table_name);
        let (schema_name, table_name) = split_name(&name)?;

        let table_ref_id = self
            .catalog
            .get_table_id_by_name(schema_name, table_name)
            .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;
        let id = self.egraph.add(Node::Table(table_ref_id));
        Ok((id, schema_name == INTERNAL_SCHEMA_NAME))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog::{ColumnCatalog, RootCatalog};
    use crate::parser::parse;

    #[test]
    fn bind_test_subquery() {
        let catalog = Arc::new(RootCatalog::new());
        let col_desc = DataTypeKind::Int32.not_null().to_column("a".into());
        let col_catalog = ColumnCatalog::new(0, col_desc);
        catalog
            .add_table(0, "t".into(), vec![col_catalog], false, vec![])
            .unwrap();

        let stmts = parse("select x.b from (select a as b from t) as x").unwrap();
        let mut binder = Binder::new(catalog);
        for stmt in stmts {
            let plan = binder.bind(stmt).unwrap();
            println!("{}", plan.pretty(10));
        }
    }
}
