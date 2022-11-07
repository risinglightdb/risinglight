// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::vec::Vec;

use super::*;
use crate::catalog::ColumnRefId;

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
    ///        (scan $1 (list $1.1 $1.2))
    ///        (scan $2 (list $2.1))
    ///     )
    ///     (scan $3 (list $3.1 $3.2))
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
    /// - `bind_table_factor(t)` => `(scan $1 (list $1.1 $1.2 $1.3))`
    /// - `bind_table_factor(select 1)` => `(values (1))`
    fn bind_table_factor(&mut self, table: TableFactor) -> Result {
        match table {
            TableFactor::Table { name, alias, .. } => {
                let table_id = self.bind_table_id(&name)?;
                let cols = self.bind_table_name(&name, false)?;
                let id = self.egraph.add(Node::Scan([table_id, cols]));
                if let Some(alias) = alias {
                    self.add_alias(alias.name, id)?;
                }
                Ok(id)
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let id = self.bind_query(*subquery)?;
                if let Some(alias) = alias {
                    self.add_alias(alias.name, id)?;
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
            _ => todo!("Support more join constraints"),
        }
    }

    /// Returns a list of all columns in the table.
    ///
    /// This function defines the table name so that it can be referred later.
    ///
    /// # Example
    /// - `bind_table_name(t)` => `(list $1.1 $1.2)`
    pub(super) fn bind_table_name(&mut self, name: &ObjectName, with_rowid: bool) -> Result {
        let name = lower_case_name(name);
        let (database_name, schema_name, table_name) = split_name(&name)?;
        if self.current_ctx().tables.contains_key(table_name) {
            return Err(BindError::DuplicatedTable(table_name.into()));
        }
        let ref_id = self
            .catalog
            .get_table_id_by_name(database_name, schema_name, table_name)
            .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;
        self.current_ctx_mut()
            .tables
            .insert(table_name.into(), ref_id);

        let table = self.catalog.get_table(&ref_id).unwrap();
        let mut ids = vec![];
        for cid in if with_rowid {
            table.all_columns_with_rowid()
        } else {
            table.all_columns()
        }
        .keys()
        {
            let column_ref_id = ColumnRefId::from_table(ref_id, *cid);
            ids.push(self.egraph.add(Node::Column(column_ref_id)));
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
        let (database_name, schema_name, table_name) = split_name(&name)?;

        let table_ref_id = self
            .catalog
            .get_table_id_by_name(database_name, schema_name, table_name)
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
                let column_ref_id = ColumnRefId::from_table(table_ref_id, id);
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
    pub(super) fn bind_table_id(&mut self, table_name: &ObjectName) -> Result {
        let name = lower_case_name(table_name);
        let (database_name, schema_name, table_name) = split_name(&name)?;

        let table_ref_id = self
            .catalog
            .get_table_id_by_name(database_name, schema_name, table_name)
            .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;
        let id = self.egraph.add(Node::Table(table_ref_id));
        Ok(id)
    }
}
