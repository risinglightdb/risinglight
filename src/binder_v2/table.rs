// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::vec::Vec;

use super::*;
use crate::catalog::ColumnRefId;

impl Binder {
    pub(super) fn bind_from(&mut self, tables: Vec<TableWithJoins>) -> Result {
        let mut node = None;
        for table in tables {
            let table_node = self.bind_table_with_joins(table)?;
            node = Some(if let Some(node) = node {
                let ty = self.egraph.add(Node::Cross);
                let expr = self.egraph.add(Node::true_());
                self.egraph.add(Node::Join([ty, expr, node, table_node]))
            } else {
                table_node
            });
        }
        Ok(node.expect("no table"))
    }

    fn bind_table_with_joins(&mut self, tables: TableWithJoins) -> Result {
        let mut node = self.bind_table(tables.relation)?;
        for join in tables.joins {
            let table = self.bind_table(join.relation)?;
            let (ty, condition) = self.bind_join_op(join.join_operator)?;
            node = self.egraph.add(Node::Join([ty, condition, node, table]));
        }
        Ok(node)
    }

    pub(super) fn bind_table(&mut self, table: TableFactor) -> Result {
        match table {
            TableFactor::Table { name, alias, .. } => {
                let name = lower_case_name(name);
                let (database_name, schema_name, table_name) = split_name(&name)?;
                let id = self.bind_table_name(database_name, schema_name, table_name)?;
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
                let ty = self.egraph.add(Node::Cross);
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

    fn bind_table_name(
        &mut self,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result {
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
        for cid in table.all_columns().keys() {
            let column_ref_id = ColumnRefId::from_table(ref_id, *cid);
            ids.push(self.egraph.add(Node::Column(column_ref_id)));
        }
        let list = self.egraph.add(Node::List(ids.into()));
        let scan = self.egraph.add(Node::Scan(list));
        Ok(scan)
    }
}
