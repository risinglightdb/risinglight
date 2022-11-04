// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use itertools::Itertools;
use sqlparser::ast::{Expr, Value};

use super::*;
use crate::catalog::{ColumnCatalog, TableRefId};
use crate::parser::{Query, SetExpr};
use crate::types::{ColumnId, DataType};

impl Binder {
    pub fn bind_insert(
        &mut self,
        table_name: ObjectName,
        columns: Vec<Ident>,
        source: Box<Query>,
    ) -> Result {
        let cols = self.bind_table_columns(table_name, &columns)?;
        let source = self.bind_query(*source)?;
        let id = self.egraph.add(Node::Insert([cols, source]));
        Ok(id)
    }
}
