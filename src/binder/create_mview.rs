use std::fmt;
use std::str::FromStr;

use pretty_xmlish::helper::delegate_fmt;
use pretty_xmlish::Pretty;
use serde::{Deserialize, Serialize};

use super::*;
use crate::catalog::SchemaId;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub struct CreateMView {
    pub schema_id: SchemaId,
    pub name: String,
}

impl fmt::Display for CreateMView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let explainer = Pretty::childless_record("CreateMView", self.pretty_table());
        delegate_fmt(&explainer, f, String::with_capacity(1000))
    }
}

impl CreateMView {
    pub fn pretty_table<'a>(&self) -> Vec<(&'a str, Pretty<'a>)> {
        vec![
            ("schema_id", Pretty::display(&self.schema_id)),
            ("name", Pretty::display(&self.name)),
        ]
    }
}

impl FromStr for CreateMView {
    type Err = ();

    fn from_str(_s: &str) -> std::result::Result<Self, Self::Err> {
        Err(())
    }
}

impl Binder {
    pub(super) fn bind_create_mview(&mut self, name: ObjectName, query: Query) -> Result<Id> {
        let name = lower_case_name(&name);
        let (schema_name, table_name) = split_name(&name)?;
        let schema = self
            .catalog
            .get_schema_by_name(schema_name)
            .ok_or_else(|| BindError::InvalidSchema(schema_name.into()))?;
        if schema.get_table_by_name(table_name).is_some() {
            return Err(BindError::TableExists(table_name.into()));
        }

        let args = self.egraph.add(Node::CreateMViewArgs(CreateMView {
            schema_id: schema.id(),
            name: table_name.into(),
        }));
        let query = self.bind_query(query)?.0;
        let id = self.egraph.add(Node::CreateMView([args, query]));
        Ok(id)
    }
}
