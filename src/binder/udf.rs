// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use egg::Id;

use crate::types::DataType;
use pretty_xmlish::helper::delegate_fmt;
use pretty_xmlish::Pretty;
use std::str::FromStr;
use std::fmt;

/// currently represents recursive sql udf
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct Udf {
    pub id: Id,
    pub name: String,
    pub body: String,
    pub return_type: DataType,
}

impl fmt::Display for Udf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let explainer = Pretty::childless_record("Udf", self.pretty_function());
        delegate_fmt(&explainer, f, String::with_capacity(1000))
    }
}

impl FromStr for Udf {
    type Err = ();

    fn from_str(_s: &str) -> std::result::Result<Self, Self::Err> {
        Err(())
    }
}

impl Udf {
    pub fn pretty_function<'a>(&self) -> Vec<(&'a str, Pretty<'a>)> {
        vec![
            ("name", Pretty::display(&self.name)),
            ("body", Pretty::display(&self.body)),
        ]
    }
}