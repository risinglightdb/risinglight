// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::str::FromStr;

use pretty_xmlish::helper::delegate_fmt;
use pretty_xmlish::Pretty;
use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub struct CreateFunction {
    name: String,
    arg_types: Vec<DataType>,
    return_types: DataType,
    language: String,
    body: String,
}

impl fmt::Display for CreateFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let explainer = Pretty::childless_record("CreateFunction", self.pretty_function());
        delegate_fmt(&explainer, f, String::with_capacity(1000))
    }
}

impl FromStr for CreateFunction {
    type Err = ();

    fn from_str(_s: &str) -> std::result::Result<Self, Self::Err> {
        Err(())
    }
}

impl CreateFunction {
    pub fn pretty_function<'a>(&self) -> Vec<(&'a str, Pretty<'a>)> {
        vec![
            ("name", Pretty::display(&self.name)),
            ("language", Pretty::display(&self.language)),
            ("body", Pretty::display(&self.body)),
        ]
    }
}

impl Binder {
    pub(super) fn bind_create_function(
        &mut self,
        _name: ObjectName,
        _args: Option<Vec<OperateFunctionArg>>,
        _return_type: Option<DataType>,
        _params: CreateFunctionBody,
    ) -> Result {
        todo!()
    }
}
