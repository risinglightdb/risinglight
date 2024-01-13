// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use crate::types::DataType;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct FunctionCatalog {
    name: String,
    arg_types: Vec<DataType>,
    return_type: DataType,
    language: String,
    body: String,
}

impl FunctionCatalog {
    pub fn new(
        name: String,
        arg_types: Vec<DataType>,
        return_type: DataType,
        language: String,
        body: String,
    ) -> Self {
        Self {
            name,
            arg_types,
            return_type,
            language,
            body,
        }
    }

    pub fn body(&self) -> String {
        self.body.clone()
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn language(&self) -> String {
        self.language.clone()
    }
}
