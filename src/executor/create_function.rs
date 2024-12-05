// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::binder::FunctionDef;
use crate::catalog::RootCatalogRef;

/// The executor of `create function` statement.
pub struct CreateFunctionExecutor {
    pub function: Box<FunctionDef>,
    pub catalog: RootCatalogRef,
}

impl CreateFunctionExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let FunctionDef {
            schema_name,
            name,
            arg_types,
            arg_names,
            return_type,
            language,
            body,
        } = *self.function;

        self.catalog.create_function(
            schema_name.clone(),
            name.clone(),
            arg_types,
            arg_names,
            return_type,
            language,
            body,
        );
    }
}
