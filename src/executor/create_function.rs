// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::binder::CreateFunction;
use crate::catalog::RootCatalogRef;

/// The executor of `create function` statement.
pub struct CreateFunctionExecutor {
    pub f: CreateFunction,
    pub catalog: RootCatalogRef,
}

impl CreateFunctionExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let CreateFunction {
            schema_name,
            name,
            arg_types,
            arg_names,
            return_type,
            language,
            body,
        } = self.f;

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
