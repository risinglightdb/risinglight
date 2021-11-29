//! Execute the queries.

use crate::{array::DataChunk, binder::BoundStatement, catalog::RootCatalogRef};

mod create;

use self::create::*;

/// The error type of execution.
#[derive(thiserror::Error, Debug)]
pub enum ExecuteError {}

pub trait Executor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError>;
}

/// A type-erased executor object.
pub type BoxedExecutor = Box<dyn Executor>;

/// The builder of executor.
pub struct ExecutorBuilder {
    catalog: RootCatalogRef,
}

impl ExecutorBuilder {
    /// Create a new executor builder.
    pub fn new(catalog: RootCatalogRef) -> ExecutorBuilder {
        ExecutorBuilder { catalog }
    }

    /// Build executor from a [BoundStatement].
    pub fn build(&self, stmt: BoundStatement) -> BoxedExecutor {
        match stmt {
            BoundStatement::CreateTable(stmt) => Box::new(CreateTableExecutor {
                stmt,
                catalog: self.catalog.clone(),
            }),
        }
    }
}
