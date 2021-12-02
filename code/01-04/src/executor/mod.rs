//! Execute the queries.

use crate::{
    array::DataChunk,
    binder::BoundStatement,
    catalog::RootCatalogRef,
    storage::{StorageError, StorageRef},
};

mod create;

use self::create::*;

/// The error type of execution.
#[derive(thiserror::Error, Debug)]
pub enum ExecuteError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

pub trait Executor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError>;
}

/// A type-erased executor object.
pub type BoxedExecutor = Box<dyn Executor>;

/// The builder of executor.
pub struct ExecutorBuilder {
    catalog: RootCatalogRef,
    storage: StorageRef,
}

impl ExecutorBuilder {
    /// Create a new executor builder.
    pub fn new(catalog: RootCatalogRef, storage: StorageRef) -> ExecutorBuilder {
        ExecutorBuilder { catalog, storage }
    }

    /// Build executor from a [BoundStatement].
    pub fn build(&self, stmt: BoundStatement) -> BoxedExecutor {
        match stmt {
            BoundStatement::CreateTable(stmt) => Box::new(CreateTableExecutor {
                stmt,
                catalog: self.catalog.clone(),
                storage: self.storage.clone(),
            }),
        }
    }
}
