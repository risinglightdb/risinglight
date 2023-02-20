use std::collections::HashMap;
use std::sync::Mutex;

use futures::StreamExt;
use tokio::sync::broadcast;

use crate::array::{DataChunk, StreamChunk};
use crate::binder::CreateTable;
use crate::catalog::{CatalogError, RootCatalogRef, TableRefId};

mod builder;
mod source;

struct StreamManager {
    catalog: RootCatalogRef,
    tables: Mutex<HashMap<TableRefId, Source>>,
}

struct Source {
    task: tokio::task::JoinHandle<()>,
    sender: broadcast::Sender<Result<DataChunk>>,
}

impl StreamManager {
    pub async fn create_source(&self, stmt: CreateTable) -> Result<()> {
        let id = self.catalog.add_table(
            stmt.schema_id,
            stmt.table_name,
            stmt.columns,
            stmt.ordered_pk_ids,
        )?;
        let catalog = self.catalog.get_table(&id).unwrap();

        let mut stream = source::build(&stmt.with, &catalog).await?;

        let (sender, _) = broadcast::channel::<Result<DataChunk>>(16);
        let sender0 = sender.clone();
        let task = tokio::spawn(async move {
            while let Some(value) = stream.next().await {
                sender0.send(value).expect("failed to send");
            }
        });
        self.tables
            .lock()
            .unwrap()
            .insert(id, Source { task, sender });
        Ok(())
    }

    pub fn drop_source(&self, id: TableRefId) -> Result<()> {
        self.catalog.drop_table(id);
        self.tables.lock().unwrap().remove(&id);
        Ok(())
    }

    pub fn create_materialized_view(&self, stmt: CreateTable) -> Result<()> {
        todo!()
    }
}

/// The result type of streaming.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type of streaming.
#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("missing field: {0}")]
    MissingField(&'static str),
    #[error("unsupported connector: {0}")]
    UnsupportedConnector(String),
}
