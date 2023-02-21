use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use tokio::sync::broadcast;

use crate::array::{DataChunk, StreamChunk};
use crate::binder::CreateTable;
use crate::catalog::{CatalogError, RootCatalogRef, TableRefId};

mod builder;
mod source;

pub struct StreamManager {
    catalog: RootCatalogRef,
    tables: Mutex<HashMap<TableRefId, Source>>,
}

struct Source {
    task: tokio::task::JoinHandle<()>,
    sender: broadcast::Sender<Result<DataChunk>>,
}

impl StreamManager {
    pub fn new(catalog: RootCatalogRef) -> Self {
        Self {
            catalog,
            tables: Mutex::new(HashMap::new()),
        }
    }

    pub async fn create_source(
        &self,
        id: TableRefId,
        options: &BTreeMap<String, String>,
    ) -> Result<()> {
        let catalog = self.catalog.get_table(&id).unwrap();

        let mut stream = source::build(options, &catalog).await?;

        let (sender, _) = broadcast::channel::<Result<DataChunk>>(16);
        let sender0 = sender.clone();
        let task = tokio::spawn(async move {
            while let Some(value) = stream.next().await {
                _ = sender0.send(value);
            }
        });
        self.tables
            .lock()
            .unwrap()
            .insert(id, Source { task, sender });
        Ok(())
    }

    pub fn drop_source(&self, id: TableRefId) -> Result<()> {
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
