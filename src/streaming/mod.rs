use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use tokio::sync::broadcast;

use self::executor::BoxDiffStream;
use crate::array::{DataChunk, StreamChunk};
use crate::binder::CreateMView;
use crate::catalog::{CatalogError, RootCatalogRef, TableRefId};
use crate::planner::RecExpr;
use crate::storage::TracedStorageError;
use crate::types::ConvertError;

mod executor;
mod source;

pub struct StreamManager {
    catalog: RootCatalogRef,
    tables: Mutex<HashMap<TableRefId, Source>>,
}

struct Source {
    task: tokio::task::JoinHandle<()>,
    sender: broadcast::Sender<Result<StreamChunk>>,
}

impl Drop for Source {
    fn drop(&mut self) {
        self.task.abort();
    }
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
        let stream = source::build(options, &catalog).await?;
        self.create_stream(id, stream)
    }

    pub fn create_mview(&self, id: TableRefId, query: RecExpr) -> Result<()> {
        let stream = executor::Builder::new(&self, &query).build();
        self.create_stream(id, stream)
    }

    pub fn create_stream(&self, id: TableRefId, mut stream: BoxDiffStream) -> Result<()> {
        let (sender, _) = broadcast::channel::<Result<StreamChunk>>(16);
        let sender0 = sender.clone();
        let task = tokio::spawn(async move {
            while let Some(value) = stream.next().await {
                // if no registered receiver, `send` will return error and the value will be
                // dropped.
                _ = sender0.send(value);
            }
        });
        self.tables
            .lock()
            .unwrap()
            .insert(id, Source { task, sender });
        Ok(())
    }

    pub fn drop_stream(&self, id: TableRefId) -> Result<()> {
        self.tables.lock().unwrap().remove(&id);
        Ok(())
    }

    pub fn get_stream(&self, id: TableRefId) -> BoxDiffStream {
        let mut rx = (self.tables.lock().unwrap().get(&id).unwrap())
            .sender
            .subscribe();
        async_stream::try_stream! {
            while let Ok(value) = rx.recv().await {
                yield value?;
            }
        }
        .boxed()
    }
}

/// The result type of streaming.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type of streaming.
#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(
        #[from]
        #[backtrace]
        #[source]
        Arc<TracedStorageError>,
    ),
    #[error("conversion error: {0}")]
    Convert(#[from] ConvertError),
    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("missing field: {0}")]
    MissingField(&'static str),
    #[error("unsupported connector: {0}")]
    UnsupportedConnector(String),
}
