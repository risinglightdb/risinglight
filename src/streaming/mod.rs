use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use tokio::sync::broadcast;

use self::array::{DeltaBatch, DeltaBatchStream, DeltaBatchStreamExt};
use crate::binder::CreateMView;
use crate::catalog::{CatalogError, RootCatalogRef, TableRefId};
use crate::planner::RecExpr;
use crate::storage::TracedStorageError;
use crate::types::ConvertError;

mod array;
mod connector;
mod executor;
mod expr;
mod table;

pub struct StreamManager {
    catalog: RootCatalogRef,
    tables: Mutex<HashMap<TableRefId, Source>>,
}

struct Source {
    task: tokio::task::JoinHandle<()>,
    sender: broadcast::Sender<Result<DeltaBatch>>,
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

    // pub async fn create_source(
    //     &self,
    //     schema: SchemaRef,
    //     options: &BTreeMap<String, String>,
    // ) -> Result<()> {
    //     let stream = source::build(schema, options).await?;
    //     self.create_stream(id, stream)
    // }

    // pub fn create_mview(&self, id: TableRefId, query: RecExpr) -> Result<()> {
    //     let stream = executor::Builder::new(&self, &query).build();
    //     self.create_stream(id, stream)
    // }

    // pub fn create_stream(&self, id: TableRefId, mut stream: DeltaBatchStream) -> Result<()> {
    //     let (sender, _) = broadcast::channel::<Result<DeltaBatch>>(16);
    //     let sender0 = sender.clone();
    //     let task = tokio::spawn(async move {
    //         while let Some(value) = stream.next().await {
    //             // if no registered receiver, `send` will return error and the value will be
    //             // dropped.
    //             _ = sender0.send(value);
    //         }
    //     });
    //     self.tables
    //         .lock()
    //         .unwrap()
    //         .insert(id, Source { task, sender });
    //     Ok(())
    // }

    // pub fn drop_stream(&self, id: TableRefId) -> Result<()> {
    //     self.tables.lock().unwrap().remove(&id);
    //     Ok(())
    // }

    // pub fn get_stream(&self, id: TableRefId) -> DeltaBatchStream {
    //     let mut rx = (self.tables.lock().unwrap().get(&id).unwrap())
    //         .sender
    //         .subscribe();
    //     async_stream::try_stream! {
    //         while let Ok(value) = rx.recv().await {
    //             yield value?;
    //         }
    //     }
    //     .boxed()
    // }
}
