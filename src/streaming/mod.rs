use std::collections::HashMap;

use tokio::sync::broadcast;

use crate::array::StreamChunk;
use crate::binder::CreateTable;
use crate::catalog::{CatalogError, RootCatalogRef, TableRefId};
use crate::types::DataValue;

struct StreamManager {
    catalog: RootCatalogRef,
    tables: HashMap<TableRefId, broadcast::Sender<StreamChunk>>,
}

impl StreamManager {
    pub fn create_source(&self, stmt: CreateTable) -> Result<()> {
        let id = self.catalog.add_table(
            stmt.schema_id,
            stmt.table_name,
            stmt.columns,
            stmt.ordered_pk_ids,
        )?;

        let connector = (stmt.with.get("connector")).ok_or(StreamError::NoConnectorSpecified)?;
        match connector {
            DataValue::String(s) if s == "nexmark" => {}
            _ => todo!("not supported connector: {:?}", connector),
        }

        Ok(())
    }

    pub fn drop_source(&self, id: TableRefId) -> Result<()> {
        self.catalog.drop_table(id);
        self.tables.remove(&id);
        Ok(())
    }
}

/// The result type of streaming.
pub type Result<T> = std::result::Result<T, StreamError>;

/// The error type of streaming.
#[derive(thiserror::Error, Debug)]
pub enum StreamError {
    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),
    #[error("field 'connector' is not specified")]
    NoConnectorSpecified,
}
