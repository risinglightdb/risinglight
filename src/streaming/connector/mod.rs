use std::collections::BTreeMap;

use anyhow::Context;
use arrow::datatypes::SchemaRef;
use futures::future::BoxFuture;

use super::*;

pub mod datagen;
pub mod nexmark;

/// A connector descriptor.
#[derive(Debug)]
pub struct Connector {
    /// The name of the connector.
    pub name: &'static str,
    /// The function to create a stream of the source.
    pub build: fn(
        schema: SchemaRef,
        options: &BTreeMap<String, String>,
    ) -> BoxFuture<Result<DeltaBatchStream>>,
}

#[linkme::distributed_slice]
pub static CONNECTORS: [Connector];

/// Build a source from schema and options.
pub async fn build(
    schema: SchemaRef,
    options: &BTreeMap<String, String>,
) -> Result<DeltaBatchStream> {
    let connector = options
        .get("connector")
        .context("missing field: connector")?;
    let connector = CONNECTORS
        .iter()
        .find(|c| c.name == connector)
        .with_context(|| format!("connector not found: {connector}"))?;
    let stream = (connector.build)(schema, options).await?;
    Ok(stream)
}
