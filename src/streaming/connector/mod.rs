use std::collections::BTreeMap;

use anyhow::Context;
use arrow::datatypes::SchemaRef;
use futures::future::BoxFuture;

use super::*;

mod blackhole;
mod datagen;
mod nexmark;

/// A connector descriptor.
#[derive(Debug)]
pub struct Connector {
    /// The name of the connector.
    pub name: &'static str,
    /// The function to create a source of the connector.
    pub build_source: Option<
        fn(
            schema: SchemaRef,
            options: &BTreeMap<String, String>,
        ) -> BoxFuture<Result<DeltaBatchStream>>,
    >,
    /// The function to create a sink of the connector.
    pub build_sink: Option<
        fn(
            schema: SchemaRef,
            options: &BTreeMap<String, String>,
        ) -> BoxFuture<Result<DeltaBatchSink>>,
    >,
}

#[linkme::distributed_slice]
pub static CONNECTORS: [Connector];

/// Build a source from schema and options.
pub async fn build_source(
    schema: SchemaRef,
    options: &BTreeMap<String, String>,
) -> Result<DeltaBatchStream> {
    let connector = options
        .get("connector")
        .context("missing field: connector")?;
    let build = CONNECTORS
        .iter()
        .find(|c| c.name == connector)
        .with_context(|| format!("connector not found: {connector}"))?
        .build_source
        .with_context(|| format!("connector does not support source: {connector}"))?;
    let stream = build(schema, options).await?;
    Ok(stream)
}
