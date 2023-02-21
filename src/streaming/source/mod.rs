use std::collections::BTreeMap;

use futures::stream::BoxStream;
use futures::TryStreamExt;

use super::*;
use crate::array::DataChunk;
use crate::catalog::TableCatalog;

pub mod datagen;
pub mod nexmark;

type BoxDataStream = BoxStream<'static, Result<DataChunk>>;

/// Build a source from table catalog and options.
pub async fn build(
    options: &BTreeMap<String, String>,
    catalog: &TableCatalog,
) -> Result<BoxDiffStream> {
    let connector = (options.get("connector")).ok_or(Error::MissingField("connector"))?;
    let stream = match connector.as_str() {
        "datagen" => self::datagen::build(options, catalog).await?,
        "nexmark" => self::nexmark::build(options, catalog).await?,
        _ => return Err(Error::UnsupportedConnector(connector.clone())),
    };
    Ok(stream.map_ok(StreamChunk::from).boxed())
}
