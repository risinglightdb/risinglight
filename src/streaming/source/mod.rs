use std::collections::BTreeMap;

use futures::stream::BoxStream;

use super::*;
use crate::array::DataChunk;
use crate::catalog::TableCatalog;

pub mod nexmark;

pub type BoxSourceStream = BoxStream<'static, Result<DataChunk>>;

/// Build a source from table catalog and options.
pub async fn build(
    options: &BTreeMap<String, String>,
    catalog: &TableCatalog,
) -> Result<BoxSourceStream> {
    let connector = (options.get("connector")).ok_or(Error::MissingField("connector"))?;
    Ok(match connector.as_str() {
        "nexmark" => self::nexmark::build(options, catalog).await?,
        _ => return Err(Error::UnsupportedConnector(connector.clone())),
    })
}
