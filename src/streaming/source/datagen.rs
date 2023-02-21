use std::time::Duration;

use futures::StreamExt;
use rand::distributions::Alphanumeric;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use super::*;
use crate::array::DataChunkBuilder;
use crate::types::{DataTypeKind, DataValue};

/// Build a datagen source.
pub async fn build(
    options: &BTreeMap<String, String>,
    catalog: &TableCatalog,
) -> Result<BoxSourceStream> {
    let rows_per_second = options
        .get("rows.per.second")
        .map(|s| {
            s.parse::<u32>()
                .map_err(|_| Error::InvalidArgument("rows.per.second must be integer".to_string()))
        })
        .transpose()?
        .unwrap_or(10000);
    let number_of_rows = options
        .get("number.of.rows")
        .map(|s| {
            s.parse::<u32>()
                .map_err(|_| Error::InvalidArgument("number.of.rows must be integer".to_string()))
        })
        .transpose()?
        .unwrap_or(u32::MAX);

    let interval = Duration::from_secs(1) / rows_per_second;
    let datatypes: Vec<_> = catalog
        .all_columns()
        .values()
        .map(|c| c.datatype())
        .collect();
    let mut generators: Vec<_> = datatypes
        .iter()
        .map(|t| RandomGenerator::new(t.kind()))
        .collect();
    Ok(async_stream::try_stream! {
        let mut builder = DataChunkBuilder::new(datatypes.iter(), 1024);
        let t0 = tokio::time::Instant::now();
        for i in 0..number_of_rows {
            tokio::time::sleep_until(t0 + interval * i).await;

            let chunk = builder.push_row(generators.iter_mut().map(|g| g.next().unwrap()));
            if let Some(chunk) = chunk {
                yield chunk;
            }
        }
    }
    .boxed())
}

struct RandomGenerator {
    datatype: DataTypeKind,
    rng: SmallRng,
    length: usize,
}

impl RandomGenerator {
    fn new(datatype: DataTypeKind) -> Self {
        Self {
            datatype,
            rng: SmallRng::seed_from_u64(0),
            length: 100,
        }
    }
}

impl Iterator for RandomGenerator {
    type Item = DataValue;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match self.datatype {
            DataTypeKind::Int16 => DataValue::Int16(self.rng.gen()),
            DataTypeKind::Int32 => DataValue::Int32(self.rng.gen()),
            DataTypeKind::Int64 => DataValue::Int64(self.rng.gen()),
            DataTypeKind::String => DataValue::String(
                (0..self.length)
                    .map(|_| self.rng.sample(Alphanumeric) as char)
                    .collect(),
            ),
            _ => todo!("random generator for {:?}", self.datatype),
        })
    }
}
