use std::time::Duration;

use anyhow::Context;
use arrow::array::*;
use arrow::datatypes::DataType;
use futures::FutureExt;
use rand::distributions::Alphanumeric;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use super::*;

#[linkme::distributed_slice(connector::CONNECTORS)]
static DATAGEN: Connector = Connector {
    name: "datagen",
    build: |schema, options| async move { build(schema, options) }.boxed(),
};

/// Build a datagen source.
pub fn build(schema: SchemaRef, options: &BTreeMap<String, String>) -> Result<DeltaBatchStream> {
    let rows_per_second = options
        .get("rows_per_second")
        .map(|s| s.parse::<u32>().context("rows_per_second must be integer"))
        .transpose()?
        .unwrap_or(10000);
    let batch_size = options
        .get("batch_size")
        .map(|s| s.parse::<u32>().context("batch_size must be integer"))
        .transpose()?
        .unwrap_or(1000);
    let number_of_rows = options
        .get("number_of_rows")
        .map(|s| s.parse::<u32>().context("number_of_rows must be integer"))
        .transpose()?
        .unwrap_or(u32::MAX);

    let batch_interval = Duration::from_secs(1) * batch_size / rows_per_second;
    let mut generators: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| RandomGenerator::new(f.data_type(), batch_size as usize))
        .collect();
    let schema0 = schema.clone();
    Ok(async_stream::try_stream! {
        let mut next_output_instant = tokio::time::Instant::now();
        for i in 0..number_of_rows {
            let batch = RecordBatch::try_new(
                schema.clone(),
                generators.iter_mut()
                    .map(|g| g.next().unwrap())
                    .collect(),
            )?.into();
            next_output_instant += batch_interval;
            tokio::time::sleep_until(next_output_instant).await;
            yield batch;
        }
    }
    .with_schema(schema0))
}

struct RandomGenerator {
    data_type: DataType,
    rng: SmallRng,
    batch_size: usize,
}

impl RandomGenerator {
    /// Create a new random generator for `data_type`.
    ///
    /// The generator will produce `batch_size` elements at a time.
    fn new(data_type: &DataType, batch_size: usize) -> Self {
        Self {
            data_type: data_type.clone(),
            rng: SmallRng::seed_from_u64(0),
            batch_size,
        }
    }
}

impl Iterator for RandomGenerator {
    type Item = ArrayRef;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match self.data_type {
            DataType::Int16 => {
                let mut builder = Int16Array::builder(self.batch_size);
                for _ in 0..self.batch_size {
                    builder.append_value(self.rng.gen());
                }
                Arc::new(builder.finish())
            }
            DataType::Int32 => {
                let mut builder = Int32Array::builder(self.batch_size);
                for _ in 0..self.batch_size {
                    builder.append_value(self.rng.gen());
                }
                Arc::new(builder.finish())
            }
            DataType::Int64 => {
                let mut builder = Int64Array::builder(self.batch_size);
                for _ in 0..self.batch_size {
                    builder.append_value(self.rng.gen());
                }
                Arc::new(builder.finish())
            }
            DataType::Utf8 => {
                let mut builder =
                    StringBuilder::with_capacity(self.batch_size, self.batch_size * 10);
                for _ in 0..self.batch_size {
                    for b in (&mut self.rng).sample_iter(&Alphanumeric).take(10) {
                        use std::fmt::Write;
                        builder.write_char(b as char).unwrap();
                    }
                    builder.append_value("");
                }
                Arc::new(builder.finish())
            }
            _ => todo!("random generator for {:?}", self.data_type),
        })
    }
}
