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
    build_source: Some(|schema, options| async move { build(schema, options) }.boxed()),
    build_sink: None,
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
    let mut number_of_rows = options
        .get("number_of_rows")
        .map(|s| s.parse::<usize>().context("number_of_rows must be integer"))
        .transpose()?
        .unwrap_or(usize::MAX);

    let batch_interval = Duration::from_secs(1) * batch_size / rows_per_second;
    let mut generators: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| RandomGenerator::new(f.data_type()))
        .collect();
    let schema0 = schema.clone();
    Ok(async_stream::try_stream! {
        let mut next_output_instant = tokio::time::Instant::now();
        while number_of_rows > 0 {
            let num_rows = number_of_rows.min(batch_size as usize);
            let batch = RecordBatch::try_new(
                schema.clone(),
                generators.iter_mut()
                    .map(|g| g.next(num_rows))
                    .collect(),
            )?.into();
            next_output_instant += batch_interval;
            number_of_rows -= num_rows;
            tokio::time::sleep_until(next_output_instant).await;
            yield batch;
        }
    }
    .with_schema(schema0))
}

struct RandomGenerator {
    data_type: DataType,
    rng: SmallRng,
}

impl RandomGenerator {
    /// Create a new random generator for `data_type`.
    fn new(data_type: &DataType) -> Self {
        Self {
            data_type: data_type.clone(),
            rng: SmallRng::seed_from_u64(0),
        }
    }
}

impl RandomGenerator {
    /// Generate a new array with `num_rows` values.
    fn next(&mut self, num_rows: usize) -> ArrayRef {
        match self.data_type {
            DataType::Int16 => {
                let mut builder = Int16Array::builder(num_rows);
                for _ in 0..num_rows {
                    builder.append_value(self.rng.gen());
                }
                Arc::new(builder.finish())
            }
            DataType::Int32 => {
                let mut builder = Int32Array::builder(num_rows);
                for _ in 0..num_rows {
                    builder.append_value(self.rng.gen());
                }
                Arc::new(builder.finish())
            }
            DataType::Int64 => {
                let mut builder = Int64Array::builder(num_rows);
                for _ in 0..num_rows {
                    builder.append_value(self.rng.gen());
                }
                Arc::new(builder.finish())
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 100);
                for _ in 0..num_rows {
                    for b in (&mut self.rng).sample_iter(&Alphanumeric).take(100) {
                        use std::fmt::Write;
                        builder.write_char(b as char).unwrap();
                    }
                    builder.append_value("");
                }
                Arc::new(builder.finish())
            }
            _ => todo!("random generator for {:?}", self.data_type),
        }
    }
}
