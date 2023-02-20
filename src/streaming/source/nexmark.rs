use std::time::Duration;

use ::nexmark::config::NexmarkConfig;
use ::nexmark::event::*;
use ::nexmark::EventGenerator;
use futures::StreamExt;

use super::*;
use crate::array::DataChunkBuilder;
use crate::types::{DataTypeKind as K, DataValue};

/// Build a nexmark source.
pub async fn build(
    options: &BTreeMap<String, String>,
    catalog: &TableCatalog,
) -> Result<BoxSourceStream> {
    let type_ = options.get("type").ok_or(Error::MissingField("type"))?;
    let event_rate = options
        .get("event.rate")
        .ok_or(Error::MissingField("event.rate"))?
        .parse::<u32>()
        .map_err(|_| Error::InvalidArgument("event.rate must be an integer".to_string()))?;
    let interval = Duration::from_secs(1) / event_rate;

    Ok(match type_.as_str() {
        "person" => async_stream::try_stream! {
            let mut gen = EventGenerator::new(NexmarkConfig::default())
                .with_type_filter(EventType::Person);
            let types = [K::Int64, K::String, K::String, K::String, K::String, K::Int64, K::String].map(|k| k.not_null());
            let mut builder = DataChunkBuilder::new(types.iter(), 1024);

            let t0 = tokio::time::Instant::now();
            while let Some(Event::Person(person)) = gen.next() {
                tokio::time::sleep_until(t0 + interval * gen.global_offset() as u32).await;

                let chunk = builder.push_row([
                    DataValue::Int64(person.id as _),
                    DataValue::String(person.email_address),
                    DataValue::String(person.credit_card),
                    DataValue::String(person.city),
                    DataValue::String(person.state),
                    DataValue::Int64(person.date_time as _),
                    DataValue::String(person.extra),
                ]);
                if let Some(chunk) = chunk {
                    yield chunk;
                }
            }
        }.boxed(),
        "auction" => todo!(),
        "bid" => todo!(),
        _ => return Err(Error::InvalidArgument(format!("invalid type: {}", type_))),
    })
}
