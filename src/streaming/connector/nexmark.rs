use std::time::Duration;

use ::nexmark::config::NexmarkConfig;
use ::nexmark::event::*;
use ::nexmark::EventGenerator;
use anyhow::{bail, Context};
use arrow::array::{ArrayBuilder, RecordBatch, StructBuilder};
use arrow_udf::types::StructType;
use chrono::NaiveDateTime;
use futures::FutureExt;

use super::*;

#[linkme::distributed_slice(connector::CONNECTORS)]
static NEXMARK: Connector = Connector {
    name: "nexmark",
    build: |schema, options| async move { build(schema, options) }.boxed(),
};

/// Build a nexmark source.
pub fn build(schema: SchemaRef, options: &BTreeMap<String, String>) -> Result<DeltaBatchStream> {
    let type_ = options.get("type").context("missing field: type")?;
    let event_rate = options
        .get("event_rate")
        .context("missing field: event_rate")?
        .parse::<u32>()
        .context("event_rate must be an integer")?;
    let batch_size = options
        .get("batch_size")
        .map(|s| s.parse::<u32>().context("batch_size must be integer"))
        .transpose()?
        .unwrap_or(1000) as usize;
    let interval = Duration::from_secs(1) / event_rate;

    Ok(match type_.as_str() {
        "person" => {
            if schema.fields() != &Person::fields() {
                bail!(
                    "schema mismatch: expected {:?}, got {:?}",
                    Person::fields(),
                    schema.fields()
                );
            }
            async_stream::try_stream! {
                let mut gen = EventGenerator::new(NexmarkConfig::default())
                    .with_type_filter(EventType::Person);
                let mut builder = StructBuilder::from_fields(Person::fields(), batch_size);

                let t0 = tokio::time::Instant::now();
                while let Some(Event::Person(person)) = gen.next() {
                    Person::from(person).append_to(&mut builder);
                    if builder.len() < batch_size {
                        continue;
                    }
                    let batch = DeltaBatch::from(RecordBatch::from(builder.finish()));
                    tokio::time::sleep_until(t0 + interval * gen.global_offset() as u32).await;
                    yield batch;
                }
                if builder.len() > 0 {
                    let batch = DeltaBatch::from(RecordBatch::from(builder.finish()));
                    tokio::time::sleep_until(t0 + interval * gen.global_offset() as u32).await;
                    yield batch;
                }
            }
            .with_schema(schema)
        }
        "auction" => todo!(),
        "bid" => todo!(),
        _ => bail!("invalid type: {type_}, expect one of: auction, bid, person",),
    })
}

#[derive(StructType)]
struct Auction {
    id: i32,
    item_name: String,
    description: String,
    initial_bid: i32,
    reserve: i32,
    date_time: NaiveDateTime,
    expires: NaiveDateTime,
    seller: i32,
    category: i32,
    extra: String,
}

impl From<::nexmark::event::Auction> for Auction {
    fn from(auction: ::nexmark::event::Auction) -> Self {
        Self {
            id: auction.id as _,
            item_name: auction.item_name,
            description: auction.description,
            initial_bid: auction.initial_bid as _,
            reserve: auction.reserve as _,
            date_time: NaiveDateTime::from_timestamp_millis(auction.date_time as i64).unwrap(),
            expires: NaiveDateTime::from_timestamp_millis(auction.expires as i64).unwrap(),
            seller: auction.seller as _,
            category: auction.category as _,
            extra: auction.extra,
        }
    }
}

#[derive(StructType)]
struct Bid {
    auction: i32,
    bidder: i32,
    price: i32,
    channel: String,
    url: String,
    date_time: NaiveDateTime,
    extra: String,
}

impl From<::nexmark::event::Bid> for Bid {
    fn from(bid: ::nexmark::event::Bid) -> Self {
        Self {
            auction: bid.auction as _,
            bidder: bid.bidder as _,
            price: bid.price as _,
            channel: bid.channel,
            url: bid.url,
            date_time: NaiveDateTime::from_timestamp_millis(bid.date_time as i64).unwrap(),
            extra: bid.extra,
        }
    }
}

#[derive(StructType)]
struct Person {
    id: i32,
    name: String,
    email_address: String,
    credit_card: String,
    city: String,
    state: String,
    date_time: NaiveDateTime,
    extra: String,
}

impl From<::nexmark::event::Person> for Person {
    fn from(person: ::nexmark::event::Person) -> Self {
        Self {
            id: person.id as _,
            name: person.name,
            email_address: person.email_address,
            credit_card: person.credit_card,
            city: person.city,
            state: person.state,
            date_time: NaiveDateTime::from_timestamp_millis(person.date_time as i64).unwrap(),
            extra: person.extra,
        }
    }
}
