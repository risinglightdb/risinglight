// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{PgWireError, PgWireResult};
use tracing::info;

use crate::Database;

pub struct Processor {
    db: Database,
}

impl Processor {
    pub fn new(db: Database) -> Self {
        Self { db }
    }
}

#[async_trait]
impl SimpleQueryHandler for Processor {
    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        info!("query:{query:?}");
        let chunks = self
            .db
            .run(query)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        if !query.to_uppercase().starts_with("SELECT") {
            return Ok(vec![Response::Execution(Tag::new("OK"))]);
        }
        let mut results = Vec::new();
        let mut headers = None;
        for chunk in chunks {
            for data_chunk in chunk.data_chunks() {
                for i in 0..data_chunk.cardinality() {
                    let headers = headers.get_or_insert_with(|| {
                        Arc::new(vec![
                            FieldInfo::new(
                                "++".into(),
                                None,
                                None,
                                Type::CHAR,
                                FieldFormat::Text
                            );
                            data_chunk.arrays().len()
                        ])
                    });
                    let mut encoder = DataRowEncoder::new(headers.clone());
                    data_chunk.arrays().iter().for_each(|a| {
                        let field = a.get_to_string(i);
                        encoder.encode_field(&field).unwrap();
                    });
                    results.push(encoder.finish());
                }
            }
        }
        Ok(vec![Response::Query(QueryResponse::new(
            headers.expect("fixme: db should return schema even if no output data"),
            stream::iter(results.into_iter()),
        ))])
    }
}
