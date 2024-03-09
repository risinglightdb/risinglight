// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};

use super::DeltaBatch;

/// A boxed [`DeltaBatch`] stream.
pub struct DeltaBatchStream {
    schema: SchemaRef,
    stream: BoxStream<'static, Result<DeltaBatch>>,
}

impl DeltaBatchStream {
    /// Returns the schema of the [`DeltaBatch`] produced by this stream.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for DeltaBatchStream {
    type Item = Result<DeltaBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl From<DeltaBatch> for DeltaBatchStream {
    fn from(batch: DeltaBatch) -> Self {
        let schema = batch.schema();
        let stream = futures::stream::once(async move { Ok(batch) }).boxed();
        DeltaBatchStream { schema, stream }
    }
}

pub trait DeltaBatchStreamExt: Stream<Item = Result<DeltaBatch>> + Send + 'static {
    fn with_schema(self, schema: SchemaRef) -> DeltaBatchStream;
}

impl<T> DeltaBatchStreamExt for T
where
    T: Stream<Item = Result<DeltaBatch>> + Send + 'static,
{
    fn with_schema(self, schema: SchemaRef) -> DeltaBatchStream {
        DeltaBatchStream {
            schema,
            stream: self.boxed(),
        }
    }
}
