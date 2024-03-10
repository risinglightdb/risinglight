// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use futures::{Sink, SinkExt};

use super::DeltaBatch;

/// A boxed [`DeltaBatch`] sink.
pub struct DeltaBatchSink {
    schema: SchemaRef,
    sink: Pin<Box<dyn Sink<DeltaBatch, Error = anyhow::Error> + Send>>,
}

impl DeltaBatchSink {
    /// Returns the schema of the [`DeltaBatch`] consumed by this sink.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Sink<DeltaBatch> for DeltaBatchSink {
    type Error = anyhow::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.sink.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: DeltaBatch) -> Result<()> {
        self.sink.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.sink.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.sink.poll_close_unpin(cx)
    }
}

pub trait DeltaBatchSinkExt: Sink<DeltaBatch, Error = anyhow::Error> + Send + 'static {
    fn with_schema(self, schema: SchemaRef) -> DeltaBatchSink;
}

impl<T> DeltaBatchSinkExt for T
where
    T: Sink<DeltaBatch, Error = anyhow::Error> + Send + 'static,
{
    fn with_schema(self, schema: SchemaRef) -> DeltaBatchSink {
        DeltaBatchSink {
            schema,
            sink: Box::pin(self),
        }
    }
}
