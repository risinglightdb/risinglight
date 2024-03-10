use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{FutureExt, Sink};

use super::*;

#[linkme::distributed_slice(connector::CONNECTORS)]
static DATAGEN: Connector = Connector {
    name: "blackhole",
    build_source: None,
    build_sink: Some(|schema, _| async move { Ok(Blackhole.with_schema(schema)) }.boxed()),
};

pub struct Blackhole;

impl Sink<DeltaBatch> for Blackhole {
    type Error = anyhow::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, _item: DeltaBatch) -> Result<()> {
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}
