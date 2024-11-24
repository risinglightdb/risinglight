use std::fmt::{self, Debug};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Poll;

use futures::Stream;

use crate::array::DataChunk;

impl<T: Stream> StreamExt for T {}

/// An extension trait for `Streams` that provides counting instrument adapters.
pub trait StreamExt: Stream + Sized {
    /// Binds a [`Counter`] to the [`Stream`] that counts the number of rows.
    #[inline]
    fn counted(self, counter: Counter) -> Counted<Self> {
        Counted {
            inner: self,
            counter,
        }
    }
}

/// Adapter for [`StreamExt::counted()`](StreamExt::counted).
#[pin_project::pin_project]
pub struct Counted<T> {
    #[pin]
    inner: T,
    counter: Counter,
}

impl<E, T: Stream<Item = Result<DataChunk, E>>> Stream for Counted<T> {
    type Item = T::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let result = this.inner.poll_next(cx);
        if let Poll::Ready(Some(Ok(chunk))) = &result {
            this.counter.inc(chunk.cardinality() as u64);
        }
        result
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// A counter.
#[derive(Default, Clone)]
pub struct Counter {
    count: Arc<AtomicU64>,
}

impl Debug for Counter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get())
    }
}

impl Counter {
    /// Increments the counter.
    pub fn inc(&self, value: u64) {
        self.count.fetch_add(value, Ordering::Relaxed);
    }

    /// Gets the current value of the counter.
    pub fn get(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}
