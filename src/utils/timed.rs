use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use futures::Stream;
use parking_lot::Mutex;

impl<T: Stream> StreamExt for T {}

/// An extension trait for `Streams` that provides tracing instrument adapters.
pub trait StreamExt: Stream + Sized {
    /// Binds a [`Span`] to the [`Stream`] that continues to record until the Stream is dropped.
    #[inline]
    fn timed(self, span: Span) -> Timed<Self> {
        Timed {
            inner: self,
            span: Some(span),
        }
    }
}

/// Adapter for [`StreamExt::timed()`](StreamExt::timed).
#[pin_project::pin_project]
pub struct Timed<T> {
    #[pin]
    inner: T,
    span: Option<Span>,
}

impl<T: Stream> Stream for Timed<T> {
    type Item = T::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let _guard = this.span.as_ref().map(|s| s.enter());

        match this.inner.poll_next(cx) {
            r @ Poll::Pending => r,
            other => {
                drop(_guard);
                this.span.take();
                other
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[derive(Debug, Default, Clone)]
pub struct Span {
    inner: Arc<Mutex<SpanInner>>,
}

#[derive(Debug, Default)]
struct SpanInner {
    busy_time: Duration,
    last_poll_time: Option<Instant>,
}

impl Span {
    pub fn enter(&self) -> Guard<'_> {
        Guard {
            span: self,
            start_time: std::time::Instant::now(),
        }
    }

    pub fn busy_time(&self) -> Duration {
        self.inner.lock().busy_time
    }

    pub fn last_poll_time(&self) -> Option<Instant> {
        self.inner.lock().last_poll_time
    }
}

pub struct Guard<'a> {
    span: &'a Span,
    start_time: std::time::Instant,
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        let now = Instant::now();
        let mut span = self.span.inner.lock();
        span.busy_time += now - self.start_time;
        span.last_poll_time = Some(now);
    }
}
