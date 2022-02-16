// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

// =====================================================================
// THIS FILE IS COPIED AND MODIFIED FROM THE CRATE awaitgroup 0.6.0 LOCATED AT
// https://github.com/ibraheemdev/awaitgroup/blob/d8ab1fd55a3b601fa241267067703a97ee1de8d1/src/lib.rs
// HERE IS THE ORIGINAL LICENSE.
//

// MIT License

// Copyright (c) 2021 ibraheemdev

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// =====================================================================

//! An asynchronous implementation of a `WaitGroup`.
//!
//! A `WaitGroup` waits for a collection of tasks to finish. The main task can create new workers
//! and pass them to each of the tasks it wants to wait for. Then, each of the tasks calls `done`
//! when it finishes executing. The main task can call `wait` to block until all registered workers
//! are done.
//!
//! # Examples
//!
//! ```rust
//! # fn main() {
//! # let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
//! # rt.block_on(async {
//! # use risinglight::utils::sync::WaitGroup;
//!
//! let mut wg = WaitGroup::new();
//!
//! for _ in 0..5 {
//!     // Create a new worker.
//!     if let Some(worker) = wg.worker() {
//!         tokio::spawn(async {
//!             // Do some work...
//!
//!             // This task is done all of its work.
//!             worker.done();
//!         });
//!     }
//! }
//!
//! // Shutdown the wait group.
//! wg.shutdown();
//!
//! // Block until all other tasks have finished their work.
//! wg.wait().await;
//! # });
//! # }
//! ```
//!
//! A `WaitGroup` can be re-used and awaited multiple times before shutdown.
//! ```rust
//! # use risinglight::utils::sync::WaitGroup;
//! # fn main() {
//! # let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
//! # rt.block_on(async {
//! let mut wg = WaitGroup::new();
//!
//! if let Some(worker) = wg.worker() {
//!     tokio::spawn(async {
//!         // Do work...
//!         worker.done();
//!     });
//! }
//!
//! // Wait for tasks to finish
//! wg.wait().await;
//!
//! // Re-use wait group
//! if let Some(worker) = wg.worker() {
//!     tokio::spawn(async {
//!         // Do more work...
//!         worker.done();
//!     });
//! }
//!
//! // Shutdown the wait group.
//! wg.shutdown();
//!
//! wg.wait().await;
//! # });
//! # }
//! ```
// #![deny(missing_debug_implementations, rust_2018_idioms)]
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use super::counter::SealableAtomicCounter;

/// Wait for a collection of tasks to finish execution.
///
/// Refer to the [crate level documentation](crate) for details.
#[derive(Default)]
pub struct WaitGroup {
    inner: Arc<Inner>,
}

#[allow(clippy::new_without_default)]
impl WaitGroup {
    /// Creates a new `WaitGroup`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new worker. If the `WaitGroup` is shutdown, returns None instead.
    pub fn worker(&self) -> Option<Worker> {
        self.inner.count.increase().map(|_| Worker {
            inner: self.inner.clone(),
        })
    }

    /// Whether the `WaitGroup` is shutdown.
    pub fn is_shutdown(&self) -> bool {
        self.inner.count.is_sealed()
    }

    /// Shutdown the `WaitGroup` atomically. Returns true if it's shutdown the first time.
    pub fn shutdown(&self) -> bool {
        self.inner.count.seal()
    }

    /// Wait until all registered workers finish executing.
    pub async fn wait(&self) {
        WaitGroupFuture::new(&self.inner).await
    }
}

struct WaitGroupFuture<'a> {
    inner: &'a Arc<Inner>,
}

impl<'a> WaitGroupFuture<'a> {
    fn new(inner: &'a Arc<Inner>) -> Self {
        Self { inner }
    }
}

impl Future for WaitGroupFuture<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let waker = cx.waker().clone();
        *self.inner.waker.lock().unwrap() = Some(waker);

        match self.inner.count.value() {
            0 => Poll::Ready(()),
            _ => Poll::Pending,
        }
    }
}

#[derive(Default)]
struct Inner {
    waker: Mutex<Option<Waker>>,
    count: SealableAtomicCounter,
}

/// A worker registered in a `WaitGroup`.
///
/// Refer to the [crate level documentation](crate) for details.
pub struct Worker {
    inner: Arc<Inner>,
}

impl Worker {
    /// Notify the `WaitGroup` that this worker has finished execution.
    pub fn done(self) {
        drop(self)
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let count = self.inner.count.decrease();

        // Wake when we are the last worker.
        if count == 1 {
            if let Some(waker) = self.inner.waker.lock().unwrap().take() {
                waker.wake();
            }
        }
    }
}

impl fmt::Debug for WaitGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.inner.count.value();
        f.debug_struct("WaitGroup").field("count", &count).finish()
    }
}

impl fmt::Debug for Worker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.inner.count.value();
        f.debug_struct("Worker").field("count", &count).finish()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_wait_group() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        rt.block_on(async {
            let wg = WaitGroup::new();

            for _ in 0..5 {
                if let Some(worker) = wg.worker() {
                    tokio::spawn(async {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        worker.done();
                    });
                } else {
                    panic!("failed new worker");
                }
            }

            wg.wait().await;
        });
    }

    #[test]
    fn test_wait_group_reuse() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        rt.block_on(async {
            let wg = WaitGroup::new();

            for _ in 0..5 {
                if let Some(worker) = wg.worker() {
                    tokio::spawn(async {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        worker.done();
                    });
                } else {
                    panic!("failed new worker");
                }
            }

            wg.wait().await;

            if let Some(worker) = wg.worker() {
                tokio::spawn(async {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    worker.done();
                });
            }

            wg.wait().await;
        });
    }

    #[test]
    fn test_wait_group_shutdown() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        rt.block_on(async {
            let wg = WaitGroup::new();

            for _ in 0..5 {
                if let Some(worker) = wg.worker() {
                    tokio::spawn(async {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        worker.done();
                    });
                }
            }

            assert!(wg.shutdown());
            assert!(wg.is_shutdown());

            assert!(wg.worker().is_none());

            wg.wait().await;
        });
    }
}
