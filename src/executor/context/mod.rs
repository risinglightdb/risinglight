// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::future::Future;
use std::intrinsics;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::utils::sync::WaitGroup;

/// Context of executors.
#[derive(Default)]
pub struct Context {
    token: CancellationToken,
    wg: WaitGroup,
}

impl Context {
    pub fn new() -> Self {
        Self {
            token: Default::default(),
            wg: WaitGroup::new(),
        }
    }
}

impl Context {
    /// Cancels the execution and shutdown the wait group in context.
    /// This invokes the cancel function of owned cancellation token.
    pub fn cancel(&self) {
        self.wg.shutdown();
        self.token.cancel();
    }

    /// Determines if the context is cancelled.
    #[inline(always)]
    #[allow(unused_unsafe)]
    pub fn is_cancelled(&self) -> bool {
        unsafe { intrinsics::unlikely(self.token.is_cancelled()) }
    }

    /// Returns a future to await on cancellation. Commonly used
    /// with select macros.
    pub async fn cancelled(&self) {
        self.token.cancelled().await
    }

    /// Exports the token.
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Utility for spawning a task managed by this context.
    /// If this context is already cancelled, then no task is spawned.
    pub fn spawn<F, T>(&self, task_builder: F) -> Option<JoinHandle<T::Output>>
    where
        F: FnOnce(CancellationToken) -> T,
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.wg.worker().map(|worker| {
            let child_token = self.token.child_token();
            let task = task_builder(child_token);
            tokio::spawn(async move {
                let ret = task.await;
                worker.done();
                ret
            })
        })
    }

    /// Utility for spawning a blocking task managed by this context.
    /// If this context is already cancelled, then no task is spawned.
    pub fn spawn_blocking<F, R>(&self, task: F) -> Option<JoinHandle<R>>
    where
        F: FnOnce(CancellationToken) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.wg.worker().map(|worker| {
            let child_token = self.token.child_token();
            tokio::task::spawn_blocking(|| {
                let ret = task(child_token);
                worker.done();
                ret
            })
        })
    }

    /// Wait until all spawned tasks are ready.
    pub async fn wait(&self) {
        self.wg.wait().await;
    }
}
