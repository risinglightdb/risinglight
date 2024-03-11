// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::catalog::CatalogError;
use crate::storage::TracedStorageError;
use crate::types::ConvertError;

/// The result type of execution.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type of execution.
#[derive(thiserror::Error, Debug, Clone)]
#[error(transparent)]
pub struct Error {
    inner: Arc<Inner>,
}

#[derive(thiserror::Error, Debug)]
enum Inner {
    #[error("storage error: {0}")]
    Storage(
        #[from]
        #[backtrace]
        TracedStorageError,
    ),
    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),
    #[error("conversion error: {0}")]
    Convert(#[from] ConvertError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("csv error: {0}")]
    Csv(#[from] csv::Error),
    #[error("tuple length mismatch: expected {expected} but got {actual}")]
    LengthMismatch { expected: usize, actual: usize },
    #[error("exceed char/varchar length limit: item length {length} > char/varchar width {width}")]
    ExceedLengthLimit { length: u64, width: u64 },
    #[error("value can not be null")]
    NotNullable,
    #[error("abort")]
    Aborted,
}

impl From<Inner> for Error {
    fn from(e: Inner) -> Self {
        Error { inner: Arc::new(e) }
    }
}

impl From<TracedStorageError> for Error {
    fn from(e: TracedStorageError) -> Self {
        Inner::from(e).into()
    }
}

impl From<CatalogError> for Error {
    fn from(e: CatalogError) -> Self {
        Inner::from(e).into()
    }
}

impl From<ConvertError> for Error {
    fn from(e: ConvertError) -> Self {
        Inner::from(e).into()
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Inner::from(e).into()
    }
}

impl From<csv::Error> for Error {
    fn from(e: csv::Error) -> Self {
        Inner::from(e).into()
    }
}

impl Error {
    pub fn length_mismatch(expected: usize, actual: usize) -> Self {
        Inner::LengthMismatch { expected, actual }.into()
    }
    pub fn not_nullable() -> Self {
        Inner::NotNullable.into()
    }
    pub fn exceed_length_limit(length: u64, width: u64) -> Self {
        Inner::ExceedLengthLimit { length, width }.into()
    }
    pub fn aborted() -> Self {
        Inner::Aborted.into()
    }
}
