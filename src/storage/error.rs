// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::backtrace::Backtrace;
use std::sync::Arc;

use thiserror::Error;

use crate::catalog::ColumnId;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("failed to read table")]
    ReadTableError,
    #[error("failed to write table")]
    WriteTableError,
    #[error("{0}({1}) not found")]
    NotFound(&'static str, String),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
    #[error("invalid column id: {0}")]
    InvalidColumn(ColumnId),
    #[error("IO error: {0}")]
    Io(#[from] Box<std::io::Error>),
    #[error("JSON decode error: {0}")]
    JsonDecode(#[from] serde_json::Error),
    #[error("Decode error: {0}")]
    Decode(String),
    #[error("Invalid checksum: found {0}, expected {1}")]
    Checksum(u64, u64),
    #[error("Prost encode error: {0}")]
    ProstEncode(prost::EncodeError),
    #[error("Prost decode error: {0}")]
    ProstDecode(prost::DecodeError),
    #[error("{0}")]
    Nested(
        #[from]
        #[backtrace]
        Arc<TracedStorageError>,
    ),
}

impl From<std::io::Error> for TracedStorageError {
    #[inline]
    fn from(e: std::io::Error) -> TracedStorageError {
        StorageError::Io(e.into()).into()
    }
}

impl From<serde_json::Error> for TracedStorageError {
    #[inline]
    fn from(e: serde_json::Error) -> TracedStorageError {
        StorageError::JsonDecode(e).into()
    }
}

impl From<prost::EncodeError> for TracedStorageError {
    #[inline]
    fn from(e: prost::EncodeError) -> TracedStorageError {
        StorageError::ProstEncode(e).into()
    }
}

impl From<prost::DecodeError> for TracedStorageError {
    #[inline]
    fn from(e: prost::DecodeError) -> TracedStorageError {
        StorageError::ProstDecode(e).into()
    }
}

impl From<Arc<TracedStorageError>> for TracedStorageError {
    #[inline]
    fn from(e: Arc<TracedStorageError>) -> TracedStorageError {
        StorageError::Nested(e).into()
    }
}

/// [`StorageResult`] with backtrace.
#[derive(Error)]
#[error("{source:?}\n{backtrace}")]
pub struct TracedStorageError {
    #[from]
    source: StorageError,
    backtrace: Backtrace,
}

impl std::fmt::Debug for TracedStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl TracedStorageError {
    pub fn duplicated(ty: &'static str, item: impl ToString) -> Self {
        StorageError::Duplicated(ty, item.to_string()).into()
    }

    pub fn not_found(ty: &'static str, item: impl ToString) -> Self {
        StorageError::NotFound(ty, item.to_string()).into()
    }

    pub fn decode(message: impl ToString) -> Self {
        StorageError::Decode(message.to_string()).into()
    }

    pub fn checksum(found: u64, expected: u64) -> Self {
        StorageError::Checksum(found, expected).into()
    }
}

pub type StorageResult<T> = std::result::Result<T, TracedStorageError>;
