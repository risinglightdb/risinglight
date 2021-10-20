use crate::types::ColumnId;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum StorageError {
    #[error("failed to read table")]
    ReadTableError,
    #[error("failed to write table")]
    WriteTableError,
    #[error("{0}({1}) not found")]
    NotFound(&'static str, u32),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
    #[error("invalid column id: {0}")]
    InvalidColumn(ColumnId),
    #[error("IO error: {0} {1:?}")]
    IOError(&'static str, std::io::ErrorKind),
}

pub type StorageResult<T> = std::result::Result<T, StorageError>;
