use crate::types::ColumnId;

#[derive(thiserror::Error, Debug)]
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
    #[error("IO error: {0}")]
    Io(#[source] Box<std::io::Error>),
    #[error("JSON decode error: {0}")]
    JsonDecode(#[from] serde_json::Error),
}

impl From<std::io::Error> for StorageError {
    #[inline]
    fn from(e: std::io::Error) -> StorageError {
        StorageError::Io((e).into())
    }
}

pub type StorageResult<T> = std::result::Result<T, StorageError>;
