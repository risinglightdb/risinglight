use crate::types::DataType;
use std::path::PathBuf;

/// The physical plan of `copy`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalCopyFromFile {
    /// The file path to copy from.
    pub path: PathBuf,
    /// The file format.
    pub format: FileFormat,
    /// The column types.
    pub column_types: Vec<DataType>,
}

/// File format.
#[derive(Debug, PartialEq, Clone)]
pub enum FileFormat {
    Csv {
        /// Delimiter to parse.
        delimiter: u8,
        /// Quote to use.
        quote: u8,
        /// Escape character to use.
        escape: Option<u8>,
        /// Whether or not the file has a header line.
        header: bool,
    },
}
