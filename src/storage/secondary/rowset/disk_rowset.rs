use std::path::PathBuf;

/// Represents a single on-disk Rowset
pub struct DiskRowset {
    pub directory: PathBuf,
}

impl DiskRowset {
    pub fn new(directory: PathBuf) -> Self {
        Self { directory }
    }
}
