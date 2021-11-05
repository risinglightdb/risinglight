use std::path::PathBuf;

/// Options for `SecondaryStorage`
#[derive(Clone)]
pub struct StorageOptions {
    pub path: PathBuf,
    pub cache_size: usize,
}

impl StorageOptions {
    pub fn default_for_test() -> Self {
        Self {
            path: PathBuf::new().join("risinglight.secondary.db"),
            cache_size: 1024,
        }
    }
}

/// Options for `ColumnBuilder`s.
#[derive(Clone)]
pub struct ColumnBuilderOptions {
    pub target_size: usize,
}

impl ColumnBuilderOptions {
    pub fn from_storage_options(_options: &StorageOptions) -> Self {
        Self { target_size: 4096 }
    }
}
