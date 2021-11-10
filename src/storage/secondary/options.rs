use std::path::PathBuf;

/// Options for `SecondaryStorage`
#[derive(Clone)]
pub struct StorageOptions {
    /// Path of the storage engine
    pub path: PathBuf,

    /// Number of cache entrires
    pub cache_size: usize,

    /// Target size (in bytes) of RowSets
    pub target_rowset_size: usize,

    /// Target size (in bytes) of blocks
    pub target_block_size: usize,
}

impl StorageOptions {
    pub fn default_for_cli() -> Self {
        Self {
            path: PathBuf::new().join("risinglight.secondary.db"),
            cache_size: 1024,
            target_rowset_size: 1 << 20,       // 1MB
            target_block_size: 16 * (1 << 10), // 16KB
        }
    }

    pub fn default_for_test(path: PathBuf) -> Self {
        Self {
            path,
            cache_size: 1024,
            target_rowset_size: 1 << 20,       // 1MB
            target_block_size: 16 * (1 << 10), // 16KB
        }
    }
}

/// Options for `ColumnBuilder`s.
#[derive(Clone)]
pub struct ColumnBuilderOptions {
    pub target_block_size: usize,
}

impl ColumnBuilderOptions {
    pub fn from_storage_options(options: &StorageOptions) -> Self {
        Self {
            target_block_size: options.target_block_size,
        }
    }
}
