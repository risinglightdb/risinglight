// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;

use risinglight_proto::rowset::block_checksum::ChecksumType;
use tracing::warn;

/// IO Backend of the rowset readers
#[derive(Clone, Copy)]
pub enum IOBackend {
    /// Use Linux's `pread` API to read from the files.
    PositionedRead,
    /// Use cross-platform API to read from files. Note that this would hurt performance
    NormalRead,
}

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

    /// I/O Backend used by the storage engine
    pub io_backend: IOBackend,

    /// Checksum type used by columns
    pub checksum_type: ChecksumType,

    /// Whether using run-length encoding
    pub is_rle: bool,
}

impl StorageOptions {
    pub fn default_for_cli() -> Self {
        Self {
            path: PathBuf::new().join("risinglight.secondary.db"),
            cache_size: 262144,                  // 4GB (16KB * 262144)
            target_rowset_size: 256 * (1 << 20), // 256MB
            target_block_size: 16 * (1 << 10),   // 16KB
            io_backend: if cfg!(target_os = "windows") {
                warn!("RisingLight's storage is running in compatibility mode (NormalRead), which might hurt I/O performance.");
                IOBackend::NormalRead
            } else {
                IOBackend::PositionedRead
            },
            checksum_type: ChecksumType::Crc32,
            is_rle: false,
        }
    }

    pub fn default_for_test(path: PathBuf) -> Self {
        Self {
            path,
            cache_size: 1024,
            target_rowset_size: 1 << 20,       // 1MB
            target_block_size: 16 * (1 << 10), // 16KB
            io_backend: IOBackend::NormalRead,
            checksum_type: ChecksumType::None,
            is_rle: false,
        }
    }
}

/// Options for `ColumnBuilder`s.
#[derive(Clone)]
pub struct ColumnBuilderOptions {
    /// Target size (in bytes) of blocks
    pub target_block_size: usize,

    /// Checksum type used by columns
    pub checksum_type: ChecksumType,

    /// Whether using run-length encoding
    pub is_rle: bool,
}

impl ColumnBuilderOptions {
    pub fn from_storage_options(options: &StorageOptions) -> Self {
        Self {
            target_block_size: options.target_block_size,
            checksum_type: options.checksum_type,
            is_rle: options.is_rle,
        }
    }

    #[cfg(test)]
    pub fn default_for_test() -> Self {
        Self {
            target_block_size: 4096,
            checksum_type: ChecksumType::Crc32,
            is_rle: false,
        }
    }

    #[cfg(test)]
    pub fn default_for_block_test() -> Self {
        Self {
            target_block_size: 128,
            checksum_type: ChecksumType::None,
            is_rle: false,
        }
    }

    #[cfg(test)]
    pub fn default_for_rle_block_test() -> Self {
        Self {
            target_block_size: 128,
            checksum_type: ChecksumType::None,
            is_rle: true,
        }
    }
}
