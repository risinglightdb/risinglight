// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

//! Basic serialization implementation of `RisingLight`.
//!
//! Note that this storage format is not stable. The current manifest persistence
//! depends on the stability of state machine of in-memory catalog. Any change in
//! catalog implementation, e.g., [`TableId`](crate::catalog::TableId) assignment, will break the
//! manifest. We will later come up with a better manifest design.

use std::io::SeekFrom;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tracing::warn;

use super::version_manager::EpochOp;
use super::{SecondaryStorage, SecondaryTable, StorageResult, TracedStorageError};
use crate::catalog::{ColumnCatalog, ColumnId, SchemaId, TableRefId};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateTableEntry {
    pub schema_id: SchemaId,
    pub table_name: String,
    pub column_descs: Vec<ColumnCatalog>,
    pub ordered_pk_ids: Vec<ColumnId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DropTableEntry {
    pub table_id: TableRefId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddRowSetEntry {
    pub table_id: TableRefId,
    pub rowset_id: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeleteRowsetEntry {
    pub table_id: TableRefId,
    pub rowset_id: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddDVEntry {
    pub table_id: TableRefId,
    pub dv_id: u64,
    pub rowset_id: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeleteDVEntry {
    pub table_id: TableRefId,
    pub dv_id: u64,
    pub rowset_id: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ManifestOperation {
    CreateTable(CreateTableEntry),
    DropTable(DropTableEntry),
    AddRowSet(AddRowSetEntry),
    DeleteRowSet(DeleteRowsetEntry),
    AddDV(AddDVEntry),
    DeleteDV(DeleteDVEntry),
    // begin transaction
    Begin,
    // end transaction
    End,
}

/// Handles all reads and writes to a manifest file
pub struct Manifest {
    file: Option<tokio::fs::File>,
    enable_fsync: bool,
}

impl Manifest {
    /// Create a mock manifest
    pub fn new_mock() -> Self {
        Self {
            file: None,
            enable_fsync: false,
        }
    }

    pub async fn open(path: impl AsRef<Path>, enable_fsync: bool) -> StorageResult<Self> {
        let file = OpenOptions::default()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())
            .await?;
        Ok(Self {
            file: Some(file),
            enable_fsync,
        })
    }

    // Reopen manifest file at `path` and seek to the end of file.
    pub async fn reopen(&mut self, path: impl AsRef<Path>) -> StorageResult<()> {
        if self.file.is_some() {
            let mut file = OpenOptions::default()
                .read(true)
                .write(true)
                .create(true)
                .open(path.as_ref())
                .await?;
            // Seek to end directly as the compacted manifest won't be replayed.
            file.seek(SeekFrom::End(0)).await?;
            self.file = Some(file);
        }

        Ok(())
    }

    pub async fn replay(&mut self) -> StorageResult<Vec<ManifestOperation>> {
        let file = if let Some(file) = &mut self.file {
            file
        } else {
            return Ok(vec![]);
        };

        let mut data = String::new();
        file.seek(SeekFrom::Start(0)).await?;
        let mut reader = BufReader::new(file);

        // TODO: don't read all to memory
        reader.read_to_string(&mut data).await?;

        let stream = Deserializer::from_str(&data).into_iter::<ManifestOperation>();

        let mut ops = vec![];
        let mut buffered_ops = vec![];
        let mut begin = false;

        for value in stream {
            let value = value?;
            match value {
                ManifestOperation::Begin => begin = true,
                ManifestOperation::End => {
                    ops.append(&mut buffered_ops);
                    begin = false;
                }
                op => {
                    if begin {
                        buffered_ops.push(op);
                    } else {
                        warn!("manifest: find entry without txn begin");
                    }
                }
            }
        }

        if !buffered_ops.is_empty() {
            warn!("manifest: find uncommitted entries");
        }

        Ok(ops)
    }

    pub async fn append(&mut self, entries: &[ManifestOperation]) -> StorageResult<()> {
        let file = if let Some(file) = &mut self.file {
            file
        } else {
            return Ok(());
        };

        let mut json = Vec::new();
        serde_json::to_writer(&mut json, &ManifestOperation::Begin)?;
        for entry in entries {
            serde_json::to_writer(&mut json, entry)?;
        }
        serde_json::to_writer(&mut json, &ManifestOperation::End)?;
        file.write_all(&json).await?;
        if self.enable_fsync {
            file.sync_data().await?;
        }
        Ok(())
    }
}

impl SecondaryStorage {
    pub(super) fn apply_create_table(&self, entry: &CreateTableEntry) -> StorageResult<()> {
        let CreateTableEntry {
            schema_id,
            table_name,
            column_descs,
            ordered_pk_ids,
        } = entry.clone();

        let schema = self
            .catalog
            .get_schema_by_id(schema_id)
            .ok_or_else(|| TracedStorageError::not_found("schema", schema_id))?;
        if schema.get_table_by_name(&table_name).is_some() {
            return Err(TracedStorageError::duplicated("table", table_name));
        }
        let table_id = self
            .catalog
            .add_table(
                schema_id,
                table_name.clone(),
                column_descs.to_vec(),
                ordered_pk_ids,
            )
            .map_err(|_| TracedStorageError::duplicated("table", table_name))?;

        let id = TableRefId {
            schema_id,
            table_id,
        };
        let table = SecondaryTable::new(
            self.options.clone(),
            id,
            &column_descs,
            self.next_id.clone(),
            self.version.clone(),
            self.block_cache.clone(),
            self.txn_mgr.clone(),
        );
        self.tables.write().insert(id, table);

        Ok(())
    }

    pub(super) async fn create_table_inner(
        &self,
        schema_id: SchemaId,
        table_name: &str,
        column_descs: &[ColumnCatalog],
        ordered_pk_ids: &[ColumnId],
    ) -> StorageResult<()> {
        let entry = CreateTableEntry {
            schema_id,
            table_name: table_name.to_string(),
            column_descs: column_descs.to_vec(),
            ordered_pk_ids: ordered_pk_ids.to_vec(),
        };

        // persist to manifest first
        self.version
            .commit_changes(vec![EpochOp::CreateTable(entry.clone())])
            .await?;

        // then apply to catalog
        self.apply_create_table(&entry)?;

        Ok(())
    }

    pub(super) fn get_table_inner(&self, table_id: TableRefId) -> StorageResult<SecondaryTable> {
        let table = self
            .tables
            .read()
            .get(&table_id)
            .ok_or_else(|| TracedStorageError::not_found("table", table_id.table_id))?
            .clone();
        Ok(table)
    }

    pub(super) fn apply_drop_table(&self, entry: &DropTableEntry) -> StorageResult<()> {
        let DropTableEntry { table_id } = entry.clone();

        self.tables
            .write()
            .remove(&table_id)
            .ok_or_else(|| TracedStorageError::not_found("table", table_id.table_id))?;
        self.catalog.drop_table(table_id);

        Ok(())
    }

    pub(super) async fn drop_table_inner(&self, table_id: TableRefId) -> StorageResult<()> {
        let mut changeset = vec![];

        let entry = DropTableEntry { table_id };

        // contrary to create table, we first modify the catalog
        self.apply_drop_table(&entry)?;

        changeset.push(EpochOp::DropTable(entry));

        let pin_version = self.version.pin();

        if let Some(rowsets) = pin_version.snapshot.get_rowsets_of(table_id.table_id) {
            for rowset_id in rowsets {
                changeset.push(EpochOp::DeleteRowSet(DeleteRowsetEntry {
                    table_id,
                    rowset_id: *rowset_id,
                }));

                if let Some(dvs) = pin_version
                    .snapshot
                    .get_dvs_of(table_id.table_id, *rowset_id)
                {
                    for dv_id in dvs {
                        changeset.push(EpochOp::DeleteDV(DeleteDVEntry {
                            table_id,
                            dv_id: *dv_id,
                            rowset_id: *rowset_id,
                        }));
                    }
                }
            }
        }

        // and then persist to manifest
        self.version.commit_changes(changeset).await?;

        Ok(())
    }
}
