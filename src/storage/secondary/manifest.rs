//! Basic serialization implementation of `RisingLight`.
//!
//! Note that this storage format is not stable. The current manifest persistence
//! depends on the stability of state machine of in-memory catalog. Any change in
//! catalog implementation, e.g., TableId assignment, will break the manifest. We will
//! later come up with a better manifest design.

use std::io::SeekFrom;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};

use crate::catalog::{ColumnCatalog, TableRefId};
use crate::types::{DatabaseId, SchemaId};

use super::{SecondaryStorage, SecondaryTable, StorageError, StorageResult};

#[derive(Clone, Serialize, Deserialize)]
pub struct CreateTableEntry {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_name: String,
    pub column_descs: Vec<ColumnCatalog>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DropTableEntry {
    pub table_id: TableRefId,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AddRowSetEntry {
    pub table_id: TableRefId,
    pub rowset_id: u32,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ManifestOperation {
    CreateTable(CreateTableEntry),
    DropTable(DropTableEntry),
    AddRowSet(AddRowSetEntry),
}

/// Handles all reads and writes to a manifest file
pub struct Manifest {
    file: tokio::fs::File,
}

impl Manifest {
    pub async fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        let file = OpenOptions::default()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())
            .await?;
        Ok(Self { file })
    }

    pub async fn replay(&mut self) -> StorageResult<Vec<ManifestOperation>> {
        let mut data = String::new();
        self.file.seek(SeekFrom::Start(0)).await?;
        let mut reader = BufReader::new(&mut self.file);

        // TODO: don't read all to memory
        reader.read_to_string(&mut data).await?;

        let stream = Deserializer::from_str(&data).into_iter::<ManifestOperation>();

        let mut ops = vec![];

        for value in stream {
            let value = value?;
            ops.push(value);
        }

        Ok(ops)
    }

    pub async fn append(&mut self, entry: ManifestOperation) -> StorageResult<()> {
        let json = serde_json::to_string(&entry)?;
        self.file.write_all(json.as_bytes()).await?;
        self.file.sync_data().await?;
        Ok(())
    }
}

impl SecondaryStorage {
    pub(super) fn apply_create_table(&self, entry: &CreateTableEntry) -> StorageResult<()> {
        let CreateTableEntry {
            database_id,
            schema_id,
            table_name,
            column_descs,
        } = entry.clone();

        let db = self
            .catalog
            .get_database_by_id(database_id)
            .ok_or(StorageError::NotFound("database", database_id))?;
        let schema = db
            .get_schema_by_id(schema_id)
            .ok_or(StorageError::NotFound("schema", schema_id))?;
        if schema.get_table_by_name(&table_name).is_some() {
            return Err(StorageError::Duplicated("table", table_name.clone()));
        }
        let table_id = schema
            .add_table(table_name.clone(), column_descs.to_vec(), false)
            .map_err(|_| StorageError::Duplicated("table", table_name.clone()))?;

        let id = TableRefId {
            database_id,
            schema_id,
            table_id,
        };
        let table = SecondaryTable::new(
            self.options.clone(),
            id,
            &column_descs,
            self.block_cache.clone(),
            self.next_rowset_id.clone(),
            self.manifest.clone(),
        );
        self.tables.write().insert(id, table);

        Ok(())
    }

    pub async fn create_table_inner(
        &self,
        database_id: DatabaseId,
        schema_id: SchemaId,
        table_name: &str,
        column_descs: &[ColumnCatalog],
    ) -> StorageResult<()> {
        let entry = CreateTableEntry {
            database_id,
            schema_id,
            table_name: table_name.to_string(),
            column_descs: column_descs.to_vec(),
        };

        let mut manifest = self.manifest.lock().await;

        self.apply_create_table(&entry)?;

        manifest
            .append(ManifestOperation::CreateTable(entry))
            .await?;

        Ok(())
    }

    pub fn get_table_inner(&self, table_id: TableRefId) -> StorageResult<SecondaryTable> {
        let table = self
            .tables
            .read()
            .get(&table_id)
            .ok_or(StorageError::NotFound("table", table_id.table_id))?
            .clone();
        Ok(table)
    }

    pub(super) fn apply_drop_table(&self, entry: &DropTableEntry) -> StorageResult<()> {
        let DropTableEntry { table_id } = entry.clone();

        self.tables
            .write()
            .remove(&table_id)
            .ok_or(StorageError::NotFound("table", table_id.table_id))?;

        let db = self
            .catalog
            .get_database_by_id(table_id.database_id)
            .unwrap();
        let schema = db.get_schema_by_id(table_id.schema_id).unwrap();
        schema.delete_table(table_id.table_id);

        Ok(())
    }

    pub async fn drop_table_inner(&self, table_id: TableRefId) -> StorageResult<()> {
        let entry = DropTableEntry { table_id };

        let mut manifest = self.manifest.lock().await;

        self.apply_drop_table(&entry)?;

        manifest.append(ManifestOperation::DropTable(entry)).await?;

        Ok(())
    }
}
