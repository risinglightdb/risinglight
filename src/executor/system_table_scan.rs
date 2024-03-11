// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use reqwest::Client;
use risinglight_proto::rowset::block_statistics::BlockStatisticsType;
use serde::Deserialize;

use super::*;
use crate::array::*;
use crate::catalog::{ColumnRefId, RootCatalogRef, TableRefId};
use crate::storage::{Storage, StorageColumnRef, Table};

/// Scan a system table.
pub struct SystemTableScan<S: Storage> {
    pub catalog: RootCatalogRef,
    pub storage: Arc<S>,
    pub table_id: TableRefId,
    pub columns: Vec<ColumnRefId>,
}

impl<S: Storage> SystemTableScan<S> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let table = self
            .catalog
            .get_table(&self.table_id)
            .expect("table not found");
        assert_eq!(self.columns.len(), table.all_columns().len());

        yield match table.name() {
            "contributors" => contributors().await?,
            "pg_tables" => pg_tables(self.catalog),
            "pg_attribute" => pg_attribute(self.catalog),
            "pg_stat" => pg_stat(self.catalog, &*self.storage).await?,
            name => panic!("unknown system table: {:?}", name),
        };
    }
}

#[allow(clippy::doc_markdown)]
/// Get contributors from GitHub.
async fn contributors() -> Result<DataChunk, ExecutorError> {
    #[derive(Deserialize)]
    struct User {
        login: String,
    }
    let client = Client::new();
    let response = client
        .get("https://api.github.com/repos/risinglightdb/risinglight/contributors?per_page=100")
        // Github API requires a User-Agent header
        .header("User-Agent", "RisingLight")
        .send()
        .await?;
    let contributors = response.json::<Vec<User>>().await?;

    Ok([ArrayImpl::new_string(StringArray::from_iter(
        contributors.iter().map(|s| Some(&s.login)),
    ))]
    .into_iter()
    .collect())
}

/// Returns `pg_tables` table.
fn pg_tables(catalog: RootCatalogRef) -> DataChunk {
    let mut schema_id = I32ArrayBuilder::new();
    let mut table_id = I32ArrayBuilder::new();
    let mut schema_name = StringArrayBuilder::new();
    let mut table_name = StringArrayBuilder::new();

    for (_, schema) in catalog.all_schemas() {
        for (_, table) in schema.all_tables() {
            schema_id.push(Some(&(schema.id() as i32)));
            table_id.push(Some(&(table.id() as i32)));
            schema_name.push(Some(&schema.name()));
            table_name.push(Some(table.name()));
        }
    }
    [
        ArrayBuilderImpl::from(schema_id),
        schema_name.into(),
        table_id.into(),
        table_name.into(),
    ]
    .into_iter()
    .collect()
}

/// Returns `pg_attribute` table.
fn pg_attribute(catalog: RootCatalogRef) -> DataChunk {
    // let mut schema_id = I32ArrayBuilder::new();
    // let mut table_id = I32ArrayBuilder::new();
    let mut schema_name = StringArrayBuilder::new();
    let mut table_name = StringArrayBuilder::new();
    let mut column_id = I32ArrayBuilder::new();
    let mut column_name = StringArrayBuilder::new();
    let mut column_type = StringArrayBuilder::new();
    let mut column_not_null = BoolArrayBuilder::new();

    for (_, schema) in catalog.all_schemas() {
        for (_, table) in schema.all_tables() {
            for (_, column) in table.all_columns() {
                let name = column.name();
                let data_type = column.data_type().to_string().to_ascii_lowercase();
                let not_null = !column.is_nullable();

                // schema_id.push(Some(&(sid as i32)));
                // table_id.push(Some(&(tid as i32)));
                schema_name.push(Some(&schema.name()));
                table_name.push(Some(table.name()));
                column_id.push(Some(&(column.id() as i32)));
                column_name.push(Some(name));
                column_type.push(Some(&data_type));
                column_not_null.push(Some(&not_null));
            }
        }
    }

    [
        ArrayBuilderImpl::from(schema_name),
        table_name.into(),
        column_id.into(),
        column_name.into(),
        column_type.into(),
        column_not_null.into(),
    ]
    .into_iter()
    .collect()
}

/// Returns `pg_stat` table.
async fn pg_stat(
    catalog: RootCatalogRef,
    storage: &impl Storage,
) -> Result<DataChunk, ExecutorError> {
    // let mut schema_id = I32ArrayBuilder::new();
    // let mut table_id = I32ArrayBuilder::new();
    // let mut column_id = I32ArrayBuilder::new();
    let mut schema_name = StringArrayBuilder::new();
    let mut table_name = StringArrayBuilder::new();
    let mut column_name = StringArrayBuilder::new();
    let mut n_row = I32ArrayBuilder::new();
    let mut n_distinct = I32ArrayBuilder::new();

    if let Some(storage) = storage.as_disk() {
        for (sid, schema) in catalog.all_schemas() {
            if sid == RootCatalog::SYSTEM_SCHEMA_ID {
                continue;
            }
            for (tid, table) in schema.all_tables() {
                let stable = storage.get_table(TableRefId::new(sid, tid))?;

                for (cid, column) in table.all_columns() {
                    let txn = stable.read().await?;
                    let values = txn.aggreagate_block_stat(&[
                        (BlockStatisticsType::RowCount, StorageColumnRef::Idx(cid)),
                        (
                            BlockStatisticsType::DistinctValue,
                            StorageColumnRef::Idx(cid),
                        ),
                    ]);
                    let row = values[0].as_usize().unwrap().unwrap() as i32;
                    let distinct = values[1].as_usize().unwrap().unwrap() as i32;

                    // schema_id.push(Some(&(sid as i32)));
                    // table_id.push(Some(&(tid as i32)));
                    // column_id.push(Some(&(cid as i32)));
                    schema_name.push(Some(&schema.name()));
                    table_name.push(Some(table.name()));
                    column_name.push(Some(column.name()));
                    n_row.push(Some(&row));
                    n_distinct.push(Some(&distinct));
                }
            }
        }
    }
    Ok(DataChunk::from_iter([
        ArrayBuilderImpl::from(schema_name),
        table_name.into(),
        column_name.into(),
        n_row.into(),
        n_distinct.into(),
    ]))
}
