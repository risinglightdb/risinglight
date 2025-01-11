// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_statistics::BlockStatisticsType;

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
            "contributors" => contributors(),
            "pg_tables" => pg_tables(self.catalog),
            "pg_indexes" => pg_indexes(self.catalog),
            "pg_attribute" => pg_attribute(self.catalog),
            "pg_stat" => pg_stat(self.catalog, &*self.storage).await?,
            name => panic!("unknown system table: {:?}", name),
        };
    }
}

// TODO: find a better way to maintain the contributors list instead of hard-coding, and get total
// contributors when contributors is more than 100. (per_page max is 100)
// update this funciton with `curl https://api.github.com/repos/risinglightdb/risinglight/contributors?per_page=100 | jq ".[].login"`
fn contributors() -> DataChunk {
    let contributors = vec![
        "skyzh",
        "wangrunji0408",
        "MingjiHan99",
        "pleiadesian",
        "TennyZhuang",
        "xxchan",
        "st1page",
        "caicancai",
        "Fedomn",
        "arkbriar",
        "likg227",
        "lokax",
        "zzl200012",
        "unconsolable",
        "BaymaxHWY",
        "alissa-tung",
        "ludics",
        "Sunt-ing",
        "yinfredyue",
        "xiaoyong-z",
        "Kikkon",
        "D2Lark",
        "xzhseh",
        "ice1000",
        "kwannoel",
        "GoGim1",
        "eliasyaoyc",
        "wangqiim",
        "silver-ymz",
        "adlternative",
        "yingjunwu",
        "chaixuqing",
        "WindowsXp-Beta",
        "tabVersion",
        "SkyFan2002",
        "FANNG1",
        "XieJiann",
        "yuzi-neko",
        "xinchengxx",
        "sundy-li",
        "nanderstabel",
        "jetjinser",
        "cadl",
        "Gun9niR",
        "zehaowei",
        "rapiz1",
        "LiuYuHui",
        "Ted-Jiang",
        "Y7n05h",
        "RinChanNOWWW",
        "noneback",
        "chowc",
        "xiaguan",
        "JayiceZ",
        "danipozo",
        "PsiACE",
        "yeya24",
    ];
    [ArrayImpl::new_string(StringArray::from_iter(
        contributors.iter().map(|s| Some(*s)).sorted(),
    ))]
    .into_iter()
    .collect()
}

/// Returns `pg_indexes` table.
fn pg_indexes(catalog: RootCatalogRef) -> DataChunk {
    let mut schema_id = I32ArrayBuilder::new();
    let mut index_id = I32ArrayBuilder::new();
    let mut table_id = I32ArrayBuilder::new();
    let mut schema_name = StringArrayBuilder::new();
    let mut table_name = StringArrayBuilder::new();
    let mut index_name = StringArrayBuilder::new();
    let mut on_columns = StringArrayBuilder::new();

    for (_, schema) in catalog.all_schemas() {
        for (_, table) in schema.all_tables() {
            for index in schema.get_indexes_on_table(table.id()) {
                let index = schema.get_index_by_id(index).unwrap();
                schema_id.push(Some(&(schema.id() as i32)));
                table_id.push(Some(&(table.id() as i32)));
                index_id.push(Some(&(index.id() as i32)));
                schema_name.push(Some(&schema.name()));
                table_name.push(Some(table.name()));
                index_name.push(Some(index.name()));
                on_columns.push(Some(&format!(
                    "[{}]",
                    index
                        .column_idxs()
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                )));
            }
        }
    }
    [
        ArrayBuilderImpl::from(schema_id),
        schema_name.into(),
        ArrayBuilderImpl::from(table_id),
        table_name.into(),
        ArrayBuilderImpl::from(index_id),
        index_name.into(),
        on_columns.into(),
    ]
    .into_iter()
    .collect()
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
async fn pg_stat(catalog: RootCatalogRef, storage: &impl Storage) -> Result<DataChunk> {
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
                if table.is_view() {
                    continue;
                }
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
