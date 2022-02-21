// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::{ArrayImpl, Utf8Array};

/// The executor of internal tables.
pub struct InternalTableExecutor {
    pub table_name: String,
}

impl InternalTableExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        match self.table_name.as_ref() {
            "contributors" => {
                yield contributors();
            }
            _ => {
                panic!(
                    "InternalTableExecutor::execute: unknown table name: {}",
                    self.table_name
                );
            }
        }
    }
}

// TODO: find a better way to maintain the contributors list instead of hard-coding.
fn contributors() -> DataChunk {
    let contributors = vec![
        "skyzh",
        "MingjiHan99",
        "wangrunji0408",
        "pleiadesian",
        "TennyZhuang",
        "st1page",
        "likg227",
        "xxchan",
        "arkbriar",
        "Fedomn",
        "zzl200012",
        "Sunt-ing",
        "alissa-tung",
        "ludics",
        "tabVersion",
        "yingjunwu",
        "xiaoyong-z",
        "PsiACE",
        "LiuYuHui",
        "rapiz1",
        "zehaowei",
        "nanderstabel",
    ];
    [ArrayImpl::Utf8(Utf8Array::from_iter(
        contributors.iter().map(|s| Some(*s)),
    ))]
    .into_iter()
    .collect()
}
