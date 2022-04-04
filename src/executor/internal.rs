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
// update this funciton with `curl https://api.github.com/repos/risinglightdb/risinglight/contributors | jq ".[].login"`
fn contributors() -> DataChunk {
    let contributors = vec![
        "skyzh",
        "MingjiHan99",
        "wangrunji0408",
        "pleiadesian",
        "TennyZhuang",
        "st1page",
        "xxchan",
        "arkbriar",
        "Fedomn",
        "likg227",
        "zzl200012",
        "BaymaxHWY",
        "ludics",
        "alissa-tung",
        "Sunt-ing",
        "xiaoyong-z",
        "kwannoel",
        "WindowsXp-Beta",
        "tabVersion",
        "yingjunwu",
        "PsiACE",
        "D2Lark",
        "RinChanNOWWW",
        "Y7n05h",
        "LiuYuHui",
        "rapiz1",
        "zehaowei",
        "adlternative",
        "nanderstabel",
        "sundy-li",
    ];
    [ArrayImpl::new_utf8(Utf8Array::from_iter(
        contributors.iter().map(|s| Some(*s)).sorted(),
    ))]
    .into_iter()
    .collect()
}
