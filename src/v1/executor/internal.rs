// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

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
        "wangrunji0408",
        "MingjiHan99",
        "pleiadesian",
        "TennyZhuang",
        "xxchan",
        "st1page",
        "Fedomn",
        "arkbriar",
        "likg227",
        "lokax",
        "BaymaxHWY",
        "zzl200012",
        "alissa-tung",
        "ludics",
        "shmiwy",
        "yinfredyue",
        "unconsolable",
        "xiaoyong-z",
        "Kikkon",
        "eliasyaoyc",
        "GoGim1",
        "kwannoel",
        "D2Lark",
        "tabVersion",
        "WindowsXp-Beta",
        "chaixuqing",
        "yingjunwu",
        "adlternative",
        "wangqiim",
        "yeya24",
        "PsiACE",
        "JayiceZ",
        "chowc",
        "noneback",
        "RinChanNOWWW",
        "SkyFan2002",
        "Y7n05h",
        "Ted-Jiang",
        "LiuYuHui",
        "rapiz1",
        "zehaowei",
        "Gun9niR",
        "cadl",
        "nanderstabel",
        "sundy-li",
        "xinchengxx",
        "yuzi-neko",
        "XieJiann",
    ];
    [ArrayImpl::new_utf8(Utf8Array::from_iter(
        contributors.iter().map(|s| Some(*s)).sorted(),
    ))]
    .into_iter()
    .collect()
}
