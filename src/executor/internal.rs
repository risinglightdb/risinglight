// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.
use super::*;
use crate::array::{ArrayImpl, StringArray};
use crate::catalog::{TableRefId, CONTRIBUTORS_TABLE_ID};
/// The executor of internal tables.
pub struct InternalTableExecutor {
    pub table_id: TableRefId,
}

impl InternalTableExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        match self.table_id.table_id {
            CONTRIBUTORS_TABLE_ID => {
                yield contributors();
            }
            _ => {
                panic!(
                    "InternalTableExecutor::execute: unknown table ref id: {}",
                    self.table_id
                );
            }
        }
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
    [ArrayImpl::new_string(StringArray::from_iter(
        contributors.iter().map(|s| Some(*s)).sorted(),
    ))]
    .into_iter()
    .collect()
}
