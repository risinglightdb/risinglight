use std::fmt::{Display, Formatter};

use chrono::{Datelike, NaiveDate};

/// The same as `NaiveDate::from_ymd(1970, 1, 1).num_days_from_ce()`.
/// Minus this magic number to store the number of days since 1970-01-01.
pub const UNIX_EPOCH_DAYS: i32 = 719_163;

/// Date type
#[derive(PartialOrd, PartialEq, Debug, Copy, Clone, Default)]
pub struct Date(i32);

impl Date {
    pub const fn new(inner: i32) -> Self {
        Date(inner)
    }

    /// Convert string to date
    pub fn from_str(s: &str) -> chrono::ParseResult<Date> {
        NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map(|ret| Date(ret.num_days_from_ce() - UNIX_EPOCH_DAYS))
    }

    /// Get the inner value of date type
    pub fn get_inner(&self) -> i32 {
        self.0
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            NaiveDate::from_num_days_from_ce_opt(self.0 + UNIX_EPOCH_DAYS)
                .unwrap()
                .format("%Y-%m-%d")
        )
    }
}
