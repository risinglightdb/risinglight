use std::fmt::{Display, Formatter};
use std::str::FromStr;

use chrono::{Datelike, NaiveDate};

/// A wrapper for [`NaiveDate`]
#[derive(PartialOrd, PartialEq, Debug, Copy, Clone)]
pub struct Date(NaiveDate);

// TODO: implement customized Date type
impl Date {
    pub fn from_ymd(year: i32, month: u32, day: u32) -> Self {
        Date(NaiveDate::from_ymd(year, month, day))
    }
    pub fn from_str(s: &str) -> chrono::ParseResult<Date> {
        match NaiveDate::from_str(s) {
            Ok(d) => Ok(Date(d)),
            Err(e) => Err(e),
        }
    }
    pub fn year(&self) -> i32 {
        self.0.year()
    }
    pub fn month(&self) -> u32 {
        self.0.month()
    }
    pub fn day(&self) -> u32 {
        self.0.day()
    }
    pub const fn const_default() -> Self {
        Date(chrono::naive::MIN_DATE)
    }
}

impl Default for Date {
    fn default() -> Self {
        Date(chrono::naive::MIN_DATE)
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.0)
    }
}
