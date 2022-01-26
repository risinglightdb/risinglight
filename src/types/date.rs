// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt::{Display, Formatter};

use chrono::{Datelike, NaiveDate};
use serde::Serialize;

use crate::types::Interval;

/// The same as `NaiveDate::from_ymd(1970, 1, 1).num_days_from_ce()`.
/// Minus this magic number to store the number of days since 1970-01-01.
pub const UNIX_EPOCH_DAYS: i32 = 719_163;

/// Date type
#[derive(PartialOrd, PartialEq, Debug, Copy, Clone, Default, Hash, Serialize)]
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

impl std::ops::Add<Interval> for Date {
    type Output = Date;

    fn add(self, rhs: Interval) -> Self::Output {
        // Add days
        let days = self.0 + rhs.get_days();

        // Add years
        let date = NaiveDate::from_num_days_from_ce(days + UNIX_EPOCH_DAYS);
        let years = date.year() + rhs.get_years();

        Date::new(
            NaiveDate::from_ymd(years, date.month(), date.day()).num_days_from_ce()
                - UNIX_EPOCH_DAYS,
        )
    }
}

impl std::ops::Sub<Interval> for Date {
    type Output = Date;

    fn sub(self, rhs: Interval) -> Self::Output {
        // Add days
        let days = self.0 - rhs.get_days();

        // Add years
        let date = NaiveDate::from_num_days_from_ce(days + UNIX_EPOCH_DAYS);
        let years = date.year() - rhs.get_years();

        Date::new(
            NaiveDate::from_ymd(years, date.month(), date.day()).num_days_from_ce()
                - UNIX_EPOCH_DAYS,
        )
    }
}

impl std::ops::Mul<Interval> for Date {
    type Output = Date;

    fn mul(self, rhs: Interval) -> Self::Output {
        panic!("invalid operation: {:?} * {:?}", self, rhs)
    }
}

impl std::ops::Div<Interval> for Date {
    type Output = Date;

    fn div(self, rhs: Interval) -> Self::Output {
        panic!("invalid operation: {:?} / {:?}", self, rhs)
    }
}

impl std::ops::Rem<Interval> for Date {
    type Output = Date;

    fn rem(self, rhs: Interval) -> Self::Output {
        panic!("invalid operation: {:?} % {:?}", self, rhs)
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
