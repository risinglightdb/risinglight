// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use chrono::{Datelike, NaiveDate};
use serde::Serialize;

use crate::types::Interval;

/// The same as `NaiveDate::from_ymd(1970, 1, 1).num_days_from_ce()`.
/// Minus this magic number to store the number of days since 1970-01-01.
pub const UNIX_EPOCH_DAYS: i32 = 719_163;

/// Date type
#[derive(PartialOrd, Ord, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
pub struct Date(i32);

impl Date {
    pub const fn new(inner: i32) -> Self {
        Date(inner)
    }

    /// Get the inner value of date type
    pub fn get_inner(&self) -> i32 {
        self.0
    }
}

pub type ParseDateError = chrono::ParseError;

impl FromStr for Date {
    type Err = chrono::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map(|ret| Date(ret.num_days_from_ce() - UNIX_EPOCH_DAYS))
    }
}

impl std::ops::Add<Interval> for Date {
    type Output = Date;

    fn add(self, rhs: Interval) -> Self::Output {
        // Add days
        let days = self.0 + rhs.days();
        let date = NaiveDate::from_num_days_from_ce_opt(days + UNIX_EPOCH_DAYS).unwrap();

        // Add months and years
        let mut day = date.day();
        let mut month = date.month() as i32 + rhs.months();
        let mut year = date.year() + rhs.years();
        if month > 12 {
            month -= 12;
            year += 1;
        } else if month <= 0 {
            month += 12;
            year -= 1;
        }
        assert!((1..=12).contains(&month));

        // Fix the days after changing date.
        // For example, 1970.1.31 + 1 month = 1970.2.28
        day = day.min(get_month_days(year, month as usize));

        Date::new(
            NaiveDate::from_ymd_opt(year, month as u32, day)
                .unwrap()
                .num_days_from_ce()
                - UNIX_EPOCH_DAYS,
        )
    }
}

/// return the days of the `year-month`
const fn get_month_days(year: i32, month: usize) -> u32 {
    const fn is_leap_year(year: i32) -> bool {
        year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
    }
    const LEAP_DAYS: &[u32] = &[0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    const NORMAL_DAYS: &[u32] = &[0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    if is_leap_year(year) {
        LEAP_DAYS[month]
    } else {
        NORMAL_DAYS[month]
    }
}

impl std::ops::Sub<Interval> for Date {
    type Output = Date;

    fn sub(self, rhs: Interval) -> Self::Output {
        self + (-rhs)
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
