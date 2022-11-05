// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt::{Display, Formatter};
use std::iter::Sum;
use std::num::ParseIntError;
use std::ops::{Add, Neg, Sub};
use std::str::FromStr;

use serde::Serialize;

/// Interval type
#[derive(PartialOrd, Ord, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
pub struct Interval {
    months: i32,
    days: i32,
    ms: i32,
}

impl Interval {
    pub const fn from_days(days: i32) -> Self {
        Interval {
            months: 0,
            days,
            ms: 0,
        }
    }

    pub const fn from_months(months: i32) -> Self {
        Interval {
            months,
            days: 0,
            ms: 0,
        }
    }

    pub const fn from_years(years: i32) -> Self {
        Interval {
            months: years * 12,
            days: 0,
            ms: 0,
        }
    }

    pub const fn from_md(months: i32, days: i32) -> Self {
        Interval {
            months,
            days,
            ms: 0,
        }
    }

    pub const fn from_secs(seconds: i32) -> Self {
        Interval {
            months: 0,
            days: 0,
            ms: seconds * 1000,
        }
    }

    pub const fn years(&self) -> i32 {
        self.months / 12
    }

    pub const fn months(&self) -> i32 {
        self.months % 12
    }

    pub const fn days(&self) -> i32 {
        self.days
    }

    pub const fn hours(&self) -> i32 {
        self.ms / 1000 / 60 / 60
    }

    pub const fn minutes(&self) -> i32 {
        self.ms / 1000 / 60 % 60
    }

    pub const fn seconds(&self) -> i32 {
        self.ms / 1000 % 60
    }

    pub const fn num_months(&self) -> i32 {
        self.months
    }

    pub const fn is_zero(&self) -> bool {
        matches!(
            self,
            Interval {
                months: 0,
                days: 0,
                ms: 0
            }
        )
    }

    pub const fn is_positive(&self) -> bool {
        self.months >= 0 && self.days >= 0 && self.ms >= 0 && !self.is_zero()
    }
}

impl Add for Interval {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let months = self.months + rhs.months;
        let mut days = self.days + rhs.days;
        let mut ms = self.ms + rhs.ms;
        days += ms / (1000 * 60 * 60 * 24);
        ms %= 1000 * 60 * 60 * 24;
        Interval { months, days, ms }
    }
}

impl Add for &Interval {
    type Output = Interval;

    fn add(self, rhs: Self) -> Self::Output {
        (*self).add(*rhs)
    }
}

impl Sub for Interval {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        self + -rhs
    }
}

impl Neg for Interval {
    type Output = Interval;

    fn neg(self) -> Self::Output {
        Interval {
            months: -self.months,
            days: -self.days,
            ms: -self.ms,
        }
    }
}

impl Sum for Interval {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Interval::default(), |a, b| a + b)
    }
}

impl<'a> Sum<&'a Interval> for Interval {
    fn sum<I: Iterator<Item = &'a Interval>>(iter: I) -> Self {
        iter.fold(Interval::default(), |a, b| a + *b)
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut space = "";
        let mut write = |val: i32, unit: &str| {
            let res = match val {
                0 => return Ok(()),
                1 | -1 => write!(f, "{space}{val} {unit}"),
                _ => write!(f, "{space}{val} {unit}s"),
            };
            space = " ";
            res
        };
        write(self.years(), "year")?;
        write(self.months(), "month")?;
        write(self.days(), "day")?;
        write(self.hours(), "hour")?;
        write(self.minutes(), "minute")?;
        write(self.seconds(), "second")?;
        Ok(())
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ParseIntervalError {
    #[error("invalid number: {0}")]
    InvalidNum(String, ParseIntError),
    #[error("invalid unit: {0}")]
    InvalidUnit(String),
}

impl FromStr for Interval {
    type Err = ParseIntervalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut years = 0;
        let mut months = 0;
        let mut days = 0;
        let mut hours = 0;
        let mut minutes = 0;
        let mut seconds = 0;

        let mut last_val: Option<i32> = None;
        let s = s.replace('_', " "); // allow '_' as alias for space
        for token in s.trim().split_ascii_whitespace() {
            if let Some(val) = last_val {
                match token {
                    "year" | "years" => years = val,
                    "month" | "months" => months = val,
                    "day" | "days" => days = val,
                    "hour" | "hours" => hours = val,
                    "minute" | "minutes" => minutes = val,
                    "second" | "seconds" => seconds = val,
                    unit => return Err(Self::Err::InvalidUnit(unit.into())),
                }
                last_val = None;
            } else {
                let val = token
                    .parse()
                    .map_err(|e| Self::Err::InvalidNum(token.into(), e))?;
                last_val = Some(val);
            }
        }
        Ok(Interval {
            months: years * 12 + months,
            days,
            ms: ((hours * 60 + minutes) * 60 + seconds) * 1000,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse() {
        assert_eq!(
            "1 second".parse::<Interval>().unwrap(),
            Interval::from_secs(1),
        );
        assert_eq!(
            "-18 months".parse::<Interval>().unwrap(),
            Interval::from_months(-18),
        );
        assert_eq!(
            "1 year 2 months 3 days 4 hours 5 minutes 6 seconds"
                .parse::<Interval>()
                .unwrap(),
            Interval {
                months: 14,
                days: 3,
                ms: 14_706_000
            }
        );
    }

    #[test]
    fn display() {
        assert_eq!(Interval::from_secs(1).to_string(), "1 second");
        assert_eq!(Interval::from_secs(2).to_string(), "2 seconds");
        assert_eq!(Interval::from_secs(-1).to_string(), "-1 second");
        assert_eq!(
            Interval {
                months: 14,
                days: 3,
                ms: 14_706_000
            }
            .to_string(),
            "1 year 2 months 3 days 4 hours 5 minutes 6 seconds"
        );
    }
}
