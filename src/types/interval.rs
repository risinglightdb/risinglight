// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt::{Display, Formatter};
use std::ops::Neg;

use serde::Serialize;

/// Interval type
#[derive(PartialOrd, PartialEq, Debug, Copy, Clone, Default, Hash, Eq, Serialize)]
pub struct Interval {
    months: i32,
    days: i32,
}

impl Interval {
    pub const fn from_days(days: i32) -> Self {
        Interval { months: 0, days }
    }

    pub const fn from_months(months: i32) -> Self {
        Interval { months, days: 0 }
    }

    pub const fn from_years(years: i32) -> Self {
        Interval {
            months: years * 12,
            days: 0,
        }
    }

    pub const fn from_md(months: i32, days: i32) -> Self {
        Interval { months, days }
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

    pub const fn num_months(&self) -> i32 {
        self.months
    }
}

impl Neg for Interval {
    type Output = Interval;

    fn neg(self) -> Self::Output {
        Interval {
            months: -self.months,
            days: -self.days,
        }
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} years {} months {} days",
            self.years(),
            self.months(),
            self.days()
        )
    }
}
