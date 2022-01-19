// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt::{Display, Formatter};

/// Interval type
#[derive(PartialOrd, PartialEq, Debug, Copy, Clone, Default, Hash)]
pub struct Interval {
    years: i32,
    days: i32,
}

impl Interval {
    pub const fn new(years: i32, days: i32) -> Self {
        Interval { years, days }
    }

    pub fn get_years(&self) -> i32 {
        self.years
    }

    pub fn get_days(&self) -> i32 {
        self.days
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} years {} days", self.years, self.days)
    }
}
