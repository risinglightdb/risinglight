use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::OnceLock;

use chrono::{DateTime, Datelike, FixedOffset, NaiveDateTime, Timelike, Utc};
use serde::Serialize;

/// unix timestamp counts from 1970-01-01 00:00:00,
///
/// postgres timestamp counts from 2000-01-01 00:00:00,
///
/// this is the difference between them
const THIRTY_YEARS_MICROSECONDS: i64 = 946_684_800_000_000;

/// global timezone
static TIME_ZONE: OnceLock<FixedOffset> = OnceLock::new();

/// input format without timezone
const TIMESTAMP_FORMATS: [&str; 3] = [
    "%Y-%m-%d %H:%M:%S",    // 1991-01-08 04:05:06
    "%Y-%m-%d %H:%M:%S AD", // 1991-01-08 04:05:06 AD
    "%Y-%m-%d %H:%M:%S BC", // 1991-01-08 04:05:06 BC
];
/// input format with timezone
const TIMESTAMP_TZ_FORMATS: [&str; 5] = [
    "%Y-%m-%d %H:%M:%S %z",    // 1991-01-08 04:05:06 +08:00
    "%Y-%m-%d %H:%M:%S %z AD", // 1991-01-08 04:05:06 +08:00 AD
    "%Y-%m-%d %H:%M:%S %z BC", // 1991-01-08 04:05:06 +08:00 BC
    "%Y-%m-%d %H:%M:%S AD %z", // 1991-01-08 04:05:06 AD +08:00
    "%Y-%m-%d %H:%M:%S BC %z", // 1991-01-08 04:05:06 BC +08:00
];

#[derive(PartialOrd, Ord, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
pub struct Timestamp(i64);

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ParseTimestampError {
    #[error("invalid string: {0}")]
    InvalidString(String),
    #[error("invalid year: {0}")]
    InvalidYear(i32),
}

impl Timestamp {
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    pub fn get_inner(&self) -> i64 {
        self.0
    }
}

impl Display for Timestamp {
    /// ISO 8601 format: `YYYY-MM-DD HH:MM:SS`
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let dt = NaiveDateTime::from_timestamp_millis((self.0 - THIRTY_YEARS_MICROSECONDS) / 1000)
            .ok_or(std::fmt::Error)?;
        dt_fmt(&dt, f)
    }
}

impl FromStr for Timestamp {
    type Err = ParseTimestampError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        for fmt in TIMESTAMP_FORMATS.iter().chain(TIMESTAMP_TZ_FORMATS.iter()) {
            // like postgresql,silently ignore timezone
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
                return dt_to_timestamp(&dt, &FixedOffset::east_opt(0).unwrap(), s.contains("BC"))
                    .map(Self);
            }
        }
        Err(ParseTimestampError::InvalidString(s.to_string()))
    }
}

#[derive(PartialOrd, Ord, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
pub struct TimestampTz(i64);

impl TimestampTz {
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    pub fn get_inner(&self) -> i64 {
        self.0
    }
}

impl Display for TimestampTz {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let dt = NaiveDateTime::from_timestamp_millis((self.0 - THIRTY_YEARS_MICROSECONDS) / 1000)
            .ok_or(std::fmt::Error)?;
        let sys_tz = TIME_ZONE.get_or_init(|| FixedOffset::east_opt(0).unwrap());
        let dt = DateTime::<FixedOffset>::from_utc(dt, *sys_tz)
            .with_timezone(&Utc)
            .naive_local();
        dt_fmt(&dt, f)?;
        write!(f, " {}", sys_tz)
    }
}

impl FromStr for TimestampTz {
    type Err = ParseTimestampError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let sys_tz = TIME_ZONE.get_or_init(|| FixedOffset::east_opt(0).unwrap());
        for fmt in TIMESTAMP_FORMATS {
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
                return dt_to_timestamp(&dt, sys_tz, s.contains("BC")).map(Self);
            }
        }
        for fmt in TIMESTAMP_TZ_FORMATS {
            if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
                let dt = dt.naive_utc();
                return dt_to_timestamp(&dt, sys_tz, s.contains("BC")).map(Self);
            }
        }
        Err(ParseTimestampError::InvalidString(s.to_string()))
    }
}

fn dt_fmt(dt: &NaiveDateTime, f: &mut Formatter<'_>) -> std::fmt::Result {
    if dt.year() < 0 {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02} BC",
            -dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second()
        )
    } else {
        write!(f, "{}", dt)
    }
}

fn dt_to_timestamp(
    dt: &NaiveDateTime,
    tz: &FixedOffset,
    is_bc: bool,
) -> Result<i64, ParseTimestampError> {
    let mut dt = DateTime::<FixedOffset>::from_utc(*dt, *tz)
        .with_timezone(&Utc)
        .naive_local();
    if is_bc {
        let new_date = dt
            .date()
            .with_year(-dt.year())
            .ok_or_else(|| ParseTimestampError::InvalidYear(-dt.year()))?;
        dt = NaiveDateTime::new(new_date, dt.time());
    }
    Ok(dt.timestamp_micros() + THIRTY_YEARS_MICROSECONDS)
}
