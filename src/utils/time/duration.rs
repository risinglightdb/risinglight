// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

pub trait RoundingDuration {
    /// Round duration to the nearest microseconds.
    fn round_to_micros(self) -> Self;

    /// Round duration to the nearest milliseconds.
    fn round_to_millis(self) -> Self;

    /// Round duration to the nearest seconds.
    fn round_to_seconds(self) -> Self;
}

impl RoundingDuration for std::time::Duration {
    fn round_to_micros(self) -> Self {
        Duration::new(self.as_secs(), (self.subsec_nanos() + 500) / 1000 * 1000)
    }

    fn round_to_millis(self) -> Self {
        Duration::new(
            self.as_secs(),
            (self.subsec_nanos() + 500_000) / 1_000_000 * 1_000_000,
        )
    }

    fn round_to_seconds(self) -> Self {
        Duration::new(
            self.as_secs() + (self.subsec_nanos() >= 500_000_000) as u64,
            0,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rounding_duration() {
        assert_eq!(Duration::new(0, 1).round_to_micros(), Duration::new(0, 0));
        assert_eq!(Duration::new(0, 499).round_to_micros(), Duration::new(0, 0));
        assert_eq!(
            Duration::new(0, 500).round_to_micros(),
            Duration::new(0, 1_000)
        );
        assert_eq!(
            Duration::new(0, 999).round_to_micros(),
            Duration::new(0, 1_000)
        );

        assert_eq!(
            Duration::from_micros(1).round_to_millis(),
            Duration::from_micros(0)
        );
        assert_eq!(
            Duration::from_micros(499).round_to_millis(),
            Duration::from_micros(0)
        );
        assert_eq!(
            Duration::from_micros(500).round_to_millis(),
            Duration::from_micros(1_000)
        );
        assert_eq!(
            Duration::from_micros(999).round_to_millis(),
            Duration::from_micros(1_000)
        );

        assert_eq!(
            Duration::from_millis(1).round_to_seconds(),
            Duration::from_millis(0)
        );
        assert_eq!(
            Duration::from_millis(499).round_to_seconds(),
            Duration::from_millis(0)
        );
        assert_eq!(
            Duration::from_millis(500).round_to_seconds(),
            Duration::from_millis(1_000)
        );
        assert_eq!(
            Duration::from_millis(999).round_to_seconds(),
            Duration::from_millis(1_000)
        );
    }
}
