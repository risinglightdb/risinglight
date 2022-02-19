// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicI64, Ordering};

/// `SealableAtomicCounter` is an atomic counter supports increasing and decreasing atomically,
/// and supports atomically sealing the counter. After it's sealed, only decreasing is allowed
/// and increase returns None. It uses \[0, +inf) (before sealed) and (-inf, -1\] (after
/// sealed) as the range of the counter value.
///
/// `The SealableAtomicCounter` is useful to implement concurrent wait group that supports the
/// atomic shutdown. Please refer to `risinglight::utils::sync::WaitGroup` for details.
#[derive(Default)]
pub struct SealableAtomicCounter {
    /// Value of the counter. It's in [0, +inf) when not sealed.
    /// After sealed, the value is mapped to (-inf, -1] and always be negative.
    value: AtomicI64,
}

impl SealableAtomicCounter {
    /// Indicates whether the counter has been sealed.
    pub fn is_sealed(&self) -> bool {
        let v = self.value.load(Ordering::Acquire);
        v < 0
    }

    /// Seal the counter atomically. Returns true if it's sealed for the first time.
    pub fn seal(&self) -> bool {
        let mut v = self.value.load(Ordering::Acquire);
        while v >= 0 {
            match self
                .value
                .compare_exchange(v, -v - 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(cur) => v = cur,
            }
        }
        false
    }

    /// Get the real counter value from the raw value, whether it's sealed or not.
    fn real_value(v: i64) -> i64 {
        if v >= 0 {
            v
        } else {
            -v - 1
        }
    }

    /// Get the counter value (always non-negative) atomically.
    pub fn value(&self) -> i64 {
        let v = self.value.load(Ordering::Acquire);
        Self::real_value(v)
    }

    /// Increase the counter atomically and returns the value before.
    /// If the counter is sealed, do nothing and return None instead.
    pub fn increase(&self) -> Option<i64> {
        let mut v = self.value.load(Ordering::Acquire);
        while v >= 0 {
            match self
                .value
                .compare_exchange(v, v + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Some(v),
                Err(cur) => v = cur,
            }
        }
        None
    }

    /// Decrease the counter atomically and returns the value before.
    /// Panics if decrease a counter with value 0.
    pub fn decrease(&self) -> i64 {
        let mut v = self.value.load(Ordering::Acquire);

        loop {
            // 0 / -1 is the zero-value before/after sealed.
            if v == 0 || v == -1 {
                panic!("decrease too much")
            }

            let next_v = if v > 0 { v - 1 } else { v + 1 };

            match self
                .value
                .compare_exchange(v, next_v, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Self::real_value(v),
                Err(cur) => v = cur,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sealable_counter() {
        let counter = SealableAtomicCounter::default();

        assert_eq!(Some(0), counter.increase());
        assert_eq!(Some(1), counter.increase());

        assert_eq!(2, counter.value());

        assert!(counter.seal());
        assert!(counter.is_sealed());
        assert!(!counter.seal());
        assert!(counter.is_sealed());

        assert_eq!(None, counter.increase());

        assert_eq!(2, counter.value());

        assert_eq!(2, counter.decrease());
        assert_eq!(1, counter.decrease());
    }
}
