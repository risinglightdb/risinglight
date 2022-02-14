use std::sync::atomic::{AtomicI64, Ordering};

pub struct SealableAtomicCounter {
    value: AtomicI64,
}

impl SealableAtomicCounter {
    pub fn new() -> Self {
        Self {
            value: AtomicI64::new(0),
        }
    }
}

impl Default for SealableAtomicCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl SealableAtomicCounter {
    pub fn is_sealed(&self) -> bool {
        let v = self.value.load(Ordering::Acquire);
        v < 0
    }

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

    fn real_value(v: i64) -> i64 {
        if v >= 0 {
            v
        } else {
            -v - 1
        }
    }

    pub fn value(&self) -> i64 {
        let v = self.value.load(Ordering::Acquire);
        Self::real_value(v)
    }

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

    pub fn decrease(&self) -> i64 {
        let mut v = self.value.load(Ordering::Acquire);

        loop {
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
        let counter = SealableAtomicCounter::new();

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
