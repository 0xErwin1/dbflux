use std::time::{Duration, Instant};

pub const BUCKET_CAPACITY: u8 = 5;
pub const REFILL_INTERVAL: Duration = Duration::from_secs(2);

/// Token-bucket rate limiter for `Warning`/`Info` toasts.
///
/// The bucket starts full. Each `try_consume` call attempts to take one token.
/// Tokens refill based on elapsed time since the last refill, up to the
/// `BUCKET_CAPACITY` cap. The algorithm is O(1) per call.
///
/// `Error` and `Fatal` toasts bypass this entirely — callers skip
/// `try_consume` for those severities.
pub struct TokenBucket {
    tokens: u8,
    last_refill: Instant,
    now_fn: fn() -> Instant,
}

impl TokenBucket {
    pub fn new() -> Self {
        Self::with_clock(Instant::now)
    }

    /// Constructs a bucket with an injectable clock — use this in tests for
    /// deterministic time control.
    pub fn with_clock(now_fn: fn() -> Instant) -> Self {
        Self {
            tokens: BUCKET_CAPACITY,
            last_refill: (now_fn)(),
            now_fn,
        }
    }

    /// Returns `true` if the caller may proceed (a token was consumed).
    /// Returns `false` when the bucket was empty after attempting a refill.
    pub fn try_consume(&mut self) -> bool {
        self.refill();
        if self.tokens == 0 {
            return false;
        }
        self.tokens -= 1;
        true
    }

    fn refill(&mut self) {
        let now = (self.now_fn)();
        let elapsed = now.duration_since(self.last_refill);
        let refill_count =
            (elapsed.as_millis() / REFILL_INTERVAL.as_millis()).min(u8::MAX as u128) as u8;
        if refill_count > 0 {
            self.tokens = self.tokens.saturating_add(refill_count).min(BUCKET_CAPACITY);
            self.last_refill = now;
        }
    }
}

impl Default for TokenBucket {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    thread_local! {
        static FAKE_NOW: Cell<Option<Instant>> = const { Cell::new(None) };
    }

    fn fake_clock() -> Instant {
        FAKE_NOW.with(|c| c.get().expect("fake clock not initialised"))
    }

    fn set_fake_now(t: Instant) {
        FAKE_NOW.with(|c| c.set(Some(t)));
    }

    #[test]
    fn five_consecutive_consumes_succeed() {
        let base = Instant::now();
        set_fake_now(base);
        let mut bucket = TokenBucket::with_clock(fake_clock);

        for i in 1..=5 {
            assert!(bucket.try_consume(), "consume {i} should succeed");
        }
    }

    #[test]
    fn sixth_consume_fails_immediately() {
        let base = Instant::now();
        set_fake_now(base);
        let mut bucket = TokenBucket::with_clock(fake_clock);

        for _ in 0..5 {
            bucket.try_consume();
        }

        assert!(!bucket.try_consume(), "sixth consume must fail when bucket is empty");
    }

    #[test]
    fn one_token_refills_after_refill_interval() {
        let base = Instant::now();
        set_fake_now(base);
        let mut bucket = TokenBucket::with_clock(fake_clock);

        // Drain the bucket.
        for _ in 0..5 {
            bucket.try_consume();
        }
        assert!(!bucket.try_consume());

        // Advance past one refill interval.
        set_fake_now(base + REFILL_INTERVAL + Duration::from_millis(1));
        assert!(bucket.try_consume(), "one refilled token should allow one more consume");
    }

    #[test]
    fn bucket_caps_at_capacity_after_long_idle() {
        let base = Instant::now();
        set_fake_now(base);
        let mut bucket = TokenBucket::with_clock(fake_clock);

        // Drain completely.
        for _ in 0..5 {
            bucket.try_consume();
        }

        // Advance by 60 s — far beyond 5 * REFILL_INTERVAL.
        set_fake_now(base + Duration::from_secs(60));

        // Must succeed exactly BUCKET_CAPACITY times, then fail.
        for i in 1..=BUCKET_CAPACITY {
            assert!(bucket.try_consume(), "consume {i} after refill must succeed");
        }
        assert!(!bucket.try_consume(), "capacity cap: no more than BUCKET_CAPACITY tokens");
    }
}
