//! The toast throttle moved to `bolt::toast`, where the bucket became
//! generic in capacity and refill interval with the toast policy as the
//! caller that fixes the values. Re-exported at the old path so
//! `user_error::throttle::TokenBucket` keeps resolving; the bucket's test
//! suite moved with it.

pub use bolt::toast::{BUCKET_CAPACITY, REFILL_INTERVAL, TokenBucket};
