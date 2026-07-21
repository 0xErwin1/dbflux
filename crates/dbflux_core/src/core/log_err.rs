//! Extension trait for logging-and-discarding fallible results.
//!
//! The project rule forbids `let _ =` on a fallible expression. When the
//! caller genuinely wants to ignore an error but still leave a trace, this
//! trait provides `.log_err()` as the sanctioned alternative: it logs the
//! error (with the call-site location) and yields the success value as an
//! `Option`, so the dropped error is never silent.

use std::fmt::Display;
use std::panic::Location;

/// Logs the error of a `Result` before discarding it.
///
/// Intended for fire-and-forget call sites where propagation is not possible
/// but a silent drop would hide a real failure. The success value is returned
/// as `Some(_)`; an error is logged at error level and replaced with `None`.
pub trait LogErr<T> {
    /// Log the error (with caller location) and return the value as `Option`.
    fn log_err(self) -> Option<T>;

    /// Like [`LogErr::log_err`] but prefixes the log line with `context`.
    fn log_err_with(self, context: &str) -> Option<T>;
}

impl<T, E: Display> LogErr<T> for Result<T, E> {
    #[track_caller]
    fn log_err(self) -> Option<T> {
        match self {
            Ok(value) => Some(value),
            Err(error) => {
                let location = Location::caller();
                log::error!("{}:{}: {}", location.file(), location.line(), error);
                None
            }
        }
    }

    #[track_caller]
    fn log_err_with(self, context: &str) -> Option<T> {
        match self {
            Ok(value) => Some(value),
            Err(error) => {
                let location = Location::caller();
                log::error!(
                    "{}:{}: {}: {}",
                    location.file(),
                    location.line(),
                    context,
                    error
                );
                None
            }
        }
    }
}
