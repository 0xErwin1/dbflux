use crate::DbError;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

/// Runs a blocking, non-cancellable call on its own thread and gives up after
/// `timeout`.
///
/// For calls into libraries that offer neither an async API nor a timeout, but
/// that can block on a resource which never answers — the platform keyring, for
/// one. Without a bound such a call holds the calling thread (in the app, often
/// the GPUI main thread) forever.
///
/// A worker that overruns the bound is abandoned rather than joined: the call
/// cannot be cancelled, and reporting the timeout is what keeps the caller
/// responsive. The abandoned thread finishes on its own once the underlying
/// call returns (if it ever does).
#[allow(clippy::result_large_err)]
pub(crate) fn run_with_timeout<T, Call>(
    label: &str,
    timeout: Duration,
    call: Call,
) -> Result<T, DbError>
where
    T: Send + 'static,
    Call: FnOnce() -> Result<T, DbError> + Send + 'static,
{
    let (sender, receiver) = mpsc::channel();

    std::thread::Builder::new()
        .name("dbflux-blocking".to_string())
        .spawn(move || {
            if sender.send(call()).is_err() {
                // The caller gave up on this call before it finished; the result
                // has nowhere to go. Nothing is lost: the caller already reported
                // the timeout.
            }
        })
        .map_err(|e| DbError::io_message(format!("failed to start a blocking worker: {e}")))?;

    match receiver.recv_timeout(timeout) {
        Ok(result) => result,
        Err(RecvTimeoutError::Timeout) => {
            log::warn!(
                "Blocking call '{label}' did not answer within {timeout:?}; giving up on it"
            );
            Err(DbError::Timeout)
        }
        Err(RecvTimeoutError::Disconnected) => Err(DbError::io_message(format!(
            "blocking call '{label}' ended without a result"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn run_with_timeout_returns_the_call_result() {
        let result = run_with_timeout("test", Duration::from_secs(5), || Ok(7_u32))
            .expect("an answering call returns its result");
        assert_eq!(result, 7);
    }

    #[test]
    fn run_with_timeout_propagates_the_call_error() {
        let result: Result<(), DbError> = run_with_timeout("test", Duration::from_secs(5), || {
            Err(DbError::NotSupported("no backend".to_string()))
        });
        assert!(matches!(result, Err(DbError::NotSupported(_))));
    }

    /// A call that never answers must not hold the caller: before the timeout
    /// existed, this blocked the calling thread forever.
    #[test]
    fn run_with_timeout_gives_up_on_a_hung_call() {
        let started = Instant::now();
        let result = run_with_timeout("test", Duration::from_millis(50), || {
            std::thread::sleep(Duration::from_secs(30));
            Ok(())
        });

        assert!(
            matches!(result, Err(DbError::Timeout)),
            "a hung call reports a timeout, got {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the caller is released long before the hung call returns"
        );
    }
}
