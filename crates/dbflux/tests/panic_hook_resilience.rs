/// Verifies that `unwrap_or_else(|p| p.into_inner())` allows a panic hook to
/// survive a poisoned mutex without re-panicking (double-panic → abort).
///
/// The actual mutexes in `main.rs` (`AUDIT_SERVICE_FOR_PANIC`, `PREV_PANIC_HOOK`)
/// use this exact pattern. We test it here on equivalent `Mutex<Option<T>>`
/// instances because `main.rs` pulls in the full GPUI binary tree, which makes
/// inline `#[test]` expansion abort due to macro recursion depth in this env.
use std::panic;
use std::sync::Mutex;

static MOCK_AUDIT: Mutex<Option<i32>> = Mutex::new(None);
static MOCK_PREV_HOOK: Mutex<Option<i32>> = Mutex::new(None);

#[test]
fn poisoned_mutex_does_not_re_panic_with_into_inner() {
    let _ = panic::catch_unwind(|| {
        let _guard = MOCK_AUDIT.lock().unwrap();
        panic!("poison the mutex on purpose");
    });

    assert!(MOCK_AUDIT.lock().is_err(), "mutex must be poisoned now");

    let result = panic::catch_unwind(|| {
        let guard = MOCK_AUDIT
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        drop(guard);
    });

    assert!(
        result.is_ok(),
        "unwrap_or_else(|p| p.into_inner()) must not re-panic on a poisoned mutex"
    );
}

#[test]
fn poisoned_prev_hook_mutex_does_not_re_panic_with_into_inner() {
    let _ = panic::catch_unwind(|| {
        let _guard = MOCK_PREV_HOOK.lock().unwrap();
        panic!("poison the prev-hook mutex on purpose");
    });

    assert!(MOCK_PREV_HOOK.lock().is_err(), "mutex must be poisoned now");

    let result = panic::catch_unwind(|| {
        let guard = MOCK_PREV_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        drop(guard);
    });

    assert!(
        result.is_ok(),
        "unwrap_or_else(|p| p.into_inner()) must not re-panic on a poisoned mutex"
    );
}
