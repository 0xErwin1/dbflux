//! Sandbox resource limits applied to every script run.
//!
//! Values and rationale are documented in the design: memory is 4x
//! `dbflux_lua`'s 16 MiB budget (JS holds materialised documents, Lua hooks
//! do not); stack is 2x QuickJS's own default, bounding runaway recursion;
//! the wall-clock deadline matches `run_script`'s existing 30s
//! `timeout_ms`. The interrupt only fires between bytecode ops, so it
//! cannot cut an in-flight driver call — that gap is covered at the
//! dispatch boundary instead (see `engine::run`).

use std::time::Duration;

/// Maximum heap memory a single script run may allocate.
pub const MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;

/// Maximum JS call-stack size for a single script run.
pub const MAX_STACK_SIZE_BYTES: usize = 512 * 1024;

/// Wall-clock budget for JS execution time within one script run. This is
/// JS-time only — it cannot preempt a blocking driver call already in
/// flight; the dispatcher checks cancellation before each dispatch instead.
pub const WALL_CLOCK_DEADLINE: Duration = Duration::from_secs(30);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_limit_is_four_times_the_lua_engine_budget() {
        const LUA_MEMORY_LIMIT_BYTES: usize = 16 * 1024 * 1024;
        assert_eq!(MEMORY_LIMIT_BYTES, LUA_MEMORY_LIMIT_BYTES * 4);
    }

    #[test]
    fn stack_size_is_twice_the_quickjs_default() {
        const QUICKJS_DEFAULT_STACK_SIZE_BYTES: usize = 256 * 1024;
        assert_eq!(MAX_STACK_SIZE_BYTES, QUICKJS_DEFAULT_STACK_SIZE_BYTES * 2);
    }

    #[test]
    fn wall_clock_deadline_matches_run_script_timeout() {
        assert_eq!(WALL_CLOCK_DEADLINE, Duration::from_millis(30_000));
    }
}
