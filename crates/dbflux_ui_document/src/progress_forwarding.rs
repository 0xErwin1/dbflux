//! Forwards a background run's progress into the Tasks panel.
//!
//! Long-running document flows (export, import, migration, dump analysis)
//! publish their progress into a shared counter from the background worker,
//! and the foreground polls that counter on a fixed tick to update the
//! matching `TaskManager` entry.

use std::time::Duration;

use dbflux_core::{TaskId, TaskStatus};
use dbflux_ui_base::AppStateEntity;
use gpui::{Context, Entity};

pub(crate) const PROGRESS_TICK: Duration = Duration::from_millis(150);

/// Computes the fraction (0.0–1.0) of a run done so far, for the task-manager
/// progress bar. Returns `None` when the total is unknown or zero, in which
/// case progress stays indeterminate rather than showing a misleading bar.
pub(crate) fn progress_fraction(done: u64, total: Option<u64>) -> Option<f32> {
    let total = total.filter(|&total| total > 0)?;
    Some((done as f32 / total as f32).clamp(0.0, 1.0))
}

/// Spawns a detached loop that, every [`PROGRESS_TICK`], forwards the
/// `(done, total)` pair returned by `read_progress` to the task `task_id` and
/// then re-renders the calling entity, so its own counters advance too (the
/// app-state notify only refreshes the Tasks panel).
///
/// The fraction is written only when the total is known. The loop stops on
/// the first tick that finds the task removed or no longer `Running`, which
/// covers completion, failure and cancellation. It keeps running when the
/// calling entity is dropped, until the task itself leaves `Running`.
pub(crate) fn spawn_task_progress_forwarder<T: 'static>(
    app_state: Entity<AppStateEntity>,
    task_id: TaskId,
    read_progress: impl Fn() -> (u64, Option<u64>) + 'static,
    cx: &mut Context<T>,
) {
    cx.spawn(async move |this, cx| {
        loop {
            cx.background_executor().timer(PROGRESS_TICK).await;

            let still_running = cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    let Some(snapshot) = state.tasks().get(task_id) else {
                        return false;
                    };
                    if snapshot.status != TaskStatus::Running {
                        return false;
                    }

                    let (done, total) = read_progress();
                    if let Some(fraction) = progress_fraction(done, total) {
                        state.tasks_mut().update_progress(task_id, fraction);
                        cx.notify();
                    }

                    true
                })
            });

            if !still_running {
                break;
            }

            this.update(cx, |_this, cx| cx.notify()).ok();
        }
    })
    .detach();
}

#[cfg(test)]
mod tests {
    // Import only what we need — avoid `use super::*` which pulls in
    // `gpui::*` and triggers macro recursion (see `task_runner.rs`).
    use super::{PROGRESS_TICK, progress_fraction, spawn_task_progress_forwarder};

    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    use dbflux_core::{TaskId, TaskKind};
    use dbflux_ui_base::AppStateEntity;
    use gpui::{AppContext as _, Entity, TestAppContext};

    #[test]
    fn progress_fraction_is_none_when_total_unknown() {
        assert_eq!(progress_fraction(1024, None), None);
    }

    #[test]
    fn progress_fraction_is_none_when_total_is_zero() {
        assert_eq!(progress_fraction(0, Some(0)), None);
    }

    #[test]
    fn progress_fraction_computes_ratio_when_total_known() {
        assert_eq!(progress_fraction(50, Some(200)), Some(0.25));
    }

    #[test]
    fn progress_fraction_clamps_above_one() {
        // The analyzer's progress callback may briefly overshoot the
        // reported total (e.g. trailing checksum bytes); the fraction must
        // never exceed 1.0.
        assert_eq!(progress_fraction(300, Some(200)), Some(1.0));
    }

    struct Host;

    struct Harness {
        app_state: Entity<AppStateEntity>,
        task_id: TaskId,
        progress: Arc<Mutex<(u64, Option<u64>)>>,
        reads: Rc<Cell<usize>>,
        host_renders: Rc<Cell<usize>>,
        _host: Entity<Host>,
        _subscription: gpui::Subscription,
    }

    fn start_forwarding(cx: &mut TestAppContext) -> Harness {
        let app_state = cx.update(|cx| {
            cx.new(|_| {
                let storage_runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("isolated storage runtime");
                AppStateEntity::new_with_storage_runtime(storage_runtime)
                    .expect("test storage setup")
            })
        });

        let (task_id, _cancel_token) = app_state.update(cx, |state, _cx| {
            state.start_task(TaskKind::Export, "export rows")
        });

        let progress = Arc::new(Mutex::new((0u64, None::<u64>)));
        let reads = Rc::new(Cell::new(0usize));
        let host_renders = Rc::new(Cell::new(0usize));

        let host = cx.update(|cx| cx.new(|_| Host));

        let subscription = cx.update(|cx| {
            let host_renders = Rc::clone(&host_renders);
            cx.observe(&host, move |_host, _cx| {
                host_renders.set(host_renders.get() + 1);
            })
        });

        host.update(cx, |_host, cx| {
            let progress = Arc::clone(&progress);
            let reads = Rc::clone(&reads);

            spawn_task_progress_forwarder(
                app_state.clone(),
                task_id,
                move || {
                    reads.set(reads.get() + 1);
                    *progress
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                },
                cx,
            );
        });

        Harness {
            app_state,
            task_id,
            progress,
            reads,
            host_renders,
            _host: host,
            _subscription: subscription,
        }
    }

    fn tick(cx: &mut TestAppContext) {
        cx.executor().advance_clock(PROGRESS_TICK);
        cx.run_until_parked();
    }

    fn task_progress(harness: &Harness, cx: &mut TestAppContext) -> Option<f32> {
        harness.app_state.read_with(cx, |state, _cx| {
            state
                .tasks()
                .get(harness.task_id)
                .and_then(|snapshot| snapshot.progress)
        })
    }

    #[gpui::test]
    fn forwards_progress_only_once_the_total_is_known(cx: &mut TestAppContext) {
        let harness = start_forwarding(cx);

        cx.run_until_parked();
        assert_eq!(
            harness.reads.get(),
            0,
            "nothing is read before the first tick"
        );

        *harness.progress.lock().expect("progress lock") = (40, None);
        tick(cx);
        assert_eq!(harness.reads.get(), 1);
        assert_eq!(task_progress(&harness, cx), None);
        assert_eq!(harness.host_renders.get(), 1);

        *harness.progress.lock().expect("progress lock") = (40, Some(160));
        tick(cx);
        assert_eq!(harness.reads.get(), 2);
        assert_eq!(task_progress(&harness, cx), Some(0.25));
        assert_eq!(harness.host_renders.get(), 2);
    }

    #[gpui::test]
    fn stops_once_the_task_leaves_running(cx: &mut TestAppContext) {
        let harness = start_forwarding(cx);

        tick(cx);
        assert_eq!(harness.reads.get(), 1);

        harness.app_state.update(cx, |state, _cx| {
            state.tasks_mut().complete(harness.task_id);
        });

        tick(cx);
        tick(cx);
        assert_eq!(harness.reads.get(), 1, "a finished task is not polled");
        assert_eq!(harness.host_renders.get(), 1);
    }

    #[gpui::test]
    fn stops_once_the_task_is_cancelled(cx: &mut TestAppContext) {
        let harness = start_forwarding(cx);

        harness.app_state.update(cx, |state, _cx| {
            state.tasks_mut().cancel(harness.task_id);
        });

        tick(cx);
        tick(cx);
        assert_eq!(harness.reads.get(), 0);
        assert_eq!(harness.host_renders.get(), 0);
    }
}
