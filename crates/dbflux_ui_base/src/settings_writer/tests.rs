use std::cell::RefCell;
use std::rc::Rc;

use dbflux_core::GeneralSettings;
use dbflux_storage::bootstrap::StorageRuntime;
use gpui::{AppContext as _, Entity, TestAppContext};

use super::{enqueue, flush_pending, persist_general_settings};
use crate::app_state_entity::{AppStateEntity, AppStateGlobal};
use crate::toast::{ToastGlobal, ToastHost};

fn app_state(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
    let app_state = cx.update(|cx| {
        cx.new(|_| {
            AppStateEntity::new_with_storage_runtime(
                StorageRuntime::in_memory().expect("isolated storage runtime"),
            )
            .expect("test storage setup")
        })
    });
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
        cx.set_global(AppStateGlobal {
            entity: app_state.clone(),
        });
    });
    app_state
}

fn saved_max_history(app_state: &Entity<AppStateEntity>, cx: &mut TestAppContext) -> i64 {
    cx.update(|cx| {
        app_state
            .read(cx)
            .storage_runtime()
            .general_settings()
            .get()
            .expect("read saved settings")
            .map(|dto| dto.max_history_entries)
            .unwrap_or_default()
    })
}

fn db_path(app_state: &Entity<AppStateEntity>, cx: &gpui::App) -> std::path::PathBuf {
    app_state
        .read(cx)
        .storage_runtime()
        .dbflux_db_path()
        .to_path_buf()
}

fn with_max_history(entries: usize) -> GeneralSettings {
    GeneralSettings {
        max_history_entries: entries,
        ..GeneralSettings::default()
    }
}

type Outcomes = Rc<RefCell<Vec<(usize, bool)>>>;

fn record(outcomes: &Outcomes, tag: usize) -> impl FnOnce(bool, &mut gpui::App) + 'static {
    let outcomes = outcomes.clone();
    move |saved, _cx| outcomes.borrow_mut().push((tag, saved))
}

/// The write leaves the UI thread: nothing is on disk until the background
/// work runs, and the callback reports the outcome once it has.
#[gpui::test]
fn a_change_is_saved_off_the_ui_thread(cx: &mut TestAppContext) {
    let app_state = app_state(cx);
    let before = saved_max_history(&app_state, cx);
    let outcomes = Outcomes::default();

    cx.update(|cx| {
        persist_general_settings(&app_state, with_max_history(77), record(&outcomes, 1), cx)
    });

    assert_eq!(saved_max_history(&app_state, cx), before);
    assert!(outcomes.borrow().is_empty());

    cx.run_until_parked();

    assert_eq!(saved_max_history(&app_state, cx), 77);
    assert_eq!(*outcomes.borrow(), vec![(1, true)]);
}

/// Changes queued while a write is pending collapse into the latest one, and
/// every caller hears that its change was saved.
#[gpui::test]
fn changes_queued_together_are_saved_once_as_the_latest(cx: &mut TestAppContext) {
    let app_state = app_state(cx);
    let outcomes = Outcomes::default();

    cx.update(|cx| {
        for (tag, entries) in [(1, 50), (2, 60), (3, 70)] {
            persist_general_settings(
                &app_state,
                with_max_history(entries),
                record(&outcomes, tag),
                cx,
            );
        }
    });
    cx.run_until_parked();

    assert_eq!(saved_max_history(&app_state, cx), 70);
    assert_eq!(*outcomes.borrow(), vec![(1, true), (2, true), (3, true)]);
    assert_eq!(cx.update(|cx| super::writes_started(cx)), 1);
}

/// Changes made after a write has finished start a new one, which coalesces
/// them and leaves the latest value on disk.
#[gpui::test]
fn a_later_change_starts_a_new_write_and_lands_last(cx: &mut TestAppContext) {
    let app_state = app_state(cx);
    let outcomes = Outcomes::default();

    cx.update(|cx| {
        persist_general_settings(&app_state, with_max_history(50), record(&outcomes, 1), cx)
    });
    cx.executor().run_until_parked();
    cx.update(|cx| {
        persist_general_settings(&app_state, with_max_history(60), record(&outcomes, 2), cx);
        persist_general_settings(&app_state, with_max_history(70), record(&outcomes, 3), cx);
    });
    cx.run_until_parked();

    assert_eq!(saved_max_history(&app_state, cx), 70);
    assert_eq!(*outcomes.borrow(), vec![(1, true), (2, true), (3, true)]);
    assert_eq!(cx.update(|cx| super::writes_started(cx)), 2);
}

/// A write that fails is reported once, tells its callers it was not saved,
/// and does not stop the next write.
#[gpui::test]
fn a_failed_write_is_reported_and_the_next_one_still_runs(cx: &mut TestAppContext) {
    let app_state = app_state(cx);
    let outcomes = Outcomes::default();
    let not_a_database = tempfile::tempdir().expect("scratch directory");

    cx.update(|cx| {
        enqueue(
            not_a_database.path().to_path_buf(),
            with_max_history(50),
            record(&outcomes, 1),
            cx,
        )
    });
    cx.run_until_parked();

    assert_eq!(*outcomes.borrow(), vec![(1, false)]);
    assert_eq!(cx.update(|cx| app_state.read(cx).unread_error_count), 1);

    cx.update(|cx| {
        persist_general_settings(&app_state, with_max_history(60), record(&outcomes, 2), cx)
    });
    cx.run_until_parked();

    assert_eq!(saved_max_history(&app_state, cx), 60);
    assert_eq!(*outcomes.borrow(), vec![(1, false), (2, true)]);
    assert_eq!(cx.update(|cx| app_state.read(cx).unread_error_count), 1);
}

/// Quitting writes a pending change on the spot instead of dropping it with
/// the background task.
#[gpui::test]
fn a_pending_change_is_written_when_the_app_quits(cx: &mut TestAppContext) {
    let app_state = app_state(cx);
    let outcomes = Outcomes::default();

    cx.update(|cx| {
        persist_general_settings(&app_state, with_max_history(88), record(&outcomes, 1), cx);
        flush_pending(cx);
    });

    assert_eq!(saved_max_history(&app_state, cx), 88);
    assert_eq!(*outcomes.borrow(), vec![(1, true)]);

    cx.run_until_parked();
    assert_eq!(cx.update(|cx| super::writes_started(cx)), 0);
}

/// A write the drain has already taken, but that has not reached the disk,
/// is still written when the app quits. Its callback stays with the drain
/// task, which the process exit can cut short, so quitting does not run it.
#[gpui::test]
fn quitting_writes_a_change_already_taken_by_the_drain(cx: &mut TestAppContext) {
    let app_state = app_state(cx);
    let outcomes = Outcomes::default();

    cx.update(|cx| {
        persist_general_settings(&app_state, with_max_history(88), record(&outcomes, 1), cx);
        let in_flight = super::take_pending(&db_path(&app_state, cx), cx);
        assert!(in_flight.is_some(), "the drain takes the change");
        flush_pending(cx);
        assert!(
            outcomes.borrow().is_empty(),
            "the callback belongs to the in-flight write, not to the quit flush"
        );
    });

    assert_eq!(saved_max_history(&app_state, cx), 88);
}

/// A write taken by the drain that only starts after a newer change was
/// written on quit leaves the newer value on disk.
#[gpui::test]
fn a_stale_write_never_lands_after_a_newer_one(cx: &mut TestAppContext) {
    let app_state = app_state(cx);
    let outcomes = Outcomes::default();

    let stale_path = cx.update(|cx| db_path(&app_state, cx));
    let stale = cx.update(|cx| {
        persist_general_settings(&app_state, with_max_history(50), record(&outcomes, 1), cx);
        let stale =
            super::take_pending(&db_path(&app_state, cx), cx).expect("the drain takes the change");
        persist_general_settings(&app_state, with_max_history(70), record(&outcomes, 2), cx);
        flush_pending(cx);
        stale
    });

    let (write, guard) = stale;
    let result = super::write_guarded(&guard, write.seq, &stale_path, &write.settings);
    assert!(result.is_ok(), "a stale write is skipped, not failed");
    cx.update(|cx| super::finish(&stale_path, write.seq, result, write.callbacks, cx));

    assert_eq!(saved_max_history(&app_state, cx), 70);
    // The skipped change is covered by the newer value on disk, so its caller
    // hears that it was saved, and no error is reported.
    assert_eq!(*outcomes.borrow(), vec![(2, true), (1, true)]);
    assert_eq!(cx.update(|cx| app_state.read(cx).unread_error_count), 0);
}

/// Changes made while a write is in flight collapse into one write after it.
#[gpui::test]
fn changes_made_during_a_write_are_saved_once_after_it(cx: &mut TestAppContext) {
    let app_state = app_state(cx);
    let outcomes = Outcomes::default();

    cx.update(|cx| {
        persist_general_settings(&app_state, with_max_history(50), record(&outcomes, 1), cx);
        let _in_flight =
            super::take_pending(&db_path(&app_state, cx), cx).expect("the drain takes the change");
        persist_general_settings(&app_state, with_max_history(60), record(&outcomes, 2), cx);
        persist_general_settings(&app_state, with_max_history(70), record(&outcomes, 3), cx);
    });
    cx.run_until_parked();

    assert_eq!(saved_max_history(&app_state, cx), 70);
    assert_eq!(*outcomes.borrow(), vec![(2, true), (3, true)]);
    assert_eq!(cx.update(|cx| super::writes_started(cx)), 2);
}

/// Changes for two databases queued before the drain runs are each written
/// to their own database, and each caller hears about its own write.
#[gpui::test]
fn changes_for_two_databases_are_written_separately(cx: &mut TestAppContext) {
    let first = app_state(cx);
    let second = cx.update(|cx| {
        cx.new(|_| {
            AppStateEntity::new_with_storage_runtime(
                StorageRuntime::in_memory().expect("isolated storage runtime"),
            )
            .expect("test storage setup")
        })
    });
    let not_a_database = tempfile::tempdir().expect("scratch directory");
    let outcomes = Outcomes::default();

    cx.update(|cx| {
        persist_general_settings(&first, with_max_history(50), record(&outcomes, 1), cx);
        persist_general_settings(&second, with_max_history(60), record(&outcomes, 2), cx);
        enqueue(
            not_a_database.path().to_path_buf(),
            with_max_history(70),
            record(&outcomes, 3),
            cx,
        );
    });
    cx.run_until_parked();

    assert_eq!(saved_max_history(&first, cx), 50);
    assert_eq!(saved_max_history(&second, cx), 60);
    let mut outcomes = outcomes.borrow().clone();
    outcomes.sort_unstable();
    assert_eq!(outcomes, vec![(1, true), (2, true), (3, false)]);
}

/// A write that fails on quit goes through the same error report as any
/// other failed write, and its caller hears that it was not saved.
#[gpui::test]
fn a_failed_write_on_quit_is_reported(cx: &mut TestAppContext) {
    let app_state = app_state(cx);
    let outcomes = Outcomes::default();
    let not_a_database = tempfile::tempdir().expect("scratch directory");

    cx.update(|cx| {
        enqueue(
            not_a_database.path().to_path_buf(),
            with_max_history(50),
            record(&outcomes, 1),
            cx,
        );
        flush_pending(cx);
    });

    assert_eq!(*outcomes.borrow(), vec![(1, false)]);
    assert_eq!(cx.update(|cx| app_state.read(cx).unread_error_count), 1);
}

/// Quitting the app runs the registered quit hook, which writes the change
/// the drain never got to.
#[gpui::test]
fn quitting_the_app_writes_the_pending_change(cx: &mut TestAppContext) {
    let app_state = app_state(cx);
    let path = cx.update(|cx| db_path(&app_state, cx));
    let outcomes = Outcomes::default();

    cx.update(|cx| {
        persist_general_settings(&app_state, with_max_history(88), record(&outcomes, 1), cx)
    });
    cx.quit();

    let connection = rusqlite::Connection::open(&path).expect("open the settings database");
    let stored: i64 = connection
        .query_row(
            "SELECT max_history_entries FROM cfg_general_settings",
            [],
            |row| row.get(0),
        )
        .expect("read the stored settings");
    assert_eq!(stored, 88);
    assert_eq!(*outcomes.borrow(), vec![(1, true)]);
}
