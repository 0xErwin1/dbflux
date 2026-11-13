//! Writes the general settings to `dbflux.db` off the UI thread.
//!
//! A caller applies a change in memory first, so it takes effect at once, and
//! then hands the settings to [`persist_general_settings`]. One writer runs at
//! a time: a change that arrives while a write is in flight waits as the
//! pending write, and a later change replaces it, so holding a key that
//! changes a setting costs a few writes instead of one per repeat.
//!
//! Every change gets a sequence number, and a write that finds a newer one
//! already on disk skips itself. That keeps an older value from landing after
//! a newer one even when the app quits while a write is in flight and writes
//! the latest change on the spot.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use dbflux_app::config_loader::save_general_settings_at;
use dbflux_core::GeneralSettings;
use dbflux_storage::error::StorageError;
use gpui::{App, AsyncApp, Entity, Global};

use crate::app_state_entity::AppStateEntity;
use crate::user_error::{ErrorKind, UserFacingError, report_error};

#[cfg(test)]
mod tests;

/// Runs on the foreground once the write holding a change has finished, with
/// whether it reached the disk.
type SavedCallback = Box<dyn FnOnce(bool, &mut App)>;

struct PendingWrite {
    seq: u64,
    settings: GeneralSettings,
    callbacks: Vec<SavedCallback>,
}

/// The change a drain task is writing, kept so quitting can write it too.
struct InFlightWrite {
    seq: u64,
    settings: GeneralSettings,
}

/// The writes of one database. Each database has its own, so a change for
/// one never replaces a change queued for another.
#[derive(Default)]
struct DatabaseWriter {
    next_seq: u64,
    pending: Option<PendingWrite>,
    in_flight: Option<InFlightWrite>,
    /// Whether a drain task is running. It keeps running until no write is
    /// pending, so a second one is never started next to it.
    draining: bool,
    /// The sequence number of the newest change on disk. Held for the whole
    /// of every write, so writes never overlap and a stale one sees it.
    last_written: Arc<Mutex<u64>>,
}

#[derive(Default)]
struct SettingsWriter {
    databases: HashMap<PathBuf, DatabaseWriter>,
    quit_hook_registered: bool,
    #[cfg(test)]
    writes_started: usize,
}

impl Global for SettingsWriter {}

/// Saves `settings` to the database of `app_state` on a background thread.
///
/// `on_saved` runs on the foreground when the write that holds this change
/// has finished: `true` once it is on disk, `false` when the write failed. A
/// failure is reported to the user here, so callers must not report it again.
/// The caller applies the change in memory itself, before calling this.
///
/// When the app quits while the change is being written, the quit hook writes
/// it on the spot, but `on_saved` may never run: the background write that
/// owns the callback can be cut short by the process exiting. A pending
/// change that no write has taken yet still has its `on_saved` run on quit.
pub fn persist_general_settings(
    app_state: &Entity<AppStateEntity>,
    settings: GeneralSettings,
    on_saved: impl FnOnce(bool, &mut App) + 'static,
    cx: &mut App,
) {
    let db_path = app_state
        .read(cx)
        .storage_runtime()
        .dbflux_db_path()
        .to_path_buf();

    enqueue(db_path, settings, on_saved, cx);
}

fn enqueue(
    db_path: PathBuf,
    settings: GeneralSettings,
    on_saved: impl FnOnce(bool, &mut App) + 'static,
    cx: &mut App,
) {
    register_quit_hook(cx);

    let writer = cx
        .default_global::<SettingsWriter>()
        .databases
        .entry(db_path.clone())
        .or_default();
    let callback: SavedCallback = Box::new(on_saved);
    writer.next_seq += 1;
    let seq = writer.next_seq;

    match &mut writer.pending {
        Some(pending) => {
            pending.seq = seq;
            pending.settings = settings;
            pending.callbacks.push(callback);
        }
        None => {
            writer.pending = Some(PendingWrite {
                seq,
                settings,
                callbacks: vec![callback],
            });
        }
    }

    if !writer.draining {
        writer.draining = true;
        cx.spawn(async move |cx| drain(db_path, cx).await).detach();
    }
}

/// Writes the pending changes of one database one at a time until none is
/// left.
async fn drain(db_path: PathBuf, cx: &mut AsyncApp) {
    while let Some((write, last_written)) = cx.update(|cx| take_pending(&db_path, cx)) {
        let PendingWrite {
            seq,
            settings,
            callbacks,
        } = write;

        let result = cx
            .background_executor()
            .spawn({
                let db_path = db_path.clone();
                async move { write_guarded(&last_written, seq, &db_path, &settings) }
            })
            .await;

        cx.update(|cx| finish(&db_path, seq, result, callbacks, cx));
    }
}

/// The next write of `db_path` and the lock it runs under, or `None` after
/// marking that database idle. Both happen in one foreground update, so a
/// change enqueued after this sees an idle writer and starts a new drain.
fn take_pending(db_path: &Path, cx: &mut App) -> Option<(PendingWrite, Arc<Mutex<u64>>)> {
    let settings_writer = cx.default_global::<SettingsWriter>();
    let writer = settings_writer.databases.get_mut(db_path)?;

    let Some(write) = writer.pending.take() else {
        writer.draining = false;
        return None;
    };

    writer.in_flight = Some(InFlightWrite {
        seq: write.seq,
        settings: write.settings.clone(),
    });
    let last_written = writer.last_written.clone();

    #[cfg(test)]
    {
        settings_writer.writes_started += 1;
    }

    Some((write, last_written))
}

/// Writes change `seq` unless a newer one is already on disk, in which case
/// the newer value stands for it and the write succeeds without touching the
/// database: its callers are told it was saved, since what they asked for is
/// on disk or was replaced by a later change.
fn write_guarded(
    last_written: &Mutex<u64>,
    seq: u64,
    db_path: &Path,
    settings: &GeneralSettings,
) -> Result<(), StorageError> {
    let mut last_written = last_written.lock().unwrap_or_else(PoisonError::into_inner);
    if seq <= *last_written {
        return Ok(());
    }

    save_general_settings_at(db_path, settings)?;
    *last_written = seq;
    Ok(())
}

fn finish(
    db_path: &Path,
    seq: u64,
    result: Result<(), StorageError>,
    callbacks: Vec<SavedCallback>,
    cx: &mut App,
) {
    if let Some(writer) = cx
        .default_global::<SettingsWriter>()
        .databases
        .get_mut(db_path)
        && writer
            .in_flight
            .as_ref()
            .is_some_and(|write| write.seq == seq)
    {
        writer.in_flight = None;
    }

    run_callbacks(result, callbacks, cx);
}

/// Reports a failed write once, then tells every caller of the write whether
/// their change reached the disk.
fn run_callbacks(result: Result<(), StorageError>, callbacks: Vec<SavedCallback>, cx: &mut App) {
    let saved = match result {
        Ok(()) => true,
        Err(error) => {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("settings.general.save.error", error = error),
                ),
                cx,
            );
            false
        }
    };

    for callback in callbacks {
        callback(saved, cx);
    }
}

/// A background task does not survive the app quitting, so the pending write
/// is made on the spot instead.
fn register_quit_hook(cx: &mut App) {
    let writer = cx.default_global::<SettingsWriter>();
    if writer.quit_hook_registered {
        return;
    }
    writer.quit_hook_registered = true;

    cx.on_app_quit(|cx| {
        flush_pending(cx);
        async {}
    })
    .detach();
}

/// Writes the newest change of every database on the calling thread: the
/// pending one, or else the one a drain task is still writing, which a
/// background task may not finish before the process exits. A drain write
/// that starts later finds this one on disk and skips itself. A failure is
/// reported like any other, so it reaches the audit log even though no
/// window is left to show it.
fn flush_pending(cx: &mut App) {
    let Some(writer) = cx.try_global::<SettingsWriter>() else {
        return;
    };
    let db_paths: Vec<PathBuf> = writer.databases.keys().cloned().collect();

    for db_path in db_paths {
        let Some(writer) = cx
            .default_global::<SettingsWriter>()
            .databases
            .get_mut(&db_path)
        else {
            continue;
        };
        let last_written = writer.last_written.clone();

        let (seq, settings, callbacks) = match writer.pending.take() {
            Some(write) => (write.seq, write.settings, write.callbacks),
            None => match &writer.in_flight {
                Some(write) => (write.seq, write.settings.clone(), Vec::new()),
                None => continue,
            },
        };

        let result = write_guarded(&last_written, seq, &db_path, &settings);
        run_callbacks(result, callbacks, cx);
    }
}

#[cfg(test)]
pub(crate) fn writes_started(cx: &App) -> usize {
    cx.try_global::<SettingsWriter>()
        .map_or(0, |writer| writer.writes_started)
}
