//! The close and quit state of a file document that saves edits.
//!
//! A file document keeps one [`FileEditLifecycle`] and asks it what a
//! finished save, a quit and the shutdown flush mean for the tab. What the
//! document saves and how it reports a save stay with the document.

use crate::file_save::{check_local_save, verify_version};
use crate::file_source::{FileLocation, SourceVersion, read_version};
use crate::pane::QuitDisposition;

/// The largest local file the shutdown flush saves without asking.
///
/// A save reads the whole file and writes it again, then syncs it to disk,
/// and the shutdown waits 2 s for every pending document write
/// (`DOCUMENT_FLUSH_TIMEOUT` in the `dbflux` binary) before it stops the
/// process. At a conservative 16 MiB/s for reading, writing and syncing
/// together (a slow disk or a network file system), 16 MiB takes 1 s, which
/// leaves half the budget for the sync's latency and for the scripts that
/// flush within the same wait. A larger file asks before the quit, where its
/// save has all the time it needs.
pub(crate) const SHUTDOWN_SAVE_MAX_BYTES: u64 = 16 * 1024 * 1024;

/// What the interrupted-close flow, a confirmed quit and the shutdown flush
/// asked of a file document.
#[derive(Debug, Default)]
pub(crate) struct FileEditLifecycle {
    /// Set when a save was started by the interrupted-close flow, so a save
    /// that lands also asks the workspace to close the tab.
    close_after_save: bool,

    /// Set once the shutdown flush started its save, so the flush polling
    /// the document starts it once and a refused save is not retried.
    shutdown_save_started: bool,

    /// Set when the user quit without saving the pending changes, so the
    /// shutdown flush does not write them.
    discarded_for_quit: bool,
}

impl FileEditLifecycle {
    /// Records that the next save to finish was asked for by the
    /// interrupted-close flow.
    pub(crate) fn close_after_save(&mut self) {
        self.close_after_save = true;
    }

    /// Records that the next save to finish was asked for by a quit, so it
    /// no longer closes the tab even when a close asked for it first.
    pub(crate) fn keep_open_after_save(&mut self) {
        self.close_after_save = false;
    }

    /// Whether the save that just finished must close the tab when it
    /// succeeded. The intent is dropped either way, so a later save cannot
    /// close a tab the user kept.
    pub(crate) fn take_close_after_save(&mut self) -> bool {
        std::mem::take(&mut self.close_after_save)
    }

    /// Records that the user quit without saving the pending changes.
    pub(crate) fn discard_for_quit(&mut self) {
        self.discarded_for_quit = true;
    }

    /// Whether the shutdown flush must write the pending changes now.
    ///
    /// Only the first poll of the flush decides: it returns whether
    /// `has_unsaved_changes` holds and the user did not discard them for the
    /// quit. Every later poll returns false, so a refused save is not
    /// retried. `has_unsaved_changes` is false while a save already runs,
    /// which the flush waits for instead of repeating.
    pub(crate) fn start_shutdown_flush(&mut self, has_unsaved_changes: bool) -> bool {
        if self.shutdown_save_started {
            return false;
        }

        self.shutdown_save_started = true;

        has_unsaved_changes && !self.discarded_for_quit
    }
}

/// What quitting means for a document's pending changes.
///
/// A clean document loses nothing. A document whose save would go through
/// now without asking (`local_save_goes_through`, called only for a dirty
/// document) is saved by the shutdown flush. Everything else needs the
/// user's decision before the quit starts.
pub(crate) fn quit_disposition(
    dirty: bool,
    local_save_goes_through: impl FnOnce() -> bool,
) -> QuitDisposition {
    if !dirty {
        return QuitDisposition::Clean;
    }

    if local_save_goes_through() {
        QuitDisposition::SavedOnQuit
    } else {
        QuitDisposition::NeedsDecision
    }
}

/// Whether the file system would let a save of the local file at `location`
/// go through now: the file is at most [`SHUTDOWN_SAVE_MAX_BYTES`] long, it
/// is still the `opened` version and that version can show a change
/// ([`verify_version`]), and its permission bits, the operating system and
/// its directory let it be replaced ([`check_local_save`]). An object is
/// never saved without asking, so it always returns false.
///
/// `source_length` is the length of the opened version. The checks are a few
/// file system calls on the calling thread, one of which creates and removes
/// a staging file, and nothing is reported.
pub(crate) fn local_file_saves_now(
    location: &FileLocation,
    opened: &SourceVersion,
    source_length: u64,
) -> bool {
    if source_length > SHUTDOWN_SAVE_MAX_BYTES {
        return false;
    }

    let FileLocation::Local { path } = location else {
        return false;
    };

    let version_matches =
        read_version(location).is_ok_and(|current| verify_version(opened, &current).is_ok());

    version_matches && check_local_save(path).is_ok()
}
