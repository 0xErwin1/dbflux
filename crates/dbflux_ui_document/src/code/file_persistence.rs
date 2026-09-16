//! Physical-write serialization for file-backed code documents.
//!
//! Every write to the real file — debounced autosave, explicit save, Save As —
//! is routed through the per-document [`PhysicalWriteQueue`]. The queue starts
//! one write at a time and only ever starts the next one from the previous
//! write's completion: an in-flight write always lands before its replacement,
//! and superseding a queued autosave happens by replacing the queued item, not
//! by cancelling running work.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

/// What asked for the write; drives completion-side reporting and queueing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WriteKind {
    /// Debounced autosave: conflict-checked against the document's baseline
    /// and never reported through the save/close event flow.
    Auto,
    /// Explicit save (Ctrl+S): an intentional overwrite, no conflict check,
    /// reported through the save-outcome flow.
    Explicit,
    /// Save As: writes a path the document may not have written before, then
    /// retargets the document at it.
    SaveAs { used_fallback: bool },
    /// Close-driven flush of pending edits: conflict-checked like an autosave
    /// (closing must not overwrite a change made outside dbflux), but unlike an
    /// autosave it reports its outcome and asks the workspace to close the tab
    /// once the write lands.
    CloseFlush,
    /// Graceful-shutdown flush of pending edits: conflict-checked like an
    /// autosave so quitting never overwrites a change made outside dbflux, but
    /// unlike a close flush it never asks the workspace to close the tab and is
    /// never reported as a user save.
    ShutdownFlush,
}

/// The raw bytes a document last loaded from, or successfully wrote to, one
/// file path.
///
/// Pairing the bytes with their path is what makes the baseline trustworthy:
/// bytes read from one file can never authorize a write to a different one, so a
/// stale baseline cannot be mistaken for consent to overwrite a file the
/// document never read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct FileBaseline {
    pub(super) path: PathBuf,
    pub(super) bytes: String,
}

impl FileBaseline {
    pub(super) fn new(path: PathBuf, bytes: String) -> Self {
        Self { path, bytes }
    }
}

/// One queued write to the real file.
pub(super) struct PhysicalWrite {
    /// Destination of the write.
    pub(super) path: PathBuf,
    /// Full file bytes, annotation header included.
    pub(super) content: String,
    /// Buffer text captured alongside `content`, for dirty reconciliation.
    pub(super) saved_input: String,
    /// Session-restore shadow file to keep in sync (autosave only).
    pub(super) shadow: Option<PathBuf>,
    pub(super) kind: WriteKind,
}

impl PhysicalWrite {
    pub(super) fn auto(
        path: PathBuf,
        content: String,
        saved_input: String,
        shadow: Option<PathBuf>,
    ) -> Self {
        Self {
            path,
            content,
            saved_input,
            shadow,
            kind: WriteKind::Auto,
        }
    }

    pub(super) fn explicit(path: PathBuf, content: String, saved_input: String) -> Self {
        Self {
            path,
            content,
            saved_input,
            shadow: None,
            kind: WriteKind::Explicit,
        }
    }

    pub(super) fn save_as(
        path: PathBuf,
        content: String,
        saved_input: String,
        used_fallback: bool,
    ) -> Self {
        Self {
            path,
            content,
            saved_input,
            shadow: None,
            kind: WriteKind::SaveAs { used_fallback },
        }
    }

    pub(super) fn close_flush(path: PathBuf, content: String, saved_input: String) -> Self {
        Self {
            path,
            content,
            saved_input,
            shadow: None,
            kind: WriteKind::CloseFlush,
        }
    }

    pub(super) fn shutdown_flush(path: PathBuf, content: String, saved_input: String) -> Self {
        Self {
            path,
            content,
            saved_input,
            shadow: None,
            kind: WriteKind::ShutdownFlush,
        }
    }
}

/// What happened to one write attempt on the background thread.
pub(super) enum WriteOutcome {
    /// The bytes landed; the document adopts them as its new baseline.
    Written,
    /// Autosave only: the file no longer holds the bytes this document last
    /// loaded or wrote, so the write was refused to spare the foreign change.
    ExternalConflict,
    /// Autosave only: the file this document had written is gone.
    ExternallyDeleted,
    /// Autosave only: the document has no trustworthy loaded baseline for this
    /// path, so refusing is safer than creating or overwriting the file blind.
    BaselineUnknown,
    /// The write did not land; the document's on-disk state is unchanged.
    Failed(std::io::Error),
}

/// A finished write plus the baseline the document should adopt from it.
pub(super) struct ExecutedWrite {
    pub(super) write: PhysicalWrite,
    pub(super) outcome: WriteOutcome,
    /// The raw bytes this document now owns on disk, and the path they landed
    /// at, if any.
    pub(super) new_baseline: Option<FileBaseline>,
}

/// Reads the current file bytes, or `None` when the file does not exist.
fn read_disk_state(path: &PathBuf) -> Result<Option<String>, std::io::Error> {
    match std::fs::read_to_string(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Keeps the session-restore shadow in sync with the captured content.
///
/// The physical file is the user-visible artifact, so a shadow failure is
/// logged rather than toasted: the document's data already has a home.
fn write_shadow_best_effort(write: &PhysicalWrite) {
    if let Some(shadow) = &write.shadow
        && let Err(e) = std::fs::write(shadow, &write.content)
    {
        log::warn!(
            "Auto-save shadow write failed for {}: {e}",
            shadow.display()
        );
    }
}

/// Performs the physical byte write for one destination path.
///
/// Kept as a single step so a test can reproduce a partial write failure on the
/// real write path rather than on a stand-in for it.
fn write_physical_bytes(path: &Path, content: &str) -> std::io::Result<()> {
    #[cfg(test)]
    if let Some(fault) = tests::take_physical_write_fault() {
        return fault(path, content);
    }

    std::fs::write(path, content)
}

/// Replaces the destination's contents without ever leaving a truncated
/// original behind.
///
/// The bytes are staged in a private file inside the destination's own
/// directory and renamed onto it only once that staging file is complete, so a
/// failure while writing leaves the previous file untouched. Permissions of an
/// existing destination survive the replacement, and a read-only destination is
/// refused instead of being replaced through its directory.
fn replace_file_contents(destination: &Path, content: &str) -> std::io::Result<()> {
    let destination = resolve_write_destination(destination)?;
    let directory = parent_directory(&destination);

    let existing = match std::fs::metadata(&destination) {
        Ok(metadata) => Some(metadata),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e),
    };

    if existing
        .as_ref()
        .is_some_and(|metadata| metadata.permissions().readonly())
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("{} is read-only", destination.display()),
        ));
    }

    let staged_path = stage_path(directory);
    let new_file_permissions = create_staging_file(&staged_path)?;
    let staged_permissions = existing
        .as_ref()
        .map_or(new_file_permissions, |metadata| metadata.permissions());

    let committed = (|| -> std::io::Result<()> {
        write_physical_bytes(&staged_path, content)?;
        std::fs::set_permissions(&staged_path, staged_permissions)?;
        std::fs::rename(&staged_path, &destination)
    })();

    if let Err(commit_error) = committed {
        discard_staging(&staged_path);
        return Err(commit_error);
    }

    Ok(())
}

/// Resolves the path the bytes must actually land on.
///
/// Symlinks are followed first: replacing the link itself with a regular file
/// would silently detach the document from wherever the link points.
fn resolve_write_destination(path: &Path) -> std::io::Result<PathBuf> {
    match std::fs::canonicalize(path) {
        Ok(real_path) => Ok(real_path),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => match std::fs::read_link(path) {
            // A dangling symlink still names a real destination to create.
            Ok(link_target) => Ok(link_destination(path, link_target)),
            Err(link_error) if link_error.kind() == std::io::ErrorKind::NotFound => {
                Ok(path.to_path_buf())
            }
            Err(link_error) => Err(link_error),
        },
        Err(e) => Err(e),
    }
}

/// Where a relative symlink points, resolved against the link's own directory.
fn link_destination(link: &Path, link_target: PathBuf) -> PathBuf {
    if link_target.is_absolute() {
        return link_target;
    }

    parent_directory(link).join(link_target)
}

/// The directory a file lives in; a bare file name means the current directory.
fn parent_directory(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

/// A unique hidden staging name inside `directory`, so the commit rename stays
/// on one filesystem and never crosses a mount point.
fn stage_path(directory: &Path) -> PathBuf {
    directory.join(format!(".dbflux-stage-{}.tmp", uuid::Uuid::new_v4()))
}

/// Creates the staging file exclusively and returns the permissions a newly
/// created file keeps, so a brand-new destination is not created more
/// restrictively than a plain write would create it.
///
/// A name collision fails at `create_new` before the file belongs to this
/// attempt, so nothing is removed and an existing path is never deleted. Once
/// the file exists it was created here, and any later setup failure must take
/// that uncommitted file with it instead of leaking an empty temporary.
fn create_staging_file(path: &Path) -> std::io::Result<std::fs::Permissions> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;

    let staged = prepare_staging_file(path, &file);

    if staged.is_err() {
        discard_staging(path);
    }

    staged
}

/// Applies the private staging permissions and reports the mode a fresh
/// destination keeps. Split out from `create_staging_file` so the ownership
/// boundary between "we created it" and "the name collided" has one owner.
fn prepare_staging_file(
    path: &Path,
    file: &std::fs::File,
) -> std::io::Result<std::fs::Permissions> {
    let new_file_permissions = file.metadata()?.permissions();

    #[cfg(test)]
    if tests::take_staging_setup_fault() {
        return Err(std::io::Error::other("injected staging setup failure"));
    }

    // The uncommitted bytes stay owner-only while they carry a temporary name.
    #[cfg(unix)]
    {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }

    Ok(new_file_permissions)
}

/// Removes a staging file that must not be committed. A leftover that cannot be
/// removed is traced rather than failing silently.
fn discard_staging(staged_path: &Path) {
    if let Err(e) = std::fs::remove_file(staged_path) {
        log::warn!(
            "Failed to remove staging file {}: {e}",
            staged_path.display()
        );
    }
}

/// Executes one queued write synchronously; runs on the background executor.
///
/// Autosaves and close flushes are conflict-checked against `baseline` — the raw bytes this
/// document last loaded or successfully wrote to a specific path — so foreign
/// changes are never silently overwritten. Without a baseline, or with a
/// baseline that belongs to a different path, the write refuses: creating or
/// overwriting a file the document never read is exactly the blind write the
/// baseline exists to prevent. Explicit saves and Save As intentionally
/// overwrite and only adopt the new baseline on success.
pub(super) fn execute_write(
    write: PhysicalWrite,
    baseline: Option<&FileBaseline>,
) -> ExecutedWrite {
    let denied = match write.kind {
        WriteKind::Auto | WriteKind::CloseFlush | WriteKind::ShutdownFlush => match baseline {
            // No loaded baseline, or one that belongs to another file: the file
            // was never read or written by this document, so there is nothing to
            // compare against and nothing that authorizes a write here.
            None => {
                write_shadow_best_effort(&write);
                Some(WriteOutcome::BaselineUnknown)
            }
            Some(owned) if owned.path != write.path => {
                write_shadow_best_effort(&write);
                Some(WriteOutcome::BaselineUnknown)
            }
            Some(owned) => match read_disk_state(&write.path) {
                Ok(Some(disk_bytes)) if disk_bytes != owned.bytes => {
                    write_shadow_best_effort(&write);
                    Some(WriteOutcome::ExternalConflict)
                }
                Ok(None) => {
                    write_shadow_best_effort(&write);
                    Some(WriteOutcome::ExternallyDeleted)
                }
                Ok(Some(_)) => None,
                Err(e) => {
                    return ExecutedWrite {
                        outcome: WriteOutcome::Failed(e),
                        new_baseline: None,
                        write,
                    };
                }
            },
        },
        WriteKind::Explicit | WriteKind::SaveAs { .. } => None,
    };

    if let Some(outcome) = denied {
        return ExecutedWrite {
            outcome,
            new_baseline: None,
            write,
        };
    }

    if let Err(e) = replace_file_contents(&write.path, &write.content) {
        if matches!(
            write.kind,
            WriteKind::Auto | WriteKind::CloseFlush | WriteKind::ShutdownFlush
        ) {
            write_shadow_best_effort(&write);
        }
        return ExecutedWrite {
            outcome: WriteOutcome::Failed(e),
            new_baseline: None,
            write,
        };
    }

    if matches!(
        write.kind,
        WriteKind::Auto | WriteKind::CloseFlush | WriteKind::ShutdownFlush
    ) {
        write_shadow_best_effort(&write);
    }

    let new_baseline = FileBaseline::new(write.path.clone(), write.content.clone());
    ExecutedWrite {
        outcome: WriteOutcome::Written,
        new_baseline: Some(new_baseline),
        write,
    }
}

/// FIFO queue of physical writes with autosave coalescing.
///
/// A write runs alone; while it runs, later writes wait in `waiting`. A new
/// autosave replaces a waiting autosave (latest-wins: a burst of edits lands
/// once, with the newest bytes), but explicit saves and Save As are never
/// dropped or reordered — they append after whatever is queued.
pub(super) struct PhysicalWriteQueue {
    /// Whether a write's task is currently running.
    running: bool,
    /// What the running write is, so a close-safe caller can tell whether a
    /// close flush is already in flight. `None` while the queue is idle.
    running_kind: Option<WriteKind>,
    /// Writes waiting for the running one to finish, in landing order.
    waiting: VecDeque<PhysicalWrite>,
    /// The bytes this document last loaded or wrote, paired with the path
    /// they came from.
    baseline: Option<FileBaseline>,
    /// Set once a shutdown flush has been queued for this document.
    ///
    /// A graceful shutdown polls until nothing is outstanding. Once a shutdown
    /// flush has been queued, its refusal must not be retried forever, so the
    /// next poll reports idle instead of stacking another identical write.
    shutdown_flush_started: bool,
}

impl PhysicalWriteQueue {
    pub(super) fn new() -> Self {
        Self {
            running: false,
            running_kind: None,
            waiting: VecDeque::new(),
            baseline: None,
            shutdown_flush_started: false,
        }
    }

    /// Queues a write. Returns `true` when the caller must start it now (the
    /// queue was idle), `false` when an earlier write must finish first.
    pub(super) fn push(&mut self, write: PhysicalWrite) -> bool {
        if self.running {
            let replaces_waiting_autosave = write.kind == WriteKind::Auto
                && self
                    .waiting
                    .back()
                    .is_some_and(|back| back.kind == WriteKind::Auto);

            if replaces_waiting_autosave && let Some(back) = self.waiting.back_mut() {
                *back = write;
            } else {
                self.waiting.push_back(write);
            }

            return false;
        }

        self.waiting.push_back(write);
        true
    }

    /// Takes the next write to run and marks the queue busy. Returns `None`
    /// while a write is still running or nothing is queued.
    pub(super) fn next_to_start(&mut self) -> Option<PhysicalWrite> {
        if self.running {
            return None;
        }

        let next = self.waiting.pop_front()?;
        self.running = true;
        self.running_kind = Some(next.kind);
        Some(next)
    }

    /// Frees the running slot once a write's completion has been applied.
    pub(super) fn mark_finished(&mut self) {
        self.running = false;
        self.running_kind = None;
    }

    /// Returns `true` while any write is in flight or waiting to start.
    ///
    /// A close-safe caller uses this together with the buffer's dirty state to
    /// decide whether a flush is needed at all: a clean, idle document closes
    /// without a pointless write.
    pub(super) fn has_pending(&self) -> bool {
        self.running || !self.waiting.is_empty()
    }

    /// Returns `true` while a close-driven flush is running or waiting.
    ///
    /// A repeated close uses this to stay a no-op instead of stacking another
    /// `CloseFlush`: the flush already in flight reports back to the same close,
    /// and one autosave or explicit save does not count, so closing still queues
    /// its flush behind them.
    pub(super) fn has_pending_close_flush(&self) -> bool {
        self.running_kind == Some(WriteKind::CloseFlush)
            || self
                .waiting
                .iter()
                .any(|write| write.kind == WriteKind::CloseFlush)
    }

    /// Records that a shutdown flush has been queued for this document.
    pub(super) fn mark_shutdown_flush_started(&mut self) {
        self.shutdown_flush_started = true;
    }

    /// Whether a shutdown flush has already been queued for this document.
    pub(super) fn has_started_shutdown_flush(&self) -> bool {
        self.shutdown_flush_started
    }

    /// Clears the shutdown-flush record so a later poll can carry edits that
    /// arrived while the flush was in flight.
    pub(super) fn clear_shutdown_flush_started(&mut self) {
        self.shutdown_flush_started = false;
    }

    /// The raw bytes, and their path, this document last loaded or wrote.
    pub(super) fn baseline(&self) -> Option<&FileBaseline> {
        self.baseline.as_ref()
    }

    /// Adopts the bytes of a write that just landed, paired with its path.
    ///
    /// Adopting a baseline for a new path is what retargets the document (Save
    /// As). Any autosave still waiting for the previous path can no longer land
    /// against the new baseline - it would be refused as `BaselineUnknown` - so it
    /// is discarded here instead of being attempted and surfaced as a spurious
    /// refusal. Explicit saves and Save As are never discarded.
    pub(super) fn adopt_baseline(&mut self, baseline: Option<FileBaseline>) {
        self.baseline = baseline;
        self.discard_waiting_autosaves_for_other_paths();
    }

    /// Drops queued autosaves that target a path other than the current
    /// baseline's.
    ///
    /// Without a baseline there is nothing to retarget against and nothing is
    /// dropped, so a failed write never clears the queue. The document's own path
    /// normally matches every waiting autosave; only a retarget produces a
    /// mismatch, and only autosaves are dropped - an explicit save is an
    /// intentional user action that must never be silently discarded.
    fn discard_waiting_autosaves_for_other_paths(&mut self) {
        let Some(current_path) = self.baseline.as_ref().map(|baseline| baseline.path.clone())
        else {
            return;
        };

        self.waiting
            .retain(|write| write.kind != WriteKind::Auto || write.path == current_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// A fault a test installs to force a deterministic partial write.
    type PhysicalWriteFault = fn(&Path, &str) -> std::io::Result<()>;

    thread_local! {
        /// One-shot fault applied to the next physical byte write on this thread,
        /// so a partial write can be reproduced on the production path.
        static PHYSICAL_WRITE_FAULT: Cell<Option<PhysicalWriteFault>> = const { Cell::new(None) };
    }

    pub(super) fn take_physical_write_fault() -> Option<PhysicalWriteFault> {
        PHYSICAL_WRITE_FAULT.with(Cell::take)
    }

    fn inject_physical_write_fault(fault: PhysicalWriteFault) {
        PHYSICAL_WRITE_FAULT.with(|slot| slot.set(Some(fault)));
    }

    thread_local! {
        /// One-shot fault that fails staging setup after the staging file exists,
        /// reproducing the empty-temporary leak F1 reported.
        static STAGING_SETUP_FAULT: Cell<bool> = const { Cell::new(false) };
    }

    pub(super) fn take_staging_setup_fault() -> bool {
        STAGING_SETUP_FAULT.with(Cell::take)
    }

    fn inject_staging_setup_fault() {
        STAGING_SETUP_FAULT.with(|slot| slot.set(true));
    }

    /// Writes half of the bytes and then fails, leaving the truncated state a
    /// real partial write leaves behind.
    fn partial_write_fault(path: &Path, content: &str) -> std::io::Result<()> {
        let half = content.len() / 2;
        let prefix = content.get(..half).unwrap_or(content);
        std::fs::write(path, prefix)?;
        Err(std::io::Error::other("injected partial write failure"))
    }

    fn temp_write_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "dbflux-physical-write-{name}-{}.sql",
            uuid::Uuid::new_v4()
        ))
    }

    fn auto_write(path: PathBuf, content: &str) -> PhysicalWrite {
        PhysicalWrite::auto(path, content.to_string(), content.to_string(), None)
    }

    fn explicit_write(path: PathBuf, content: &str) -> PhysicalWrite {
        PhysicalWrite::explicit(path, content.to_string(), content.to_string())
    }

    fn baseline(path: &Path, bytes: &str) -> FileBaseline {
        FileBaseline::new(path.to_path_buf(), bytes.to_string())
    }

    /// A private directory per test, so leftover artifacts are detected without
    /// racing other tests over the shared temp directory.
    fn temp_write_directory(name: &str) -> PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "dbflux-physical-write-{name}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&directory).expect("the temp directory must be creatable");
        directory
    }

    /// Any staging artifact a write left behind inside `directory`.
    fn staging_leftovers(directory: &Path) -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return Vec::new();
        };

        entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with(".dbflux-stage-"))
            })
            .collect()
    }

    /// A second autosave queued behind a running one replaces the first, so a
    /// burst of edits lands once with the newest bytes.
    #[test]
    fn a_waiting_autosave_is_replaced_by_the_newest_one() {
        let mut queue = PhysicalWriteQueue::new();
        let first = auto_write(temp_write_path("coalesce"), "SELECT 1;");
        let second = auto_write(temp_write_path("coalesce"), "SELECT 2;");
        let third = auto_write(temp_write_path("coalesce"), "SELECT 3;");

        assert!(queue.push(first), "an idle queue starts the write at once");
        let started = queue.next_to_start().expect("the push said start");

        assert!(!queue.push(second), "a running write defers the next one");
        assert!(!queue.push(third), "a running write defers the next one");

        queue.mark_finished();
        let resumed = queue.next_to_start().expect("a queued write must start");
        assert_eq!(
            resumed.content, "SELECT 3;",
            "only the newest autosave waits"
        );

        queue.mark_finished();
        assert!(
            queue.next_to_start().is_none(),
            "the superseded autosave must not run"
        );
        assert_eq!(started.content, "SELECT 1;");
    }

    /// An explicit save queued behind a running autosave is never dropped or
    /// reordered, even when further autosaves queue behind it.
    #[test]
    fn an_explicit_save_is_never_replaced_by_a_later_autosave() {
        let mut queue = PhysicalWriteQueue::new();
        let autosave = auto_write(temp_write_path("order"), "SELECT 1;");
        let explicit = explicit_write(temp_write_path("order"), "SELECT 2;");
        let later_autosave = auto_write(temp_write_path("order"), "SELECT 3;");

        assert!(queue.push(autosave));
        assert!(queue.next_to_start().is_some());

        assert!(!queue.push(explicit));
        assert!(!queue.push(later_autosave));

        queue.mark_finished();
        let first = queue.next_to_start().expect("the explicit save must run");
        assert!(
            matches!(first.kind, WriteKind::Explicit),
            "the explicit save must land before any later autosave"
        );

        queue.mark_finished();
        let second = queue.next_to_start().expect("the later autosave must run");
        assert_eq!(second.content, "SELECT 3;");
    }

    /// Two explicit saves queue in order behind a running write; neither is
    /// coalesced away.
    #[test]
    fn two_explicit_saves_keep_their_order() {
        let mut queue = PhysicalWriteQueue::new();
        assert!(queue.push(auto_write(temp_write_path("two"), "A;")));
        assert!(queue.next_to_start().is_some());

        assert!(!queue.push(explicit_write(temp_write_path("two"), "B;")));
        assert!(!queue.push(explicit_write(temp_write_path("two"), "C;")));

        queue.mark_finished();
        let first = queue.next_to_start().expect("the first save must run");
        assert_eq!(first.content, "B;");
        queue.mark_finished();
        let second = queue.next_to_start().expect("the second save must run");
        assert_eq!(second.content, "C;");
    }

    /// While a close flush is running or waiting the queue reports it, so a
    /// repeated close can stay a no-op without stacking another flush. A running
    /// autosave never counts: closing must still queue its flush behind one.
    #[test]
    fn the_queue_reports_a_running_or_waiting_close_flush() {
        let path = temp_write_path("close-flush-pending");
        let mut queue = PhysicalWriteQueue::new();

        assert!(
            !queue.has_pending_close_flush(),
            "an empty queue has no close flush"
        );

        assert!(queue.push(auto_write(path.clone(), "AUTO;")));
        assert!(
            queue.has_pending() && !queue.has_pending_close_flush(),
            "an autosave is not a close flush, so closing may still queue behind it"
        );
        assert!(queue.next_to_start().is_some());
        assert!(
            !queue.has_pending_close_flush(),
            "a running autosave is still not a close flush"
        );

        assert!(!queue.push(PhysicalWrite::close_flush(
            path.clone(),
            "MINE;".to_string(),
            "MINE;".to_string(),
        )));
        assert!(
            queue.has_pending_close_flush(),
            "a waiting close flush is reported"
        );

        queue.mark_finished();
        let running = queue.next_to_start().expect("the close flush starts");
        assert!(matches!(running.kind, WriteKind::CloseFlush));
        assert!(
            queue.has_pending_close_flush(),
            "a running close flush is reported"
        );

        queue.mark_finished();
        assert!(
            !queue.has_pending_close_flush(),
            "an idle queue has no close flush"
        );
    }

    /// An autosave must not write over bytes another process put on disk.
    #[test]
    fn an_autosave_refuses_to_write_over_foreign_bytes() {
        let path = temp_write_path("conflict");
        std::fs::write(&path, "FOREIGN;").expect("seed the foreign file");

        let executed = execute_write(
            auto_write(path.clone(), "MINE;"),
            Some(&baseline(&path, "OWNED;")),
        );

        assert!(
            matches!(executed.outcome, WriteOutcome::ExternalConflict),
            "foreign bytes must be detected against the baseline"
        );
        assert!(
            matches!(executed.new_baseline, None),
            "a refused write must not adopt a baseline"
        );
        assert_eq!(
            std::fs::read_to_string(&path).expect("the file must survive"),
            "FOREIGN;",
            "the foreign bytes must stay on disk"
        );

        std::fs::remove_file(&path).expect("the temp file must be removable");
    }

    /// An autosave must not recreate a file that vanished after the document
    /// wrote it.
    #[test]
    fn an_autosave_refuses_to_recreate_a_deleted_file() {
        let path = temp_write_path("deleted");

        let executed = execute_write(
            auto_write(path.clone(), "MINE;"),
            Some(&baseline(&path, "OWNED;")),
        );

        assert!(matches!(executed.outcome, WriteOutcome::ExternallyDeleted));
        assert!(!path.exists(), "the file must stay deleted");
    }

    /// When the disk still holds the document's own bytes, the autosave lands
    /// and the document adopts the written bytes as its new baseline.
    #[test]
    fn a_landed_autosave_adopts_the_written_bytes_as_baseline() {
        let path = temp_write_path("land");
        std::fs::write(&path, "OWNED;").expect("seed our own bytes");

        let executed = execute_write(
            auto_write(path.clone(), "NEWER;"),
            Some(&baseline(&path, "OWNED;")),
        );

        assert!(matches!(executed.outcome, WriteOutcome::Written));
        let adopted = executed
            .new_baseline
            .expect("a landed write adopts a baseline");
        assert_eq!(
            adopted.path, path,
            "the baseline stays paired with its path"
        );
        assert_eq!(
            adopted.bytes, "NEWER;",
            "the written bytes become the new baseline"
        );
        assert_eq!(
            std::fs::read_to_string(&path).expect("the write must land"),
            "NEWER;"
        );

        std::fs::remove_file(&path).expect("the temp file must be removable");
    }

    /// An autosave with no trustworthy loaded baseline must refuse rather than
    /// blindly create or overwrite the file; nothing the user typed is written,
    /// so the document keeps the edits pending instead.
    #[test]
    fn an_autosave_without_a_baseline_refuses_and_creates_no_file() {
        let path = temp_write_path("unknown-baseline");

        let executed = execute_write(auto_write(path.clone(), "MINE;"), None);

        assert!(
            executed.new_baseline.is_none(),
            "a refused write must not adopt a baseline"
        );
        assert!(
            !path.exists(),
            "an autosave with no loaded baseline must not create the file"
        );
    }

    /// A baseline belongs to the path it was read from: bytes loaded from one
    /// file must never authorize a write to a different file, even when the
    /// target already holds those bytes.
    #[test]
    fn an_autosave_refuses_a_baseline_belonging_to_another_path() {
        let loaded_path = temp_write_path("baseline-source");
        let target_path = temp_write_path("baseline-target");
        std::fs::write(&target_path, "OWNED;").expect("seed the target file");

        let executed = execute_write(
            auto_write(target_path.clone(), "MINE;"),
            Some(&baseline(&loaded_path, "OWNED;")),
        );

        assert!(
            matches!(executed.outcome, WriteOutcome::BaselineUnknown),
            "a baseline from another path must not authorize this write"
        );
        assert_eq!(
            std::fs::read_to_string(&target_path).expect("the target must survive"),
            "OWNED;",
            "the target must stay untouched"
        );

        std::fs::remove_file(&target_path).expect("the temp file must be removable");
    }

    /// F1: when staging setup fails after the staging file exists, this attempt
    /// removes the empty temporary it created instead of leaking it.
    #[test]
    fn a_failed_staging_setup_removes_its_own_staging_file() {
        let directory = temp_write_directory("staging-setup");
        let staged_path = directory.join(".dbflux-stage-test.tmp");

        inject_staging_setup_fault();
        let result = create_staging_file(&staged_path);

        assert!(result.is_err(), "the injected setup failure must surface");
        assert!(
            !staged_path.exists(),
            "a staging setup failure must remove the file this attempt created"
        );
        assert!(staging_leftovers(&directory).is_empty());

        std::fs::remove_dir_all(&directory).expect("the temp directory must be removable");
    }

    /// F1: a create_new name collision fails before the file belongs to this
    /// attempt, so an existing path is never deleted.
    #[test]
    fn a_staging_name_collision_never_removes_the_existing_file() {
        let directory = temp_write_directory("staging-collision");
        let collided_path = directory.join(".dbflux-stage-existing.tmp");
        std::fs::write(&collided_path, "PREEXISTING;").expect("seed the collided file");

        let result = create_staging_file(&collided_path);

        assert!(
            result.is_err(),
            "create_new must refuse to reuse an existing path"
        );
        assert_eq!(
            std::fs::read_to_string(&collided_path).expect("the existing file must survive"),
            "PREEXISTING;",
            "a collision must never delete a file this attempt did not create"
        );

        std::fs::remove_dir_all(&directory).expect("the temp directory must be removable");
    }

    /// An explicit save is an intentional overwrite: foreign bytes on disk do
    /// not stop it, and it adopts the written bytes as the new baseline.
    #[test]
    fn an_explicit_save_overwrites_foreign_bytes() {
        let path = temp_write_path("explicit");
        std::fs::write(&path, "FOREIGN;").expect("seed the foreign file");

        let executed = execute_write(
            explicit_write(path.clone(), "MINE;"),
            Some(&baseline(&path, "OWNED;")),
        );

        assert!(matches!(executed.outcome, WriteOutcome::Written));
        assert_eq!(
            std::fs::read_to_string(&path).expect("the write must land"),
            "MINE;",
            "a deliberate Ctrl+S overwrites whatever is on disk"
        );
        assert_eq!(
            executed
                .new_baseline
                .expect("a landed save adopts a baseline")
                .bytes,
            "MINE;"
        );

        std::fs::remove_file(&path).expect("the temp file must be removable");
    }

    /// A write that fails partway through must not damage the bytes already on
    /// disk.
    #[test]
    fn a_partial_write_failure_leaves_the_original_bytes_intact() {
        let directory = temp_write_directory("partial");
        let path = directory.join("script.sql");
        std::fs::write(&path, "ORIGINAL;").expect("seed the original file");

        inject_physical_write_fault(partial_write_fault);
        let executed = execute_write(explicit_write(path.clone(), "REPLACEMENT;"), None);

        assert!(
            matches!(executed.outcome, WriteOutcome::Failed(_)),
            "the injected partial write must surface as a failure"
        );
        assert_eq!(
            std::fs::read_to_string(&path).expect("the original must be readable"),
            "ORIGINAL;",
            "a failed write must leave the original bytes on disk"
        );
        assert!(
            staging_leftovers(&directory).is_empty(),
            "a failed write must not leave staging files behind"
        );

        std::fs::remove_dir_all(&directory).expect("the temp directory must be removable");
    }

    /// A staged replacement keeps the file's permissions and leaves nothing
    /// behind once it lands.
    #[cfg(unix)]
    #[test]
    fn a_staged_replacement_preserves_permissions_and_leaves_no_staging_file() {
        use std::os::unix::fs::PermissionsExt;

        let directory = temp_write_directory("permissions");
        let path = directory.join("script.sql");
        std::fs::write(&path, "ORIGINAL;").expect("seed the original file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640))
            .expect("the seeded permissions must apply");

        let executed = execute_write(explicit_write(path.clone(), "REPLACEMENT;"), None);

        assert!(matches!(executed.outcome, WriteOutcome::Written));
        assert_eq!(
            std::fs::read_to_string(&path).expect("the write must land"),
            "REPLACEMENT;"
        );
        let mode = std::fs::metadata(&path)
            .expect("the replaced file must exist")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            mode, 0o640,
            "a staged replacement must keep the original permissions"
        );
        assert!(
            staging_leftovers(&directory).is_empty(),
            "a landed write must not leave staging files behind"
        );

        std::fs::remove_dir_all(&directory).expect("the temp directory must be removable");
    }

    /// A brand-new file keeps the permissions a plain write would give it, not
    /// the private ones its staging file carries.
    #[cfg(unix)]
    #[test]
    fn a_new_document_keeps_plain_creation_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let directory = temp_write_directory("creation");
        let path = directory.join("script.sql");
        let reference = directory.join("reference.sql");
        std::fs::write(&reference, "REFERENCE;").expect("seed the reference file");

        let executed = execute_write(explicit_write(path.clone(), "CREATED;"), None);

        assert!(matches!(executed.outcome, WriteOutcome::Written));
        assert_eq!(
            std::fs::read_to_string(&path).expect("the file must be created"),
            "CREATED;"
        );
        let created_mode = std::fs::metadata(&path)
            .expect("the created file must exist")
            .permissions()
            .mode()
            & 0o777;
        let reference_mode = std::fs::metadata(&reference)
            .expect("the reference file must exist")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            created_mode, reference_mode,
            "a staged creation must keep the plain creation permissions"
        );

        std::fs::remove_dir_all(&directory).expect("the temp directory must be removable");
    }

    /// A read-only file is refused, not replaced through its directory.
    #[cfg(unix)]
    #[test]
    fn a_read_only_file_is_not_replaced() {
        use std::os::unix::fs::PermissionsExt;

        let directory = temp_write_directory("read-only");
        let path = directory.join("script.sql");
        std::fs::write(&path, "ORIGINAL;").expect("seed the original file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))
            .expect("the seeded permissions must apply");

        if std::fs::write(&path, "PROBE;").is_ok() {
            // A privileged process can write read-only files, so the refusal
            // this test asserts does not apply here.
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                .expect("the temp file must stay removable");
            std::fs::remove_dir_all(&directory).expect("the temp directory must be removable");
            return;
        }

        let executed = execute_write(explicit_write(path.clone(), "REPLACEMENT;"), None);

        let failure = match executed.outcome {
            WriteOutcome::Failed(error) => error,
            _ => panic!("a read-only file must be refused"),
        };
        assert_eq!(failure.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(
            std::fs::read_to_string(&path).expect("the original must be readable"),
            "ORIGINAL;",
            "a refused write must leave the original bytes on disk"
        );
        assert!(
            staging_leftovers(&directory).is_empty(),
            "a refused write must not even stage bytes"
        );

        std::fs::remove_dir_all(&directory).expect("the temp directory must be removable");
    }

    /// A symlinked document keeps its link and receives its bytes through it.
    #[cfg(unix)]
    #[test]
    fn a_symlinked_document_writes_through_the_link() {
        let directory = temp_write_directory("symlink");
        let target = directory.join("real.sql");
        let link = directory.join("link.sql");
        std::fs::write(&target, "ORIGINAL;").expect("seed the original file");
        std::os::unix::fs::symlink(&target, &link).expect("the symlink must be creatable");

        let executed = execute_write(explicit_write(link.clone(), "REPLACEMENT;"), None);

        assert!(matches!(executed.outcome, WriteOutcome::Written));
        assert_eq!(
            std::fs::read_to_string(&target).expect("the target must be readable"),
            "REPLACEMENT;",
            "the bytes must land in the file the link points at"
        );
        let link_metadata = std::fs::symlink_metadata(&link).expect("the link must exist");
        assert!(
            link_metadata.file_type().is_symlink(),
            "a staged replacement must not turn the link into a regular file"
        );
        assert_eq!(
            std::fs::read_link(&link).expect("the link must be readable"),
            target,
            "the link must keep pointing at its target"
        );
        assert!(
            staging_leftovers(&directory).is_empty(),
            "a landed write must not leave staging files behind"
        );

        std::fs::remove_dir_all(&directory).expect("the temp directory must be removable");
    }

    /// Retargeting adopts the chosen path's baseline. An autosave still waiting
    /// for the previous path can never land against that baseline, so it is
    /// discarded at the retarget instead of being attempted and refused.
    #[test]
    fn a_waiting_autosave_for_a_retargeted_path_is_discarded() {
        let previous_path = temp_write_path("retarget-previous");
        let chosen_path = temp_write_path("retarget-chosen");
        let mut queue = PhysicalWriteQueue::new();

        let save_as = PhysicalWrite::save_as(
            chosen_path.clone(),
            "SAVED;".to_string(),
            "SAVED;".to_string(),
            false,
        );
        assert!(
            queue.push(save_as),
            "an idle queue starts the write at once"
        );
        assert!(
            queue.next_to_start().is_some(),
            "the Save As starts running"
        );

        assert!(
            !queue.push(auto_write(previous_path.clone(), "STALE;")),
            "the autosave waits behind the running Save As"
        );

        // Save As lands: the document adopts the chosen path's baseline.
        queue.adopt_baseline(Some(baseline(&chosen_path, "SAVED;")));
        queue.mark_finished();

        assert!(
            queue.next_to_start().is_none(),
            "an autosave for the previous path must not run against the new baseline"
        );
    }

    /// An explicit save is never dropped by a retarget: it is an intentional user
    /// action, unlike the stale autosave the retarget discards.
    #[test]
    fn a_waiting_explicit_save_survives_a_retarget() {
        let previous_path = temp_write_path("explicit-previous");
        let chosen_path = temp_write_path("explicit-chosen");
        let mut queue = PhysicalWriteQueue::new();

        let save_as = PhysicalWrite::save_as(
            chosen_path.clone(),
            "SAVED;".to_string(),
            "SAVED;".to_string(),
            false,
        );
        assert!(queue.push(save_as));
        assert!(queue.next_to_start().is_some());

        assert!(!queue.push(explicit_write(previous_path.clone(), "EXPLICIT;")));

        queue.adopt_baseline(Some(baseline(&chosen_path, "SAVED;")));
        queue.mark_finished();

        let next = queue
            .next_to_start()
            .expect("an explicit save is never dropped by a retarget");
        assert!(matches!(next.kind, WriteKind::Explicit));
    }
}
