//! Data-layer support for the recursive-delete confirmation modal.
//!
//! T35 scope: the probe, the confirm-state model, and the execution path.
//! Rendering the type-to-confirm modal itself is T36 — this module has no UI
//! wiring, only the state `render.rs` will read once that lands.
//!
//! `PrefixDeleteProbe` counts objects and total size under a prefix (or the
//! whole bucket, when the target is empty) via bounded, cancellable
//! `list_objects` pagination — a self-contained recursive walk, unlike
//! `tree.rs`'s lazy per-node expansion, because a delete confirmation needs
//! the true recursive total up front rather than only what the user has
//! chosen to expand. `list_objects` only ever returns one delimiter level at
//! a time. The first few affected keys are kept for the modal's preview list.

use super::ObjectBrowserDocument;
use super::data::db_error_to_user_facing;
use dbflux_core::{DbError, ObjectListingPage, VersioningStatus};
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::Context;
use std::collections::VecDeque;
use uuid::Uuid;

/// Bound on how many `ListObjectsV2` pages a probe walks before stopping
/// early and reporting `Capped`; a probe over a huge prefix must not run
/// unbounded.
pub const DELETE_PREFIX_PROBE_PAGE_CAP: u32 = 200;

/// How many of the affected keys the modal shows as a preview (Amendment E).
pub const DELETE_PREFIX_PREVIEW_KEYS: usize = 5;

/// Status of a probe walk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum DeletePrefixProbeState {
    #[default]
    Idle,
    Running,
    Done,
    Cancelled,
    Capped,
    Error(String),
}

/// Pure accumulator for a recursive-delete probe, applied one
/// `ObjectListingPage` at a time — testable without a connection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PrefixDeleteProbe {
    pub target: String,
    pub object_count: u64,
    pub total_bytes: u64,
    pub first_keys: Vec<String>,
    pub pages_walked: u32,
    pub state: DeletePrefixProbeState,
    generation: u64,
}

/// Result of applying one page to a probe. The walker uses
/// `discovered_prefixes` and `continuation_token` to decide what to list
/// next.
#[derive(Clone, Debug, PartialEq)]
pub struct PrefixDeleteProbeOutcome {
    /// `false` when the page belonged to a superseded generation (the probe
    /// was cancelled or restarted) and was therefore ignored.
    pub applied: bool,
    /// `true` once the page cap was reached; the walker must stop.
    pub capped: bool,
    pub discovered_prefixes: Vec<String>,
    pub continuation_token: Option<String>,
}

impl PrefixDeleteProbe {
    /// Starts a fresh probe over `target`, invalidating any probe already in
    /// flight. Returns the new generation the caller must pass back into
    /// `apply_page`.
    pub fn start(&mut self, target: String) -> u64 {
        self.generation += 1;
        self.target = target;
        self.object_count = 0;
        self.total_bytes = 0;
        self.first_keys.clear();
        self.pages_walked = 0;
        self.state = DeletePrefixProbeState::Running;
        self.generation
    }

    /// Cancels the current walk. Bumps the generation so any page still in
    /// flight from the cancelled walk is ignored when it lands.
    pub fn cancel(&mut self) {
        self.generation += 1;
        self.state = DeletePrefixProbeState::Cancelled;
    }

    pub fn is_current(&self, generation: u64) -> bool {
        self.state == DeletePrefixProbeState::Running && self.generation == generation
    }

    pub fn mark_error(&mut self, generation: u64, message: String) {
        if self.generation == generation {
            self.state = DeletePrefixProbeState::Error(message);
        }
    }

    pub fn mark_done(&mut self, generation: u64) {
        if self.is_current(generation) {
            self.state = DeletePrefixProbeState::Done;
        }
    }

    /// Folds one `ListObjectsV2` page into the running totals.
    pub fn apply_page(
        &mut self,
        generation: u64,
        page: ObjectListingPage,
    ) -> PrefixDeleteProbeOutcome {
        if !self.is_current(generation) {
            return PrefixDeleteProbeOutcome {
                applied: false,
                capped: false,
                discovered_prefixes: Vec::new(),
                continuation_token: None,
            };
        }

        self.pages_walked += 1;

        for object in &page.objects {
            self.object_count += 1;
            self.total_bytes += object.size_bytes;

            if self.first_keys.len() < DELETE_PREFIX_PREVIEW_KEYS {
                self.first_keys.push(object.key.clone());
            }
        }

        let capped = self.pages_walked >= DELETE_PREFIX_PROBE_PAGE_CAP;
        if capped {
            self.state = DeletePrefixProbeState::Capped;
        }

        PrefixDeleteProbeOutcome {
            applied: true,
            capped,
            discovered_prefixes: if capped {
                Vec::new()
            } else {
                page.common_prefixes
            },
            continuation_token: if capped {
                None
            } else {
                page.next_continuation_token
            },
        }
    }
}

/// State the type-to-confirm recursive-delete modal (T36) renders: the
/// probe's running/finished totals, the exact phrase the user must type, and
/// a versioning note when the bucket tracks history (DEC-19).
#[derive(Clone, Debug, PartialEq)]
pub struct DeletePrefixConfirmState {
    /// The prefix being deleted, or `""` for a whole-bucket delete.
    pub target: String,
    /// The exact phrase the user must type to unlock the Delete button — the
    /// prefix itself, or the bucket name for a whole-bucket delete.
    pub expected_phrase: String,
    pub probe: PrefixDeleteProbe,
    pub versioning_note: Option<String>,
}

impl DeletePrefixConfirmState {
    pub fn new(bucket: &str, target: String, versioning: Option<VersioningStatus>) -> Self {
        let expected_phrase = if target.is_empty() {
            bucket.to_string()
        } else {
            target.clone()
        };

        Self {
            target,
            expected_phrase,
            probe: PrefixDeleteProbe::default(),
            versioning_note: versioning_note(versioning),
        }
    }

    /// Whether the typed phrase unlocks the Delete button.
    pub fn confirmation_matches(&self, typed: &str) -> bool {
        typed == self.expected_phrase
    }
}

fn versioning_note(versioning: Option<VersioningStatus>) -> Option<String> {
    match versioning {
        Some(VersioningStatus::Enabled) | Some(VersioningStatus::Suspended) => Some(
            dbflux_i18n::t!("document.object_browser.delete_prefix.versioning_note"),
        ),
        _ => None,
    }
}

impl ObjectBrowserDocument {
    pub fn delete_prefix_confirm(&self) -> Option<&DeletePrefixConfirmState> {
        self.delete_prefix_confirm.as_ref()
    }

    /// Starts a bounded, cancellable probe over `target` — a prefix, or the
    /// whole bucket when `target` is empty — feeding the confirm state as
    /// pages arrive. `versioning` comes from the document's already-fetched
    /// `BucketDetails` (DEC-19); pass `None` when it has not resolved yet.
    pub fn start_delete_prefix_probe(
        &mut self,
        target: String,
        versioning: Option<VersioningStatus>,
        cx: &mut Context<Self>,
    ) {
        let mut confirm = DeletePrefixConfirmState::new(&self.bucket, target.clone(), versioning);
        let generation = confirm.probe.start(target.clone());
        self.delete_prefix_confirm = Some(confirm);
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            self.mark_probe_error(
                generation,
                dbflux_i18n::t!("document.object_browser.error.connection_unavailable"),
            );
            cx.notify();
            return;
        };

        let bucket = self.bucket.clone();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let mut queue: VecDeque<(String, Option<String>)> = VecDeque::new();
            queue.push_back((target, None));

            while let Some((prefix, token)) = queue.pop_front() {
                let still_current = cx
                    .update(|cx| {
                        entity
                            .read(cx)
                            .delete_prefix_confirm
                            .as_ref()
                            .is_some_and(|confirm| confirm.probe.is_current(generation))
                    })
                    .unwrap_or(false);

                if !still_current {
                    return;
                }

                let connection = connection.clone();
                let bucket_for_call = bucket.clone();
                let prefix_for_call = prefix.clone();

                let page_result = cx
                    .background_executor()
                    .spawn(async move {
                        match connection.object_store_api() {
                            Some(api) => api.list_objects(
                                &bucket_for_call,
                                &prefix_for_call,
                                token.as_deref(),
                            ),
                            None => Err(DbError::NotSupported(dbflux_i18n::t!(
                                "document.object_browser.error.api_unavailable"
                            ))),
                        }
                    })
                    .await;

                let page = match page_result {
                    Ok(page) => page,
                    Err(err) => {
                        report_error_async(db_error_to_user_facing(&err), cx);
                        cx.update(|cx| {
                            entity.update(cx, |doc, cx| {
                                doc.mark_probe_error(generation, err.to_string());
                                cx.notify();
                            });
                        })
                        .ok();
                        return;
                    }
                };

                let outcome = cx
                    .update(|cx| {
                        entity.update(cx, |doc, cx| {
                            let outcome = doc
                                .delete_prefix_confirm
                                .as_mut()
                                .map(|confirm| confirm.probe.apply_page(generation, page));
                            cx.notify();
                            outcome
                        })
                    })
                    .ok()
                    .flatten();

                let Some(outcome) = outcome else { return };

                if !outcome.applied || outcome.capped {
                    break;
                }

                if let Some(next_token) = outcome.continuation_token {
                    queue.push_back((prefix.clone(), Some(next_token)));
                }

                for sub_prefix in outcome.discovered_prefixes {
                    queue.push_back((sub_prefix, None));
                }
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    if let Some(confirm) = doc.delete_prefix_confirm.as_mut() {
                        confirm.probe.mark_done(generation);
                    }
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    fn mark_probe_error(&mut self, generation: u64, message: String) {
        if let Some(confirm) = self.delete_prefix_confirm.as_mut() {
            confirm.probe.mark_error(generation, message);
        }
    }

    /// Cancels an in-flight probe without discarding the modal — the user is
    /// still looking at the (now frozen) partial totals.
    pub fn cancel_delete_prefix_probe(&mut self, cx: &mut Context<Self>) {
        if let Some(confirm) = self.delete_prefix_confirm.as_mut() {
            confirm.probe.cancel();
        }
        cx.notify();
    }

    /// Dismisses the confirmation modal, cancelling any probe still running.
    pub fn close_delete_prefix_confirm(&mut self, cx: &mut Context<Self>) {
        self.cancel_delete_prefix_probe(cx);
        self.delete_prefix_confirm = None;
        self.delete_prefix_input = None;
        cx.notify();
    }

    /// Executes the recursive delete once the modal's type-to-confirm gate is
    /// satisfied (`DeletePrefixConfirmState::confirmation_matches`). Toasts
    /// and audits the outcome, then refreshes the current level.
    pub fn execute_delete_prefix(&mut self, target: String, cx: &mut Context<Self>) {
        self.delete_prefix_confirm = None;
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("document.object_browser.error.connection_unavailable"),
                ),
                cx,
            );
            return;
        };

        let audit_service = self.app_state.read(cx).audit_service().clone();
        let bucket = self.bucket.clone();
        let profile_id = self.profile_id;
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn({
                    let bucket = bucket.clone();
                    let target = target.clone();
                    async move {
                        let api = connection.object_store_api().ok_or_else(|| {
                            DbError::NotSupported(dbflux_i18n::t!(
                                "document.object_browser.error.api_unavailable"
                            ))
                        })?;
                        api.delete_prefix(&bucket, &target)
                    }
                })
                .await;

            record_prefix_delete_audit(
                &audit_service,
                profile_id,
                &bucket,
                &target,
                result.as_ref().ok().map(|outcome| outcome.deleted_count),
                result.as_ref().err().map(ToString::to_string).as_deref(),
            );

            match &result {
                Ok(outcome) => {
                    cx.update(|cx| {
                        Toast::success(crate::labels::delete_prefix_deleted_toast(
                            outcome.deleted_count,
                            &format!("s3://{bucket}/{target}"),
                        ))
                        .meta_right(now_hms())
                        .push(cx);
                    })
                    .ok();
                }
                Err(err) => report_error_async(db_error_to_user_facing(err), cx),
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| doc.reload_current_prefix(cx));
            })
            .ok();
        })
        .detach();
    }
}

/// Audits a recursive-prefix delete. Never records the affected keys — only
/// the target and the outcome.
fn record_prefix_delete_audit(
    audit_service: &dbflux_audit::AuditService,
    profile_id: Uuid,
    bucket: &str,
    target: &str,
    deleted_count: Option<u64>,
    error: Option<&str>,
) {
    use dbflux_core::chrono::Utc;
    use dbflux_core::observability::{
        EventCategory, EventOutcome, EventRecord, EventSeverity, EventSink,
    };

    let (severity, outcome) = match error {
        Some(_) => (EventSeverity::Error, EventOutcome::Failure),
        None => (EventSeverity::Info, EventOutcome::Success),
    };

    let mut summary = format!("Deleted prefix s3://{bucket}/{target}");
    if let Some(count) = deleted_count {
        summary.push_str(&format!(" ({count} objects)"));
    }
    if let Some(error) = error {
        summary.push_str(&format!(": {error}"));
    }

    let event = EventRecord::new(
        Utc::now().timestamp_millis(),
        severity,
        EventCategory::ObjectStorage,
        outcome,
    )
    .with_action("prefix_delete".to_string())
    .with_summary(summary)
    .with_actor_id("ui:user")
    .with_object_ref("prefix", format!("{bucket}/{target}"))
    .with_connection_context(profile_id.to_string(), bucket.to_string(), String::new());

    if let Err(e) = audit_service.record(event) {
        log::warn!("[object browser] failed to record prefix-delete audit event: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DELETE_PREFIX_PREVIEW_KEYS, DeletePrefixConfirmState, DeletePrefixProbeState,
        PrefixDeleteProbe,
    };
    use dbflux_core::{ObjectListingPage, ObjectSummary, VersioningStatus};

    fn page(prefixes: &[&str], objects: &[(&str, u64)]) -> ObjectListingPage {
        ObjectListingPage {
            objects: objects
                .iter()
                .map(|(key, size)| ObjectSummary {
                    key: key.to_string(),
                    size_bytes: *size,
                    storage_class: None,
                    last_modified: None,
                })
                .collect(),
            common_prefixes: prefixes.iter().map(|p| p.to_string()).collect(),
            next_continuation_token: None,
        }
    }

    /// A fresh probe accumulates counts, size, and a bounded set of preview
    /// keys across pages, and discovers sub-prefixes to enqueue.
    #[test]
    fn apply_page_accumulates_totals_and_discovers_prefixes() {
        let mut probe = PrefixDeleteProbe::default();
        let generation = probe.start("archive/".to_string());

        let outcome = probe.apply_page(
            generation,
            page(&["archive/2026/"], &[("archive/a.txt", 10)]),
        );

        assert!(outcome.applied);
        assert!(!outcome.capped);
        assert_eq!(
            outcome.discovered_prefixes,
            vec!["archive/2026/".to_string()]
        );
        assert_eq!(probe.object_count, 1);
        assert_eq!(probe.total_bytes, 10);
        assert_eq!(probe.first_keys, vec!["archive/a.txt".to_string()]);
    }

    /// The preview list stops growing once it holds the configured cap, even
    /// though `object_count`/`total_bytes` keep accumulating.
    #[test]
    fn preview_keys_are_capped_but_totals_keep_growing() {
        let mut probe = PrefixDeleteProbe::default();
        let generation = probe.start(String::new());

        let objects: Vec<(&str, u64)> = (0..DELETE_PREFIX_PREVIEW_KEYS + 3)
            .map(|i| {
                (
                    Box::leak(format!("key-{i}.txt").into_boxed_str()) as &str,
                    1,
                )
            })
            .collect();

        probe.apply_page(generation, page(&[], &objects));

        assert_eq!(probe.first_keys.len(), DELETE_PREFIX_PREVIEW_KEYS);
        assert_eq!(probe.object_count, (DELETE_PREFIX_PREVIEW_KEYS + 3) as u64);
    }

    /// A page applied under a stale (cancelled/restarted) generation is
    /// ignored — the totals it carried never land.
    #[test]
    fn stale_generation_pages_are_ignored() {
        let mut probe = PrefixDeleteProbe::default();
        let generation = probe.start("logs/".to_string());
        probe.cancel();

        let outcome = probe.apply_page(generation, page(&[], &[("logs/a.txt", 5)]));

        assert!(!outcome.applied);
        assert_eq!(probe.object_count, 0);
        assert_eq!(probe.state, DeletePrefixProbeState::Cancelled);
    }

    /// Restarting a probe clears whatever the previous run had accumulated
    /// and bumps the generation.
    #[test]
    fn restarting_clears_prior_totals_and_bumps_generation() {
        let mut probe = PrefixDeleteProbe::default();
        let gen1 = probe.start("logs/".to_string());
        probe.apply_page(gen1, page(&[], &[("logs/a.txt", 5)]));
        assert_eq!(probe.object_count, 1);

        let gen2 = probe.start("logs/".to_string());
        assert_ne!(gen1, gen2);
        assert_eq!(probe.object_count, 0);
        assert!(probe.first_keys.is_empty());
    }

    /// The expected phrase is the prefix for a scoped delete, and the bucket
    /// name for a whole-bucket delete (empty target).
    #[test]
    fn expected_phrase_is_prefix_or_bucket_name() {
        let scoped = DeletePrefixConfirmState::new("my-bucket", "logs/2026/".to_string(), None);
        assert_eq!(scoped.expected_phrase, "logs/2026/");
        assert!(scoped.confirmation_matches("logs/2026/"));
        assert!(!scoped.confirmation_matches("logs/2026"));

        let whole_bucket = DeletePrefixConfirmState::new("my-bucket", String::new(), None);
        assert_eq!(whole_bucket.expected_phrase, "my-bucket");
    }

    /// The versioning note only appears for versioned/suspended buckets —
    /// disabled versioning (or an unresolved status) shows nothing.
    #[test]
    fn versioning_note_only_appears_when_the_bucket_tracks_history() {
        let enabled = DeletePrefixConfirmState::new(
            "my-bucket",
            "logs/".to_string(),
            Some(VersioningStatus::Enabled),
        );
        assert!(enabled.versioning_note.is_some());

        let suspended = DeletePrefixConfirmState::new(
            "my-bucket",
            "logs/".to_string(),
            Some(VersioningStatus::Suspended),
        );
        assert!(suspended.versioning_note.is_some());

        let disabled = DeletePrefixConfirmState::new(
            "my-bucket",
            "logs/".to_string(),
            Some(VersioningStatus::Disabled),
        );
        assert!(disabled.versioning_note.is_none());

        let unknown = DeletePrefixConfirmState::new("my-bucket", "logs/".to_string(), None);
        assert!(unknown.versioning_note.is_none());
    }
}
