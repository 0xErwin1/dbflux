//! Bulk delete of every key matching the current pattern and type filter.
//!
//! Opening the confirmation starts a full scan of the keyspace, so the modal
//! states the real number of matches rather than what the key list happened
//! to load. Deleting requires typing the pattern back, can export the keys
//! first, runs as non-blocking deletes in fixed-size batches, and is
//! recorded in the audit log.

use super::parsing::key_type_label;
use dbflux_components::primitives::TypeToConfirm;
use dbflux_core::{
    DbError, KeyBulkDeleteRequest, KeyBulkGetRequest, KeyEntry, KeyMetadataRequest, KeyScanRequest,
    KeyType, KeyValueApi, KeyValueFeatures, TaskKind,
};
use dbflux_ui_base::SaveTargetOutcome;
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error_async};
use gpui::*;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;
use uuid::Uuid;

/// Keys removed per delete round trip.
pub(super) const BULK_DELETE_BATCH_SIZE: usize = 100;

/// Matches listed by name in the confirmation.
pub(super) const BULK_DELETE_PREVIEW_KEYS: usize = 3;

/// Keys requested per `SCAN` page while counting matches.
const BULK_SCAN_PAGE: u32 = 1_000;

/// Keys read per round trip while exporting.
const EXPORT_BATCH_SIZE: usize = 100;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum BulkScanState {
    Scanning,
    Done,
    Cancelled,
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum BulkDeleteStage {
    Confirming,
    Exporting,
    Deleting,
}

/// State of the open bulk-delete confirmation.
pub(super) struct BulkDeleteState {
    /// Pattern as the user sees and must type it back (`*` for all keys).
    pub pattern: String,
    pub type_filter: Option<KeyType>,
    pub matches: Vec<KeyEntry>,
    pub preview_ttls: HashMap<String, Option<i64>>,
    pub scan_state: BulkScanState,
    pub stage: BulkDeleteStage,
    pub exported_to: Option<PathBuf>,
    pub confirm_input: Option<Entity<TypeToConfirm>>,
    generation: u64,
}

impl BulkDeleteState {
    pub(super) fn is_current(&self, generation: u64) -> bool {
        self.generation == generation && self.scan_state == BulkScanState::Scanning
    }

    /// Whether the Delete button may run.
    pub(super) fn can_delete(&self, typed: &str) -> bool {
        self.scan_state == BulkScanState::Done
            && self.stage == BulkDeleteStage::Confirming
            && !self.matches.is_empty()
            && typed == self.pattern
    }

    /// Adds one scan page, skipping keys `SCAN` already returned.
    pub(super) fn add_page(&mut self, entries: Vec<KeyEntry>, seen: &mut HashSet<String>) {
        for entry in entries {
            if seen.insert(entry.key.clone()) {
                self.matches.push(entry);
            }
        }
    }
}

/// Deletes `keys` in batches of `batch_size` through `delete_keys`.
///
/// Returns the number of keys removed. When a batch fails, the error carries
/// how many keys the earlier batches had already removed.
#[allow(clippy::result_large_err)]
pub(super) fn unlink_in_batches(
    api: &dyn KeyValueApi,
    keys: &[String],
    keyspace: Option<u32>,
    batch_size: usize,
) -> Result<u64, (u64, DbError)> {
    let mut removed = 0;

    for batch in keys.chunks(batch_size.max(1)) {
        match api.delete_keys(&KeyBulkDeleteRequest {
            keys: batch.to_vec(),
            keyspace,
        }) {
            Ok(count) => removed += count,
            Err(error) => return Err((removed, error)),
        }
    }

    Ok(removed)
}

/// Pattern shown to the user for a scan pattern (`*` when unfiltered).
pub(super) fn display_pattern(scan_pattern: Option<&str>) -> String {
    scan_pattern.unwrap_or("*").to_string()
}

/// One line of the export file: key, type, expiry and value. Text values are
/// written as strings, structured values as JSON, binary values as hex.
fn export_line(entry: &dbflux_core::KeyGetResult) -> Result<String, serde_json::Error> {
    let value = match entry.repr {
        dbflux_core::ValueRepr::Structured | dbflux_core::ValueRepr::Stream => {
            serde_json::from_slice::<serde_json::Value>(&entry.value).unwrap_or_else(|_| {
                serde_json::Value::String(String::from_utf8_lossy(&entry.value).into())
            })
        }
        dbflux_core::ValueRepr::Binary => serde_json::json!({
            "hex": entry.value.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
        }),
        dbflux_core::ValueRepr::Text | dbflux_core::ValueRepr::Json => {
            serde_json::Value::String(String::from_utf8_lossy(&entry.value).into())
        }
    };

    serde_json::to_string(&serde_json::json!({
        "key": entry.entry.key,
        "type": entry.entry.key_type.map(key_type_label),
        "ttl_seconds": entry.entry.ttl_seconds,
        "value": value,
    }))
}

impl super::KeyValueDocument {
    /// Whether the connection can delete keys in bulk.
    pub(super) fn supports_bulk_delete(&self, cx: &App) -> bool {
        let driver_allows = self
            .get_connection(cx)
            .and_then(|connection| connection.metadata().mutation.clone())
            .is_some_and(|mutation| mutation.supports_bulk_delete);

        driver_allows && self.key_features.contains(KeyValueFeatures::BULK_DELETE)
    }

    /// Opens the confirmation for the current pattern and type filter and
    /// starts the full scan that counts the matches.
    pub(super) fn open_bulk_delete(&mut self, cx: &mut Context<Self>) {
        self.bulk_actions_open = false;

        if !self.supports_bulk_delete(cx) {
            return;
        }

        let filter = self.filter_input.read(cx).value().trim().to_string();
        let scan_pattern = super::pagination::key_scan_pattern(&filter);

        self.bulk_delete_generation = self.bulk_delete_generation.wrapping_add(1);
        let generation = self.bulk_delete_generation;

        self.bulk_delete = Some(BulkDeleteState {
            pattern: display_pattern(scan_pattern.as_deref()),
            type_filter: self.type_filter,
            matches: Vec::new(),
            preview_ttls: HashMap::new(),
            scan_state: BulkScanState::Scanning,
            stage: BulkDeleteStage::Confirming,
            exported_to: None,
            confirm_input: None,
            generation,
        });
        self.pending_bulk_delete_input = true;
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            self.report_connection_inactive(cx);
            return;
        };

        let keyspace = self.keyspace_index();
        let type_filter = self.type_filter.filter(|_| {
            self.key_features
                .contains(KeyValueFeatures::SCAN_TYPE_FILTER)
        });
        let fetch_ttls = self.key_features.contains(KeyValueFeatures::KEY_METADATA);
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let mut cursor: Option<String> = None;
            let mut seen = HashSet::new();

            loop {
                let still_current = cx.update(|cx| {
                    entity
                        .read(cx)
                        .bulk_delete
                        .as_ref()
                        .is_some_and(|state| state.is_current(generation))
                });

                if !still_current {
                    return;
                }

                let request = KeyScanRequest {
                    cursor: cursor.clone(),
                    filter: scan_pattern.clone(),
                    limit: BULK_SCAN_PAGE,
                    keyspace,
                    type_filter,
                };

                let page_result = cx
                    .background_executor()
                    .spawn({
                        let connection = connection.clone();
                        async move {
                            let api = connection.key_value_api().ok_or_else(|| {
                                DbError::NotSupported("Key-value API unavailable".to_string())
                            })?;
                            api.scan_keys(&request)
                        }
                    })
                    .await;

                let page = match page_result {
                    Ok(page) => page,
                    Err(error) => {
                        report_error_async(
                            UserFacingError::new(ErrorKind::Driver, error.to_string()),
                            cx,
                        );

                        cx.update(|cx| {
                            entity.update(cx, |this, cx| {
                                if let Some(state) = this.bulk_delete.as_mut()
                                    && state.generation == generation
                                {
                                    state.scan_state = BulkScanState::Failed(error.to_string());
                                }
                                cx.notify();
                            });
                        });
                        return;
                    }
                };

                cursor = page.next_cursor.clone();
                let finished = cursor.is_none();

                cx.update(|cx| {
                    entity.update(cx, |this, cx| {
                        if let Some(state) = this.bulk_delete.as_mut()
                            && state.is_current(generation)
                        {
                            state.add_page(page.entries, &mut seen);

                            if finished {
                                state.scan_state = BulkScanState::Done;
                            }
                        }
                        cx.notify();
                    });
                });

                if finished {
                    break;
                }
            }

            if !fetch_ttls {
                return;
            }

            let preview_keys: Vec<String> = cx.update(|cx| {
                entity
                    .read(cx)
                    .bulk_delete
                    .as_ref()
                    .map(|state| {
                        state
                            .matches
                            .iter()
                            .take(BULK_DELETE_PREVIEW_KEYS)
                            .map(|entry| entry.key.clone())
                            .collect()
                    })
                    .unwrap_or_default()
            });

            if preview_keys.is_empty() {
                return;
            }

            let metadata = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;
                    api.key_metadata(&KeyMetadataRequest {
                        keys: preview_keys,
                        keyspace,
                        include_encoding: false,
                    })
                })
                .await;

            match metadata {
                Ok(metadata) => cx.update(|cx| {
                    entity.update(cx, |this, cx| {
                        if let Some(state) = this.bulk_delete.as_mut()
                            && state.generation == generation
                        {
                            for item in metadata {
                                state.preview_ttls.insert(item.key, item.ttl_seconds);
                            }
                        }
                        cx.notify();
                    });
                }),
                Err(error) => log::debug!("Bulk delete preview TTLs unavailable: {error}"),
            }
        })
        .detach();
    }

    /// Creates the type-to-confirm field on the first render after opening,
    /// since it needs a window.
    pub(super) fn ensure_bulk_delete_input(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !std::mem::take(&mut self.pending_bulk_delete_input) {
            return;
        }

        let Some(state) = self.bulk_delete.as_mut() else {
            return;
        };

        let expected = state.pattern.clone();
        let input = cx.new(|cx| TypeToConfirm::new(expected, window, cx));
        input.update(cx, |confirm, cx| confirm.focus(window, cx));

        state.confirm_input = Some(input);
        self.focus_mode = super::KeyValueFocusMode::TextInput;
    }

    pub(super) fn close_bulk_delete(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.bulk_delete = None;
        self.pending_bulk_delete_input = false;
        self.focus_mode = super::KeyValueFocusMode::List;
        self.focus_handle.focus(window, cx);
        cx.notify();
    }

    pub(super) fn stop_bulk_delete_scan(&mut self, cx: &mut Context<Self>) {
        if let Some(state) = self.bulk_delete.as_mut()
            && state.scan_state == BulkScanState::Scanning
        {
            state.scan_state = BulkScanState::Cancelled;
        }
        cx.notify();
    }

    pub(super) fn bulk_delete_typed_text(&self, cx: &App) -> String {
        self.bulk_delete
            .as_ref()
            .and_then(|state| state.confirm_input.as_ref())
            .map(|input| input.read(cx).typed_text(cx))
            .unwrap_or_default()
    }

    /// Deletes every matched key in batches, records the outcome in the
    /// audit log and reloads the key list.
    pub(super) fn confirm_bulk_delete(&mut self, cx: &mut Context<Self>) {
        let typed = self.bulk_delete_typed_text(cx);

        let Some(state) = self.bulk_delete.as_mut() else {
            return;
        };

        if !state.can_delete(&typed) {
            return;
        }

        state.stage = BulkDeleteStage::Deleting;
        let keys: Vec<String> = state
            .matches
            .iter()
            .map(|entry| entry.key.clone())
            .collect();
        let pattern = state.pattern.clone();
        let type_filter = state.type_filter;
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            self.report_connection_inactive(cx);
            return;
        };

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::KeyMutation,
            format!("UNLINK {pattern} ({} keys)", keys.len()),
            cx,
        );

        let audit_service = self.app_state.read(cx).audit_service().clone();
        let driver_id = connection.metadata().id.clone();
        let profile_id = self.profile_id;
        let database = self.database.clone();
        let keyspace = self.keyspace_index();
        let matched = keys.len() as u64;
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let started = Instant::now();

            let result = cx
                .background_executor()
                .spawn(async move {
                    let Some(api) = connection.key_value_api() else {
                        return Err((
                            0,
                            DbError::NotSupported("Key-value API unavailable".to_string()),
                        ));
                    };
                    unlink_in_batches(api, &keys, keyspace, BULK_DELETE_BATCH_SIZE)
                })
                .await;

            record_bulk_delete_audit(
                &audit_service,
                BulkDeleteAudit {
                    profile_id,
                    database: &database,
                    driver_id: &driver_id,
                    pattern: &pattern,
                    type_filter,
                    matched,
                    duration_ms: started.elapsed().as_millis() as i64,
                },
                &result,
            );

            match &result {
                Ok(removed) => cx.update(|cx| {
                    Toast::success(dbflux_i18n::t!(
                        "document.key_value.bulk_delete.toast_deleted",
                        count = removed,
                        pattern = pattern.as_str()
                    ))
                    .meta_right(now_hms())
                    .push(cx);
                }),
                Err((_, error)) => report_error_async(
                    UserFacingError::new(ErrorKind::Driver, error.to_string()),
                    cx,
                ),
            }

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    match &result {
                        Ok(_) => this.runner.complete_mutation(task_id, cx),
                        Err((_, error)) => {
                            this.runner.fail_mutation(task_id, error.to_string(), cx)
                        }
                    }

                    this.bulk_delete = None;
                    this.focus_mode = super::KeyValueFocusMode::List;
                    this.reload_keys(cx);
                });
            });
        })
        .detach();
    }

    /// Writes every matched key with its value to a JSON Lines file chosen
    /// by the user, before anything is deleted.
    pub(super) fn export_bulk_delete_matches(&mut self, cx: &mut Context<Self>) {
        let Some(state) = self.bulk_delete.as_mut() else {
            return;
        };

        if state.scan_state != BulkScanState::Done || state.stage != BulkDeleteStage::Confirming {
            return;
        }

        state.stage = BulkDeleteStage::Exporting;
        let keys: Vec<String> = state
            .matches
            .iter()
            .map(|entry| entry.key.clone())
            .collect();
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            self.report_connection_inactive(cx);
            return;
        };

        let keyspace = self.keyspace_index();
        let suggested_name = format!(
            "{}-keys.jsonl",
            self.database
                .replace(|character: char| !character.is_alphanumeric(), "-")
        );
        let save_target_override = self.app_state.read(cx).save_target_override();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let outcome = dbflux_ui_base::file_dialog::resolve_save_target(
                save_target_override,
                dbflux_ui_base::SaveTargetRequest {
                    suggested_name: &suggested_name,
                    language_name: "JSON Lines",
                    default_extension: "jsonl",
                },
                async {
                    rfd::AsyncFileDialog::new()
                        .set_title(dbflux_i18n::t!(
                            "document.key_value.bulk_delete.export_dialog_title"
                        ))
                        .set_file_name(&suggested_name)
                        .add_filter("JSON Lines", &["jsonl"])
                        .save_file()
                        .await
                        .map(|handle| handle.path().to_path_buf())
                },
            )
            .await;

            let path = match outcome {
                SaveTargetOutcome::Selected { path, .. } => Some(path),
                SaveTargetOutcome::Cancelled => None,
                SaveTargetOutcome::Failed(message) => {
                    report_error_async(UserFacingError::new(ErrorKind::Storage, message), cx);
                    None
                }
            };

            let result = match path.clone() {
                Some(path) => {
                    cx.background_executor()
                        .spawn(async move {
                            let api = connection.key_value_api().ok_or_else(|| {
                                DbError::NotSupported("Key-value API unavailable".to_string())
                            })?;
                            write_export(api, &keys, keyspace, &path)
                        })
                        .await
                }
                None => Ok(()),
            };

            if let Err(error) = &result {
                report_error_async(
                    UserFacingError::new(ErrorKind::Storage, error.to_string()),
                    cx,
                );
            }

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if let Some(state) = this.bulk_delete.as_mut() {
                        state.stage = BulkDeleteStage::Confirming;

                        if result.is_ok() {
                            state.exported_to = path.clone();
                        }
                    }

                    if let (Ok(()), Some(path)) = (&result, &path) {
                        Toast::success(dbflux_i18n::t!(
                            "document.key_value.bulk_delete.toast_exported",
                            path = path.display().to_string()
                        ))
                        .meta_right(now_hms())
                        .push(cx);
                    }

                    cx.notify();
                });
            });
        })
        .detach();
    }
}

#[allow(clippy::result_large_err)]
fn write_export(
    api: &dyn KeyValueApi,
    keys: &[String],
    keyspace: Option<u32>,
    path: &std::path::Path,
) -> Result<(), DbError> {
    let file = std::fs::File::create(path).map_err(DbError::IoError)?;
    let mut writer = std::io::BufWriter::new(file);

    for batch in keys.chunks(EXPORT_BATCH_SIZE) {
        let mut request = KeyBulkGetRequest::new(batch.to_vec());
        request.keyspace = keyspace;

        for value in api.bulk_get(&request)?.into_iter().flatten() {
            let line =
                export_line(&value).map_err(|error| DbError::query_failed(error.to_string()))?;
            writeln!(writer, "{line}").map_err(DbError::IoError)?;
        }
    }

    writer.flush().map_err(DbError::IoError)
}

struct BulkDeleteAudit<'a> {
    profile_id: Uuid,
    database: &'a str,
    driver_id: &'a str,
    pattern: &'a str,
    type_filter: Option<KeyType>,
    matched: u64,
    duration_ms: i64,
}

/// Records a bulk delete in the audit log. Only the pattern, the type filter
/// and the counts are stored, never the deleted keys.
fn record_bulk_delete_audit(
    audit_service: &dbflux_audit::AuditService,
    audit: BulkDeleteAudit<'_>,
    result: &Result<u64, (u64, DbError)>,
) {
    use dbflux_core::chrono::Utc;
    use dbflux_core::observability::{
        EventActorType, EventCategory, EventOutcome, EventRecord, EventSeverity, EventSink, actions,
    };

    let type_label = audit.type_filter.map(key_type_label);
    let removed = match result {
        Ok(removed) => *removed,
        Err((removed, _)) => *removed,
    };

    let (severity, outcome, action) = match result {
        Ok(_) => (
            EventSeverity::Warn,
            EventOutcome::Success,
            actions::KEY_BULK_DELETE,
        ),
        Err(_) => (
            EventSeverity::Error,
            EventOutcome::Failure,
            actions::KEY_BULK_DELETE_FAILED,
        ),
    };

    let summary = format!(
        "Deleted {removed} of {} keys matching {} in {}",
        audit.matched, audit.pattern, audit.database
    );

    let details = serde_json::json!({
        "pattern": audit.pattern,
        "type": type_label,
        "matched": audit.matched,
        "deleted": removed,
        "batch_size": BULK_DELETE_BATCH_SIZE,
    });

    let mut event = EventRecord::new(
        Utc::now().timestamp_millis(),
        severity,
        EventCategory::Query,
        outcome,
    )
    .with_typed_action(action)
    .with_summary(summary)
    .with_actor_id("ui:user")
    .with_connection_context(
        audit.profile_id.to_string(),
        audit.database.to_string(),
        audit.driver_id.to_string(),
    )
    .with_object_ref("key_pattern", audit.pattern.to_string())
    .with_details_json(details.to_string())
    .with_duration_ms(audit.duration_ms);

    event.actor_type = EventActorType::User;

    if let Err((_, error)) = result {
        event = event.with_error("bulk_delete_failed", error.to_string());
    }

    if let Err(error) = audit_service.record(event) {
        log::warn!("[key-value] failed to record bulk-delete audit event: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BULK_DELETE_BATCH_SIZE, BulkDeleteStage, BulkDeleteState, BulkScanState, display_pattern,
        export_line, unlink_in_batches,
    };
    use dbflux_core::{
        DbError, KeyBulkDeleteRequest, KeyDeleteRequest, KeyEntry, KeyExistsRequest, KeyGetRequest,
        KeyGetResult, KeyScanPage, KeyScanRequest, KeySetRequest, KeyValueApi,
    };
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    struct RecordingDelete {
        batches: Mutex<Vec<usize>>,
        fail_on_batch: Option<usize>,
    }

    impl KeyValueApi for RecordingDelete {
        fn delete_keys(&self, request: &KeyBulkDeleteRequest) -> Result<u64, DbError> {
            let mut batches = self
                .batches
                .lock()
                .map_err(|_| DbError::query_failed("lock"))?;

            if self.fail_on_batch == Some(batches.len()) {
                return Err(DbError::query_failed("connection reset"));
            }

            batches.push(request.keys.len());
            Ok(request.keys.len() as u64)
        }

        fn scan_keys(&self, _request: &KeyScanRequest) -> Result<KeyScanPage, DbError> {
            Err(DbError::NotSupported("scan".to_string()))
        }

        fn get_key(&self, _request: &KeyGetRequest) -> Result<KeyGetResult, DbError> {
            Err(DbError::NotSupported("get".to_string()))
        }

        fn set_key(&self, _request: &KeySetRequest) -> Result<(), DbError> {
            Err(DbError::NotSupported("set".to_string()))
        }

        fn delete_key(&self, _request: &KeyDeleteRequest) -> Result<bool, DbError> {
            Err(DbError::NotSupported("delete".to_string()))
        }

        fn exists_key(&self, _request: &KeyExistsRequest) -> Result<bool, DbError> {
            Err(DbError::NotSupported("exists".to_string()))
        }
    }

    fn keys(count: usize) -> Vec<String> {
        (0..count).map(|index| format!("session:{index}")).collect()
    }

    #[test]
    fn unlink_runs_in_fixed_size_batches() {
        let api = RecordingDelete {
            batches: Mutex::new(Vec::new()),
            fail_on_batch: None,
        };

        let removed = unlink_in_batches(&api, &keys(250), Some(0), BULK_DELETE_BATCH_SIZE);

        assert_eq!(removed.ok(), Some(250));
        assert_eq!(*api.batches.lock().expect("batches"), vec![100, 100, 50]);
    }

    #[test]
    fn a_failing_batch_reports_what_was_already_removed() {
        let api = RecordingDelete {
            batches: Mutex::new(Vec::new()),
            fail_on_batch: Some(2),
        };

        let result = unlink_in_batches(&api, &keys(250), None, BULK_DELETE_BATCH_SIZE);

        match result {
            Err((removed, _)) => assert_eq!(removed, 200),
            Ok(_) => panic!("the third batch should fail"),
        }
    }

    #[test]
    fn nothing_is_sent_for_an_empty_match_list() {
        let api = RecordingDelete {
            batches: Mutex::new(Vec::new()),
            fail_on_batch: None,
        };

        assert_eq!(unlink_in_batches(&api, &[], None, 100).ok(), Some(0));
        assert!(api.batches.lock().expect("batches").is_empty());
    }

    fn state(pattern: &str) -> BulkDeleteState {
        BulkDeleteState {
            pattern: pattern.to_string(),
            type_filter: None,
            matches: Vec::new(),
            preview_ttls: HashMap::new(),
            scan_state: BulkScanState::Scanning,
            stage: BulkDeleteStage::Confirming,
            exported_to: None,
            confirm_input: None,
            generation: 1,
        }
    }

    #[test]
    fn deleting_needs_a_finished_scan_matches_and_the_exact_pattern() {
        let mut state = state("session:*");
        let mut seen = HashSet::new();
        state.add_page(vec![KeyEntry::new("session:1")], &mut seen);

        assert!(!state.can_delete("session:*"), "scan still running");

        state.scan_state = BulkScanState::Done;
        assert!(!state.can_delete("session:"));
        assert!(!state.can_delete("SESSION:*"));
        assert!(state.can_delete("session:*"));

        state.stage = BulkDeleteStage::Deleting;
        assert!(!state.can_delete("session:*"), "already running");
    }

    #[test]
    fn an_empty_scan_can_never_be_deleted() {
        let mut state = state("gone:*");
        state.scan_state = BulkScanState::Done;

        assert!(!state.can_delete("gone:*"));
    }

    #[test]
    fn scan_pages_are_deduplicated() {
        let mut state = state("*");
        let mut seen = HashSet::new();

        state.add_page(vec![KeyEntry::new("a"), KeyEntry::new("b")], &mut seen);
        state.add_page(vec![KeyEntry::new("b"), KeyEntry::new("c")], &mut seen);

        let names: Vec<&str> = state
            .matches
            .iter()
            .map(|entry| entry.key.as_str())
            .collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn an_unfiltered_bulk_delete_asks_for_a_star() {
        assert_eq!(display_pattern(None), "*");
        assert_eq!(display_pattern(Some("user:*")), "user:*");
    }

    #[test]
    fn export_lines_keep_text_structured_and_binary_values_readable() {
        let text = KeyGetResult {
            entry: KeyEntry::new("greeting"),
            value: b"hello".to_vec(),
            repr: dbflux_core::ValueRepr::Text,
            load_state: dbflux_core::KeyLoadState::Loaded,
        };
        let binary = KeyGetResult {
            entry: KeyEntry::new("blob"),
            value: vec![0xde, 0xad],
            repr: dbflux_core::ValueRepr::Binary,
            load_state: dbflux_core::KeyLoadState::Loaded,
        };

        assert!(
            export_line(&text)
                .expect("text line")
                .contains(r#""value":"hello""#)
        );
        assert!(
            export_line(&binary)
                .expect("binary line")
                .contains(r#""hex":"dead""#)
        );
    }
}
