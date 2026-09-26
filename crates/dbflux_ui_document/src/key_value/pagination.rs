//! Key scanning and value loading.
//!
//! The key list accumulates pages: "Load more" continues the scan from its
//! cursor instead of replacing the page. A filtered scan keeps reading on
//! its own until it has a page worth of matches or the keyspace ends,
//! because `SCAN` may return empty batches long before it finishes.

use super::metadata::CachedKeyMetadata;
use super::parsing::parse_database_name;
use dbflux_core::{
    DbError, KeyGetRequest, KeyMetadataRequest, KeyScanRequest, KeyType, KeyValueApi,
    KeyValueFeatures, TaskKind,
};
use gpui::*;
use std::time::Instant;

/// Page size when the connection sets no `scan_batch_size`.
const DEFAULT_SCAN_BATCH_SIZE: u32 = 100;

/// How far the scan goes on its own after the current page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScanMode {
    /// Stop after this page; the user asks for more.
    Page,
    /// A filtered scan: keep reading until `target` keys are loaded.
    Searching { target: usize },
    /// Keep reading until the whole keyspace has been scanned.
    WholeKeyspace,
}

/// Whether the scan continues by itself after a page arrived.
pub(super) fn should_continue_scan(mode: ScanMode, loaded: usize, cursor_pending: bool) -> bool {
    if !cursor_pending {
        return false;
    }

    match mode {
        ScanMode::Page => false,
        ScanMode::Searching { target } => loaded < target,
        ScanMode::WholeKeyspace => true,
    }
}

/// Builds the key scan pattern for the filter the user typed.
///
/// Plain text matches anywhere in the key, so it is wrapped as `*text*`. Input
/// that already contains a glob metacharacter (`*`, `?` or `[`) is passed
/// through unchanged, which lets the user search by prefix (`user:*`) or with
/// any other glob. An empty filter scans every key.
pub(super) fn key_scan_pattern(filter: &str) -> Option<String> {
    if filter.is_empty() {
        return None;
    }

    let has_glob = filter.contains(['*', '?', '[']);

    if has_glob {
        Some(filter.to_string())
    } else {
        Some(format!("*{}*", filter))
    }
}

/// Counts every key in the keyspace. A driver that cannot count, or a count
/// that fails, yields `None` so the total is simply not shown.
fn fetch_key_total(api: &dyn KeyValueApi, keyspace: Option<u32>) -> Option<u64> {
    match api.key_count(keyspace) {
        Ok(count) => Some(count),
        Err(DbError::NotSupported(_)) => None,
        Err(error) => {
            log::debug!("Key total unavailable: {error}");
            None
        }
    }
}

impl super::KeyValueDocument {
    fn scan_batch_size(&self, cx: &App) -> u32 {
        self.app_state
            .read(cx)
            .effective_settings_for_connection(Some(self.profile_id))
            .driver_values
            .get("scan_batch_size")
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SCAN_BATCH_SIZE)
    }

    /// Whether the scan is narrowed by a pattern or a type.
    pub(super) fn is_filtered_scan(&self, cx: &App) -> bool {
        !self.filter_input.read(cx).value().trim().is_empty() || self.type_filter.is_some()
    }

    /// Whether the whole keyspace has been read.
    pub(super) fn scan_complete(&self) -> bool {
        self.scan_started && self.scan_cursor.is_none()
    }

    pub(super) fn is_scanning(&self) -> bool {
        self.scan_mode != ScanMode::Page && self.runner.is_primary_active()
    }

    /// Starts the scan over: clears the loaded keys and reads the first page.
    pub(super) fn reload_keys(&mut self, cx: &mut Context<Self>) {
        self.scan_generation = self.scan_generation.wrapping_add(1);
        self.keys.clear();
        self.loaded_key_names.clear();
        self.key_rows.clear();
        self.list_cursor = None;
        self.key_metadata.clear();
        self.metadata_in_flight.clear();
        self.scan_cursor = None;
        self.scan_started = false;
        self.scanned_keys = 0;
        self.key_total = None;
        self.selected_index = None;
        self.selected_value = None;
        self.value_metadata = None;
        self.zset_pane = None;
        self.stream_pane = None;
        self.expiry_editor = None;
        self.last_error = None;
        self.string_edit_input = None;
        self.clear_ttl_state();
        self.rebuild_cached_members(cx);
        self.cancel_rename(cx);
        self.cancel_member_edit(cx);

        let batch = self.scan_batch_size(cx) as usize;
        self.scan_mode = if self.is_filtered_scan(cx) {
            ScanMode::Searching { target: batch }
        } else {
            ScanMode::Page
        };

        self.scan_next_page(cx);
    }

    /// Continues the scan by one page (`Ctrl+J`).
    pub(super) fn load_more_keys(&mut self, cx: &mut Context<Self>) {
        if !self.can_load_more_keys() {
            return;
        }

        let batch = self.scan_batch_size(cx) as usize;
        self.scan_mode = if self.is_filtered_scan(cx) {
            ScanMode::Searching {
                target: self.keys.len() + batch,
            }
        } else {
            ScanMode::Page
        };

        self.scan_next_page(cx);
    }

    /// Keeps scanning until the whole keyspace has been read.
    pub(super) fn search_whole_keyspace(&mut self, cx: &mut Context<Self>) {
        if self.scan_complete() {
            return;
        }

        self.scan_mode = ScanMode::WholeKeyspace;

        if !self.runner.is_primary_active() {
            self.scan_next_page(cx);
        }

        cx.notify();
    }

    /// Stops a scan that is reading on its own.
    pub(super) fn stop_scan(&mut self, cx: &mut Context<Self>) {
        self.scan_mode = ScanMode::Page;
        self.runner.cancel_primary(cx);
        cx.notify();
    }

    pub(super) fn can_load_more_keys(&self) -> bool {
        !self.runner.is_primary_active() && self.scan_started && self.scan_cursor.is_some()
    }

    fn scan_next_page(&mut self, cx: &mut Context<Self>) {
        let Some(connection) = self.get_connection(cx) else {
            self.last_error = Some(dbflux_i18n::t!(
                "document.key_value.mutation.error.connection_inactive"
            ));
            cx.notify();
            return;
        };

        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.last_error = Some(dbflux_i18n::t!(
                "document.key_value.pagination.error.background_task_limit"
            ));
            cx.notify();
            return;
        }

        let filter = self.filter_input.read(cx).value().trim().to_string();
        let pattern = key_scan_pattern(&filter);
        let type_filter = self.type_filter.filter(|_| {
            self.key_features
                .contains(KeyValueFeatures::SCAN_TYPE_FILTER)
        });
        let is_first_page = !self.scan_started;
        let is_unfiltered = pattern.is_none() && type_filter.is_none();
        let database = self.database.clone();
        let generation = self.scan_generation;
        let entity = cx.entity().clone();

        let description = match &pattern {
            Some(pattern) => format!("SCAN {} MATCH {}", database, pattern),
            None => format!("SCAN {}", database),
        };

        let (task_id, cancel_token) = self
            .runner
            .start_primary(TaskKind::KeyScan, description, cx);
        cx.notify();

        let request = KeyScanRequest {
            cursor: self.scan_cursor.clone(),
            filter: pattern,
            limit: self.scan_batch_size(cx),
            keyspace: parse_database_name(&database),
            type_filter,
        };

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;
                    let page = api.scan_keys(&request)?;
                    let key_total = if is_first_page {
                        fetch_key_total(api, request.keyspace)
                    } else {
                        None
                    };

                    Ok::<_, DbError>((page, key_total))
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if cancel_token.is_cancelled() || this.scan_generation != generation {
                        return;
                    }

                    match result {
                        Ok((page, key_total)) => {
                            this.runner.complete_primary(task_id, cx);
                            this.apply_scan_page(page, key_total, is_first_page, cx);

                            if is_first_page && is_unfiltered {
                                let key_names: Vec<String> =
                                    this.keys.iter().map(|e| e.key.clone()).collect();

                                this.app_state.update(cx, |state, _cx| {
                                    state.set_redis_cached_keys(
                                        this.profile_id,
                                        this.database.clone(),
                                        key_names,
                                    );
                                });
                            }

                            if should_continue_scan(
                                this.scan_mode,
                                this.keys.len(),
                                this.scan_cursor.is_some(),
                            ) {
                                this.scan_next_page(cx);
                            } else {
                                this.scan_mode = ScanMode::Page;
                            }
                        }
                        Err(error) => {
                            this.runner.fail_primary(task_id, error.to_string(), cx);
                            this.scan_mode = ScanMode::Page;
                            this.last_error = Some(error.to_string());
                        }
                    }

                    cx.notify();
                });
            });
        })
        .detach();
    }

    fn apply_scan_page(
        &mut self,
        page: dbflux_core::KeyScanPage,
        key_total: Option<u64>,
        is_first_page: bool,
        cx: &mut Context<Self>,
    ) {
        for entry in page.entries {
            if self.loaded_key_names.insert(entry.key.clone()) {
                self.keys.push(entry);
            }
        }

        self.scan_started = true;
        self.scan_cursor = page.next_cursor;
        self.scanned_keys = self
            .scanned_keys
            .saturating_add(page.scanned_keys.unwrap_or(0));

        if key_total.is_some() {
            self.key_total = key_total;
        }

        self.last_error = None;
        self.rebuild_key_rows();

        if is_first_page && self.selected_index.is_none() {
            let first_key = self.key_rows.iter().find_map(|row| row.key_index());

            if let Some(key_index) = first_key {
                self.select_index(key_index, cx);
            }
        }
    }

    /// Loads the selected key: through its ranged pane for sorted sets and
    /// streams when the driver supports it, otherwise with one `get_key`.
    pub(super) fn reload_selected_value(&mut self, cx: &mut Context<Self>) {
        let Some(key) = self.selected_key() else {
            self.selected_value = None;
            self.rebuild_cached_members(cx);
            cx.notify();
            return;
        };

        let listed_type = self
            .selected_index
            .and_then(|index| self.keys.get(index))
            .and_then(|entry| entry.key_type);

        if let Some(key_type) =
            listed_type.filter(|key_type| self.uses_ranged_pane(Some(*key_type)))
        {
            self.load_ranged_key(key, key_type, None, cx);
            return;
        }

        self.zset_pane = None;
        self.stream_pane = None;

        let Some(connection) = self.get_connection(cx) else {
            self.last_error = Some(dbflux_i18n::t!(
                "document.key_value.mutation.error.connection_inactive"
            ));
            cx.notify();
            return;
        };

        let description = format!("GET {}", dbflux_core::truncate_string_safe(&key, 60));
        let (task_id, cancel_token) = self.runner.start_primary(TaskKind::KeyGet, description, cx);
        cx.notify();

        // One-shot: consumed here so a later plain refresh of this same key
        // goes back to the configured limit.
        let load_anyway = std::mem::take(&mut self.kv_load_anyway);
        let max_value_bytes = if load_anyway {
            None
        } else {
            Some(self.kv_size_limit_bytes(cx))
        };

        let keyspace = self.keyspace_index();
        let include_metadata = self.key_features.contains(KeyValueFeatures::KEY_METADATA);
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn({
                    let key = key.clone();
                    async move {
                        let api = connection.key_value_api().ok_or_else(|| {
                            DbError::NotSupported("Key-value API unavailable".to_string())
                        })?;

                        let value = api.get_key(&KeyGetRequest {
                            key: key.clone(),
                            keyspace,
                            include_type: true,
                            include_ttl: true,
                            include_size: true,
                            max_value_bytes,
                        })?;

                        let metadata = if include_metadata {
                            api.key_metadata(&KeyMetadataRequest {
                                keys: vec![key],
                                keyspace,
                                include_encoding: true,
                            })
                            .ok()
                            .and_then(|mut items| items.pop())
                        } else {
                            None
                        };

                        Ok::<_, DbError>((value, metadata))
                    }
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if cancel_token.is_cancelled() || this.selected_key().as_deref() != Some(&key) {
                        return;
                    }

                    match result {
                        Ok((value, metadata)) => {
                            this.runner.complete_primary(task_id, cx);

                            if let Some(metadata) = &metadata {
                                this.key_metadata.insert(
                                    metadata.key.clone(),
                                    CachedKeyMetadata::from_metadata(metadata, Instant::now()),
                                );
                            }

                            this.apply_ttl_from_entry(&value.entry, cx);
                            this.selected_value = Some(value);
                            this.value_metadata = metadata;
                            this.last_error = None;
                            this.value_view_mode = super::KvValueViewMode::Table;
                            this.reset_value_view_for_new_value(cx);
                            this.rebuild_cached_members(cx);
                        }
                        Err(error) => {
                            this.runner.fail_primary(task_id, error.to_string(), cx);
                            this.clear_ttl_state();
                            this.selected_value = None;
                            this.value_metadata = None;
                            this.last_error = Some(error.to_string());
                            this.reset_value_view_for_new_value(cx);
                            this.rebuild_cached_members(cx);
                        }
                    }

                    cx.notify();
                });
            });
        })
        .detach();
    }

    /// Reads the first bytes of a string too large to load
    /// ("Preview first 64 KB").
    pub(super) fn preview_value_prefix(&mut self, cx: &mut Context<Self>) {
        let Some(key) = self.selected_key() else {
            return;
        };
        let Some(connection) = self.get_connection(cx) else {
            self.report_connection_inactive(cx);
            return;
        };

        let description = format!(
            "GETRANGE {} 0 {}",
            dbflux_core::truncate_string_safe(&key, 60),
            super::decode::VALUE_PREVIEW_BYTES - 1
        );
        let (task_id, cancel_token) = self.runner.start_primary(TaskKind::KeyGet, description, cx);
        let keyspace = self.keyspace_index();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn({
                    let key = key.clone();
                    async move {
                        let api = connection.key_value_api().ok_or_else(|| {
                            DbError::NotSupported("Key-value API unavailable".to_string())
                        })?;
                        api.get_value_prefix(&dbflux_core::KeyValuePrefixRequest {
                            key,
                            keyspace,
                            max_bytes: super::decode::VALUE_PREVIEW_BYTES,
                        })
                    }
                })
                .await;

            if let Err(error) = &result {
                dbflux_ui_base::user_error::report_error_async(
                    dbflux_ui_base::user_error::UserFacingError::new(
                        dbflux_ui_base::user_error::ErrorKind::Driver,
                        error.to_string(),
                    ),
                    cx,
                );
            }

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if cancel_token.is_cancelled() || this.selected_key().as_deref() != Some(&key) {
                        return;
                    }

                    match result {
                        Ok(mut prefix) => {
                            this.runner.complete_primary(task_id, cx);

                            if let Some(current) = &this.selected_value {
                                prefix.entry.ttl_seconds = current.entry.ttl_seconds;
                            }

                            this.selected_value = Some(prefix);
                            this.reset_value_view_for_new_value(cx);
                        }
                        Err(error) => this.runner.fail_primary(task_id, error.to_string(), cx),
                    }

                    cx.notify();
                });
            });
        })
        .detach();
    }

    pub(super) fn set_type_filter(&mut self, type_filter: Option<KeyType>, cx: &mut Context<Self>) {
        if self.type_filter == type_filter {
            return;
        }

        self.type_filter = type_filter;
        self.reload_keys(cx);
    }
}

#[cfg(test)]
mod tests {
    use super::{ScanMode, fetch_key_total, key_scan_pattern, should_continue_scan};
    use dbflux_core::{
        DbError, KeyDeleteRequest, KeyExistsRequest, KeyGetRequest, KeyGetResult, KeyScanPage,
        KeyScanRequest, KeySetRequest, KeyValueApi,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    enum CountOutcome {
        Count(u64),
        Unsupported,
        Fails,
    }

    struct CountingKeyValue {
        outcome: CountOutcome,
        calls: AtomicUsize,
    }

    impl CountingKeyValue {
        fn new(outcome: CountOutcome) -> Self {
            Self {
                outcome,
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl KeyValueApi for CountingKeyValue {
        fn key_count(&self, _keyspace: Option<u32>) -> Result<u64, DbError> {
            self.calls.fetch_add(1, Ordering::SeqCst);

            match self.outcome {
                CountOutcome::Count(count) => Ok(count),
                CountOutcome::Unsupported => Err(DbError::NotSupported("no count".to_string())),
                CountOutcome::Fails => Err(DbError::query_failed("connection reset")),
            }
        }

        fn scan_keys(&self, _request: &KeyScanRequest) -> Result<KeyScanPage, DbError> {
            Err(DbError::NotSupported(
                "not used by fetch_key_total".to_string(),
            ))
        }

        fn get_key(&self, _request: &KeyGetRequest) -> Result<KeyGetResult, DbError> {
            Err(DbError::NotSupported(
                "not used by fetch_key_total".to_string(),
            ))
        }

        fn set_key(&self, _request: &KeySetRequest) -> Result<(), DbError> {
            Err(DbError::NotSupported(
                "not used by fetch_key_total".to_string(),
            ))
        }

        fn delete_key(&self, _request: &KeyDeleteRequest) -> Result<bool, DbError> {
            Err(DbError::NotSupported(
                "not used by fetch_key_total".to_string(),
            ))
        }

        fn exists_key(&self, _request: &KeyExistsRequest) -> Result<bool, DbError> {
            Err(DbError::NotSupported(
                "not used by fetch_key_total".to_string(),
            ))
        }
    }

    #[test]
    fn key_total_is_fetched_from_the_driver() {
        let api = CountingKeyValue::new(CountOutcome::Count(42));

        assert_eq!(fetch_key_total(&api, Some(0)), Some(42));
        assert_eq!(api.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn key_total_is_hidden_when_the_driver_cannot_count() {
        let unsupported = CountingKeyValue::new(CountOutcome::Unsupported);
        let failing = CountingKeyValue::new(CountOutcome::Fails);

        assert_eq!(fetch_key_total(&unsupported, None), None);
        assert_eq!(fetch_key_total(&failing, None), None);
    }

    #[test]
    fn a_plain_page_never_continues_by_itself() {
        assert!(!should_continue_scan(ScanMode::Page, 0, true));
    }

    #[test]
    fn a_search_keeps_reading_until_its_target_or_the_end() {
        let searching = ScanMode::Searching { target: 100 };

        assert!(should_continue_scan(searching, 1, true));
        assert!(!should_continue_scan(searching, 100, true));
        assert!(!should_continue_scan(searching, 1, false));
    }

    #[test]
    fn a_whole_keyspace_search_reads_until_the_cursor_ends() {
        assert!(should_continue_scan(ScanMode::WholeKeyspace, 10_000, true));
        assert!(!should_continue_scan(ScanMode::WholeKeyspace, 1, false));
    }

    #[test]
    fn key_scan_pattern_scans_everything_for_an_empty_filter() {
        assert_eq!(key_scan_pattern(""), None);
    }

    #[test]
    fn key_scan_pattern_wraps_plain_text_as_a_substring_match() {
        assert_eq!(
            key_scan_pattern("leaderboard"),
            Some("*leaderboard*".to_string())
        );
    }

    #[test]
    fn key_scan_pattern_passes_globs_through_unchanged() {
        assert_eq!(
            key_scan_pattern("leaderboard*"),
            Some("leaderboard*".to_string())
        );
        assert_eq!(key_scan_pattern("user:?"), Some("user:?".to_string()));
        assert_eq!(
            key_scan_pattern("session:[ab]*"),
            Some("session:[ab]*".to_string())
        );
    }

    #[test]
    fn key_value_pagination_keys_resolve_in_both_locales() {
        let keys = ["document.key_value.pagination.error.background_task_limit"];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }
}
