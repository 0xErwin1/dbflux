use super::parsing::parse_database_name;
use dbflux_core::{DbError, KeyGetRequest, KeyScanRequest, KeyValueApi, TaskKind};
use dbflux_ui_base::AsyncUpdateResultExt;
use gpui::*;

impl super::KeyValueDocument {
    pub(super) fn reload_keys(&mut self, cx: &mut Context<Self>) {
        self.current_page = 1;
        self.current_cursor = None;
        self.next_cursor = None;
        self.previous_cursors.clear();
        self.load_page(cx);
    }

    pub(super) fn go_next_page(&mut self, cx: &mut Context<Self>) {
        let Some(next) = self.next_cursor.clone() else {
            return;
        };
        self.previous_cursors.push(self.current_cursor.clone());
        self.current_cursor = Some(next);
        self.current_page += 1;
        self.load_page(cx);
    }

    pub(super) fn go_prev_page(&mut self, cx: &mut Context<Self>) {
        let Some(prev) = self.previous_cursors.pop() else {
            return;
        };
        self.current_cursor = prev;
        self.current_page = self.current_page.saturating_sub(1).max(1);
        self.load_page(cx);
    }

    pub(super) fn can_go_next(&self) -> bool {
        !self.runner.is_primary_active() && self.next_cursor.is_some()
    }

    pub(super) fn can_go_prev(&self) -> bool {
        !self.runner.is_primary_active() && !self.previous_cursors.is_empty()
    }

    pub(super) fn load_page(&mut self, cx: &mut Context<Self>) {
        self.keys.clear();
        self.key_total = None;
        self.selected_index = None;
        self.selected_value = None;
        self.last_error = None;
        self.string_edit_input = None;
        self.clear_ttl_state();
        self.rebuild_cached_members(cx);
        self.cancel_rename(cx);
        self.cancel_member_edit(cx);

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
        let is_first_page = self.current_page == 1;
        let is_unfiltered = filter.is_empty();
        let pattern = key_scan_pattern(&filter);
        let database = self.database.clone();
        let entity = cx.entity().clone();

        let description = match &pattern {
            Some(pattern) => format!("SCAN {} {}", database, pattern),
            None => format!("SCAN {}", database),
        };

        let (task_id, cancel_token) = self
            .runner
            .start_primary(TaskKind::KeyScan, description, cx);
        cx.notify();

        let scan_batch_size = self
            .app_state
            .read(cx)
            .effective_settings_for_connection(Some(self.profile_id))
            .driver_values
            .get("scan_batch_size")
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(100);

        let request = KeyScanRequest {
            cursor: self.current_cursor.clone(),
            filter: pattern,
            limit: scan_batch_size,
            keyspace: parse_database_name(&database),
        };

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;
                    let page = api.scan_keys(&request)?;
                    let key_total =
                        fetch_key_total(api, request.keyspace, request.filter.as_deref());

                    Ok::<_, DbError>((page, key_total))
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    match result {
                        Ok((page, key_total)) => {
                            this.runner.complete_primary(task_id, cx);

                            this.keys = page.entries;
                            this.next_cursor = page.next_cursor;
                            this.key_total = key_total;
                            this.last_error = None;

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

                            if !this.keys.is_empty() {
                                this.selected_index = Some(0);
                                this.reload_selected_value(cx);
                            }
                        }
                        Err(error) => {
                            this.runner.fail_primary(task_id, error.to_string(), cx);
                            this.last_error = Some(error.to_string());
                        }
                    }

                    cx.notify();
                });
            });
        })
        .detach();
    }

    pub(super) fn reload_selected_value(&mut self, cx: &mut Context<Self>) {
        let Some(key) = self.selected_key() else {
            self.selected_value = None;
            self.rebuild_cached_members(cx);
            cx.notify();
            return;
        };

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
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;
                    api.get_key(&KeyGetRequest {
                        key,
                        keyspace,
                        include_type: true,
                        include_ttl: true,
                        include_size: true,
                        max_value_bytes,
                    })
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    match result {
                        Ok(value) => {
                            this.runner.complete_primary(task_id, cx);

                            let key_type = value.entry.key_type;
                            let is_hash_or_stream = matches!(
                                key_type,
                                Some(dbflux_core::KeyType::Hash | dbflux_core::KeyType::Stream)
                            );
                            this.apply_ttl_from_entry(&value.entry, cx);
                            this.selected_value = Some(value);
                            this.last_error = None;
                            this.value_view_mode = if is_hash_or_stream {
                                super::KvValueViewMode::Document
                            } else {
                                super::KvValueViewMode::Table
                            };
                            this.reset_kv_decode_state_for_new_value(cx);
                            this.rebuild_cached_members(cx);
                        }
                        Err(error) => {
                            this.runner.fail_primary(task_id, error.to_string(), cx);
                            this.clear_ttl_state();
                            this.selected_value = None;
                            this.last_error = Some(error.to_string());
                            this.reset_kv_decode_state_for_new_value(cx);
                            this.rebuild_cached_members(cx);
                        }
                    }

                    cx.notify();
                });
            });
        })
        .detach();
    }
}

/// Builds the key scan pattern for the filter the user typed.
///
/// Plain text matches anywhere in the key, so it is wrapped as `*text*`. Input
/// that already contains a glob metacharacter (`*`, `?` or `[`) is passed
/// through unchanged, which lets the user search by prefix (`user:*`) or with
/// any other glob. An empty filter scans every key.
fn key_scan_pattern(filter: &str) -> Option<String> {
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

/// Counts every key in the keyspace, but only for an unfiltered scan: a total
/// next to a filtered page would read as the number of matches.
///
/// A driver that cannot count, or a count that fails, yields `None` so the
/// total is simply not shown; the page itself already loaded.
fn fetch_key_total(
    api: &dyn KeyValueApi,
    keyspace: Option<u32>,
    pattern: Option<&str>,
) -> Option<u64> {
    if pattern.is_some() {
        return None;
    }

    match api.key_count(keyspace) {
        Ok(count) => Some(count),
        Err(DbError::NotSupported(_)) => None,
        Err(error) => {
            log::debug!("Key total unavailable: {error}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{fetch_key_total, key_scan_pattern};
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
            unimplemented!("not used by fetch_key_total")
        }

        fn get_key(&self, _request: &KeyGetRequest) -> Result<KeyGetResult, DbError> {
            unimplemented!("not used by fetch_key_total")
        }

        fn set_key(&self, _request: &KeySetRequest) -> Result<(), DbError> {
            unimplemented!("not used by fetch_key_total")
        }

        fn delete_key(&self, _request: &KeyDeleteRequest) -> Result<bool, DbError> {
            unimplemented!("not used by fetch_key_total")
        }

        fn exists_key(&self, _request: &KeyExistsRequest) -> Result<bool, DbError> {
            unimplemented!("not used by fetch_key_total")
        }
    }

    #[test]
    fn key_total_is_fetched_for_an_unfiltered_scan() {
        let api = CountingKeyValue::new(CountOutcome::Count(42));

        assert_eq!(fetch_key_total(&api, Some(0), None), Some(42));
        assert_eq!(api.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn key_total_is_hidden_and_not_fetched_while_a_filter_is_set() {
        let api = CountingKeyValue::new(CountOutcome::Count(42));

        assert_eq!(fetch_key_total(&api, Some(0), Some("*user*")), None);
        assert_eq!(api.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn key_total_is_hidden_when_the_driver_cannot_count() {
        let unsupported = CountingKeyValue::new(CountOutcome::Unsupported);
        let failing = CountingKeyValue::new(CountOutcome::Fails);

        assert_eq!(fetch_key_total(&unsupported, None, None), None);
        assert_eq!(fetch_key_total(&failing, None, None), None);
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
