use super::parsing::parse_database_name;
use crate::ui::AsyncUpdateResultExt;
use dbflux_core::{DbError, KeyGetRequest, KeyScanRequest, TaskKind};
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
        self.selected_index = None;
        self.selected_value = None;
        self.last_error = None;
        self.string_edit_input = None;
        self.clear_ttl_state();
        self.rebuild_cached_members(cx);
        self.cancel_rename(cx);
        self.cancel_member_edit(cx);

        let Some(connection) = self.get_connection(cx) else {
            self.last_error = Some("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.last_error = Some("Too many background tasks running, please wait".to_string());
            cx.notify();
            return;
        }

        let filter = self.filter_input.read(cx).value().trim().to_string();
        let is_first_page = self.current_page == 1;
        let is_unfiltered = filter.is_empty();
        let database = self.database.clone();
        let entity = cx.entity().clone();

        let description = if filter.is_empty() {
            format!("SCAN {}", database)
        } else {
            format!("SCAN {} *{}*", database, filter)
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
            filter: if filter.is_empty() {
                None
            } else {
                Some(format!("*{}*", filter))
            },
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
                    api.scan_keys(&request)
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    match result {
                        Ok(page) => {
                            this.runner.complete_primary(task_id, cx);

                            this.keys = page.entries;
                            this.next_cursor = page.next_cursor;
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
            })
            .log_if_dropped();
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
            self.last_error = Some("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        let description = format!("GET {}", dbflux_core::truncate_string_safe(&key, 60));
        let (task_id, cancel_token) = self.runner.start_primary(TaskKind::KeyGet, description, cx);
        cx.notify();

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
                            this.rebuild_cached_members(cx);
                        }
                        Err(error) => {
                            this.runner.fail_primary(task_id, error.to_string(), cx);
                            this.clear_ttl_state();
                            this.selected_value = None;
                            this.last_error = Some(error.to_string());
                            this.rebuild_cached_members(cx);
                        }
                    }

                    cx.notify();
                });
            })
            .log_if_dropped();
        })
        .detach();
    }
}
