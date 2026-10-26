//! Store calls of `SecretStoreDocument`. Each one runs on the background
//! executor, because the seam is synchronous, and lands on the foreground
//! through a generation check, so a result that a newer call replaced is
//! dropped. The first catch site of a user-triggered failure reports it.

use super::listing::PathMode;
use super::value::{RetrievalTarget, RetrievedValue, ValueFailure, value_retrieval_event};
use super::{CallTiming, DetailsState, SecretRowKey, SecretStoreDocument, StoreCall, ValueState};
use crate::object_text::db_error_to_user_facing;
use dbflux_core::observability::EventSink as _;
use dbflux_core::{
    Connection, DbError, SecretDetails, SecretListPage, SecretListRequest, SecretValue,
};
use dbflux_ui_base::user_error::{report_error, report_error_async};
use gpui::Context;
use std::sync::Arc;
use std::time::Instant;

impl SecretStoreDocument {
    fn connection(&self, cx: &Context<Self>) -> Option<Arc<dyn Connection>> {
        self.app_state
            .read(cx)
            .connections()
            .get(&self.profile_id)
            .map(|connected| connected.connection.clone())
    }

    /// The request for the current level: its path and mode on a
    /// hierarchical store, and the name filter.
    pub(crate) fn list_request(&self, page_token: Option<String>) -> SecretListRequest {
        let hierarchical = self.hierarchical();
        let name_filter = Some(self.name_filter.clone()).filter(|filter| !filter.is_empty());

        SecretListRequest {
            path: if hierarchical {
                self.path.clone()
            } else {
                None
            },
            recursive: hierarchical && self.path_mode == PathMode::Flat,
            name_filter,
            page_token,
            page_size: None,
        }
    }

    /// Lists the current level from its first page. The selection, its
    /// details and any retrieved value are dropped with the old listing.
    pub fn load_first_page(&mut self, cx: &mut Context<Self>) {
        self.selected = None;
        self.details_generation = self.details_generation.wrapping_add(1);
        self.details = DetailsState::Idle;
        self.clear_value();

        let generation = self.listing.begin(true);
        self.spawn_list(generation, None, cx);
    }

    /// Loads the next page and appends it. A no-op on the last page or while
    /// a page is loading.
    pub fn load_more(&mut self, cx: &mut Context<Self>) {
        if self.listing.loading {
            return;
        }

        let Some(token) = self.listing.next_page_token.clone() else {
            return;
        };

        let generation = self.listing.begin(false);
        self.spawn_list(generation, Some(token), cx);
    }

    /// Refresh: lists the level again and drops the retrieved value.
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        self.load_first_page(cx);
    }

    fn spawn_list(&mut self, generation: u64, page_token: Option<String>, cx: &mut Context<Self>) {
        cx.notify();

        let Some(connection) = self.connection(cx) else {
            self.listing.apply_error(
                generation,
                dbflux_i18n::t!("document.secret_store.error.connection_unavailable"),
            );
            cx.notify();
            return;
        };

        let request = self.list_request(page_token);
        let store_id = self.store.id.clone();
        let entity = cx.entity().clone();

        let task = cx.background_executor().spawn(async move {
            let started = Instant::now();
            let result = match connection.secret_store(&store_id) {
                Some(store) => store.list(&request),
                None => Err(store_unavailable(&store_id)),
            };

            (result, started.elapsed().as_millis())
        });

        cx.spawn(async move |_this, cx| {
            let (result, millis) = task.await;

            if let Err(error) = &result {
                report_error_async(db_error_to_user_facing(error), cx);
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| doc.apply_list(generation, result, millis, cx));
            });
        })
        .detach();
    }

    pub(crate) fn apply_list(
        &mut self,
        generation: u64,
        result: Result<SecretListPage, DbError>,
        millis: u128,
        cx: &mut Context<Self>,
    ) {
        let applied = match result {
            Ok(page) => {
                let count = page.entries.len();
                let applied = self.listing.apply_page(generation, page);

                if applied {
                    self.last_call = Some(CallTiming {
                        call: StoreCall::List { count },
                        millis,
                    });
                }

                applied
            }
            Err(error) => self.listing.apply_error(generation, error.to_string()),
        };

        if applied {
            cx.notify();
        }
    }

    /// Describes `name` for the details pane. Never fetches its value.
    pub(crate) fn describe(&mut self, name: String, cx: &mut Context<Self>) {
        self.details_generation = self.details_generation.wrapping_add(1);
        let generation = self.details_generation;
        self.details = DetailsState::Loading;

        let Some(connection) = self.connection(cx) else {
            self.details = DetailsState::Failed(dbflux_i18n::t!(
                "document.secret_store.error.connection_unavailable"
            ));
            return;
        };

        let store_id = self.store.id.clone();
        let entity = cx.entity().clone();

        let task = cx.background_executor().spawn({
            let name = name.clone();
            async move {
                let started = Instant::now();
                let result = match connection.secret_store(&store_id) {
                    Some(store) => store.describe(&name),
                    None => Err(store_unavailable(&store_id)),
                };

                (result, started.elapsed().as_millis())
            }
        });

        cx.spawn(async move |_this, cx| {
            let (result, millis) = task.await;

            if let Err(error) = &result {
                report_error_async(db_error_to_user_facing(error), cx);
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_details(generation, &name, result, millis, cx)
                });
            });
        })
        .detach();
    }

    pub(crate) fn apply_details(
        &mut self,
        generation: u64,
        name: &str,
        result: Result<SecretDetails, DbError>,
        millis: u128,
        cx: &mut Context<Self>,
    ) {
        let is_current = generation == self.details_generation
            && self.selected == Some(SecretRowKey::Secret(name.to_string()));

        if !is_current {
            return;
        }

        self.details = match result {
            Ok(details) => DetailsState::Loaded(Box::new(details)),
            Err(error) => DetailsState::Failed(error.to_string()),
        };
        self.last_call = Some(CallTiming {
            call: StoreCall::Describe,
            millis,
        });
        cx.notify();
    }

    /// Retrieve value: fetches the selected secret's current value. Every
    /// retrieval, successful or not, records one audit event without the
    /// value. Refused for a secret whose deletion is pending.
    pub fn retrieve_selected_value(&mut self, cx: &mut Context<Self>) {
        if !self.can_retrieve_value() {
            return;
        }

        let Some(name) = self.selected_summary().map(|summary| summary.name.clone()) else {
            return;
        };

        self.clear_value();
        let generation = self.value_generation;
        self.value = ValueState::Loading;
        cx.notify();

        let Some(connection) = self.connection(cx) else {
            self.apply_value(
                generation,
                &name,
                Err(DbError::connection_failed(dbflux_i18n::t!(
                    "document.secret_store.error.connection_unavailable"
                ))),
                0,
                cx,
            );
            return;
        };

        let store_id = self.store.id.clone();
        let entity = cx.entity().clone();

        let task = cx.background_executor().spawn({
            let name = name.clone();
            async move {
                let started = Instant::now();
                let result = match connection.secret_store(&store_id) {
                    Some(store) => store.get_value(&name, None),
                    None => Err(store_unavailable(&store_id)),
                };

                (result, started.elapsed().as_millis())
            }
        });

        cx.spawn(async move |_this, cx| {
            let (result, millis) = task.await;

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_value(generation, &name, result, millis, cx)
                });
            });
        })
        .detach();
    }

    /// Lands a retrieval. The audit event is recorded even when the user
    /// moved on, because the store was read; the value itself is then
    /// dropped instead of shown.
    pub(crate) fn apply_value(
        &mut self,
        generation: u64,
        name: &str,
        result: Result<SecretValue, DbError>,
        millis: u128,
        cx: &mut Context<Self>,
    ) {
        let is_current = generation == self.value_generation
            && self.selected == Some(SecretRowKey::Secret(name.to_string()));

        let driver_id = self
            .connection(cx)
            .map(|connection| connection.metadata().id.clone())
            .unwrap_or_default();

        let target = RetrievalTarget {
            profile_id: self.profile_id,
            driver_id: &driver_id,
            store_id: &self.store.id,
            store_label: &self.store.label,
            name,
        };

        match result {
            Ok(value) => {
                self.record_audit(
                    value_retrieval_event(&target, value.version.as_deref(), None),
                    cx,
                );

                if is_current {
                    self.value = ValueState::Loaded(Box::new(RetrievedValue::new(&value)));
                }
            }
            Err(error) => {
                let user_error = db_error_to_user_facing(&error);
                let failure = ValueFailure {
                    message: error.to_string(),
                    correlation_id: user_error.correlation_id,
                    decrypt_denied: matches!(error, DbError::PermissionDenied(_)),
                };

                self.record_audit(value_retrieval_event(&target, None, Some(&failure)), cx);
                report_error(user_error, cx);

                if is_current {
                    self.value = ValueState::Failed(failure);
                }
            }
        }

        if is_current {
            self.last_call = Some(CallTiming {
                call: StoreCall::Retrieve,
                millis,
            });
            cx.notify();
        }
    }

    fn record_audit(&self, event: dbflux_core::observability::EventRecord, cx: &gpui::App) {
        if let Err(error) = self.app_state.read(cx).audit_service().record(event) {
            log::warn!("[secret store] failed to record value retrieval audit event: {error}");
        }
    }
}

fn store_unavailable(store_id: &str) -> DbError {
    DbError::NotSupported(dbflux_i18n::t!(
        "document.secret_store.error.store_unavailable",
        store = store_id
    ))
}
