//! Store calls of `SecretStoreDocument`. Each one runs on the background
//! executor, because the seam is synchronous, and lands on the foreground
//! through a generation check, so a result that a newer call replaced is
//! dropped. The first catch site of a user-triggered failure reports it.

use super::listing::{LARGEST_PAGE_ENTRIES, PathMode, keeps_loading};
use super::value::{RetrievalTarget, RetrievedValue, ValueFailure, value_retrieval_event};
use super::{
    CallTiming, DetailsState, SecretRowKey, SecretStoreDocument, StoreCall, ValueState,
    VersionsState,
};
use crate::object_text::db_error_to_user_facing;
use dbflux_core::observability::EventSink as _;
use dbflux_core::{
    Connection, DbError, SecretDetails, SecretListPage, SecretListRequest, SecretStoreCapabilities,
    SecretValue, SecretVersionInfo,
};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::Context;
use std::sync::Arc;
use std::time::Instant;

impl SecretStoreDocument {
    pub(crate) fn connection(&self, cx: &Context<Self>) -> Option<Arc<dyn Connection>> {
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
            page_size: Some(LARGEST_PAGE_ENTRIES),
        }
    }

    /// Lists the current level from its first page. The selection, its
    /// details and any retrieved value are dropped with the old listing, and
    /// a menu open over it would act on rows that no longer exist — none of
    /// which may happen under an editor holding unsaved text.
    pub fn load_first_page(&mut self, cx: &mut Context<Self>) {
        if self.block_unsaved_value_edits(cx) {
            return;
        }

        self.selected = None;
        self.details_generation = self.details_generation.wrapping_add(1);
        self.details = DetailsState::Idle;
        self.reset_versions();
        self.clear_value();
        self.drop_context_menu();

        let generation = self.listing.begin(true);
        self.spawn_list(generation, None, cx);
    }

    /// Lists the current level again after a write, leaving the selection,
    /// its details and its value where they are: the rows are replaced when
    /// the first page lands, so a changed rotation, description or deletion
    /// date reads from the store instead of from the listing the write
    /// outdated.
    pub(crate) fn reload_listing_in_place(&mut self, cx: &mut Context<Self>) {
        let generation = self.listing.begin_reload();
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
    ///
    /// A region discovery that failed is asked again, so granting
    /// `ec2:DescribeRegions` brings the picker back without reopening the
    /// document.
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        if self.block_unsaved_value_edits(cx) {
            return;
        }

        if matches!(self.regions, super::RegionListState::Failed(_)) {
            self.load_regions(cx);
        }

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
        // A reload that replaces the rows starts from nothing, so a page that
        // is shorter than the listing it replaces still made progress.
        let replacing = self.listing.replaces_on_next_page();
        let loaded_before = if replacing {
            0
        } else {
            self.listing.entries.len()
        };
        let paths_before = if replacing {
            0
        } else {
            self.listing.child_paths.len()
        };

        let applied = match result {
            Ok(page) => {
                let count = page.entries.len();
                let applied = self.listing.apply_page(generation, page);

                if applied {
                    self.last_call = Some(CallTiming {
                        call: StoreCall::List {
                            count,
                            hierarchical: self.hierarchical(),
                        },
                        millis,
                    });
                }

                applied
            }
            Err(error) => self.listing.apply_error(generation, error.to_string()),
        };

        if !applied {
            return;
        }

        cx.notify();

        // A page that landed under the limit asks for the next one instead of
        // making the user click for it; the manual row remains for the rest.
        //
        // A page that added nothing stops the chain regardless: a store that
        // keeps handing back a token while returning no entries would
        // otherwise be asked forever.
        let made_progress = self.listing.entries.len() > loaded_before
            || self.listing.child_paths.len() > paths_before;

        if made_progress && keeps_loading(self.listing.entries.len(), self.listing.has_more()) {
            self.load_more(cx);
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

    /// Drops the history of the previous selection and invalidates any
    /// listing still in flight for it.
    pub(crate) fn reset_versions(&mut self) {
        self.versions_generation = self.versions_generation.wrapping_add(1);
        self.versions = VersionsState::Idle;
        self.versions_session = None;
        self.missing_connection_reported = false;
        self.rotation_notice = None;
    }

    /// Lists the versions of `name` for the details pane, on the background
    /// executor like [`Self::describe`]. A store without
    /// `SecretStoreCapabilities::VERSIONS` is never asked. Never fetches a
    /// value.
    pub(crate) fn load_versions(&mut self, name: String, cx: &mut Context<Self>) {
        self.request_versions(name, HistoryLoad::Requested, cx);
    }

    pub(in crate::secret_store) fn request_versions(
        &mut self,
        name: String,
        trigger: HistoryLoad,
        cx: &mut Context<Self>,
    ) {
        self.versions_generation = self.versions_generation.wrapping_add(1);
        let generation = self.versions_generation;

        if !self.store.supports(SecretStoreCapabilities::VERSIONS) {
            self.versions = VersionsState::NotSupported;
            return;
        }

        self.versions = VersionsState::Loading;
        self.versions_session = None;

        let Some(connection) = self.connection(cx) else {
            let message = dbflux_i18n::t!("document.secret_store.error.connection_unavailable");

            // Only the first catch site reports: an editor path that found
            // the same loss already did. A reload nobody asked for stays
            // quiet about it; a listing the user asked for does not.
            if trigger == HistoryLoad::Requested || !self.missing_connection_reported {
                self.missing_connection_reported = true;
                report_error(UserFacingError::new(ErrorKind::User, message.clone()), cx);
            }
            self.versions = VersionsState::Failed(message);
            return;
        };

        self.missing_connection_reported = false;
        self.versions_session = Some(connection.clone());

        let store_id = self.store.id.clone();
        let entity = cx.entity().clone();

        let task = cx.background_executor().spawn({
            let name = name.clone();
            async move {
                match connection.secret_store(&store_id) {
                    Some(store) => store.list_versions(&name),
                    None => Err(store_unavailable(&store_id)),
                }
            }
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_versions(generation, &name, result, cx)
                });
            });
        })
        .detach();
    }

    /// Lands a history. A response for another secret, or from a load that
    /// a newer selection, refresh or connection replaced, is dropped; a
    /// connection replaced since the load started drops it and lists again
    /// from the new session.
    pub(crate) fn apply_versions(
        &mut self,
        generation: u64,
        name: &str,
        result: Result<Vec<SecretVersionInfo>, DbError>,
        cx: &mut Context<Self>,
    ) {
        let is_current = generation == self.versions_generation
            && self.selected == Some(SecretRowKey::Secret(name.to_string()));

        if !is_current {
            return;
        }

        if !self.history_session_is_live(cx) {
            self.request_versions(name.to_string(), HistoryLoad::Automatic, cx);
            cx.notify();
            return;
        }

        self.versions = match result {
            Ok(versions) => VersionsState::Loaded(versions),
            Err(error) => {
                // First catch site, and only for a failure that still
                // applies: a superseded request fails silently above.
                report_error(db_error_to_user_facing(&error), cx);
                VersionsState::Failed(error.to_string())
            }
        };
        cx.notify();
    }

    /// Whether the history was listed from the connection the profile
    /// holds now. A connection replaced since — a reconnect, a region switch,
    /// a credentials refresh — is another session, whose versions the
    /// listed ids do not name.
    pub(crate) fn history_session_is_live(&self, cx: &Context<Self>) -> bool {
        match (&self.versions_session, self.connection(cx)) {
            (Some(listed), Some(live)) => Arc::ptr_eq(listed, &live),
            _ => false,
        }
    }

    /// Lists the selected secret's history again when the connection it was
    /// listed from is gone, or when a connection returned after the history
    /// had none to be listed from. Independent of the unsaved-edit guard: the
    /// history carries no edit, and stale ids must never be retrievable.
    pub(crate) fn adopt_connection_session(
        &mut self,
        trigger: HistoryLoad,
        cx: &mut Context<Self>,
    ) {
        let Some(SecretRowKey::Secret(name)) = self.selected.clone() else {
            return;
        };

        if matches!(self.versions, VersionsState::NotSupported)
            || self.history_session_is_live(cx)
            || (self.versions_session.is_none() && self.connection(cx).is_none())
        {
            return;
        }

        self.request_versions(name, trigger, cx);
        cx.notify();
    }

    /// The version the store serves as the selected secret's current value:
    /// the describe's, else the one the history marks current. Nothing is
    /// inferred: without either, no version is known to be current.
    pub(crate) fn current_version_id(&self) -> Option<&str> {
        let described = match &self.details {
            DetailsState::Loaded(details) => details.summary.version.as_deref(),
            _ => None,
        };

        described.or_else(|| self.history_current_version())
    }

    /// The retrieved version id when the store's latest information says it
    /// is no longer (or never was) the current one, evaluated on every call
    /// so a rotation that lands later turns a once-current value into an
    /// older one.
    pub(crate) fn shown_older_version(&self) -> Option<&str> {
        // Only a value that loaded is an older value; a failed or pending
        // retrieval has nothing to caption.
        if !matches!(self.value, ValueState::Loaded(_)) {
            return None;
        }

        let shown = self.value_version.as_deref()?;

        // Without a known current version the shown one stays named and
        // returnable: the retrieval asked for a version, not for the current.
        match self.current_version_id() {
            Some(current) => (shown != current).then_some(shown),
            None => Some(shown),
        }
    }

    fn history_current_version(&self) -> Option<&str> {
        let VersionsState::Loaded(versions) = &self.versions else {
            return None;
        };

        versions
            .iter()
            .find(|version| version.is_current)
            .map(|version| version.id.as_str())
    }

    pub(crate) fn is_current_version(&self, version: &str) -> bool {
        self.current_version_id() == Some(version)
    }

    /// Retrieves the value of the version at `index` of the history, through
    /// the same audited retrieval Retrieve value uses. The request always
    /// names the row's id, the current version's row included: the current
    /// version can rotate between the listing and the click.
    pub fn activate_version(&mut self, index: usize, cx: &mut Context<Self>) {
        let VersionsState::Loaded(versions) = &self.versions else {
            return;
        };

        let Some(id) = versions.get(index).map(|version| version.id.clone()) else {
            return;
        };

        // The ids belong to the session that listed them; against another
        // connection they could name a different version or none.
        if !self.history_session_is_live(cx) {
            self.adopt_connection_session(HistoryLoad::Requested, cx);
            return;
        }

        self.retrieve_version(Some(id), cx);
    }

    /// Whether Back to current may run: a historical value is shown and no
    /// retrieval is in flight.
    pub fn can_return_to_current(&self) -> bool {
        self.shown_older_version().is_some()
    }

    /// Replaces a historical value with the current one.
    pub fn back_to_current_version(&mut self, cx: &mut Context<Self>) {
        if self.can_return_to_current() {
            self.retrieve_version(None, cx);
        }
    }

    /// Retrieve value: fetches the selected secret's current value. Every
    /// retrieval, successful or not, records one audit event without the
    /// value. Refused for a secret whose deletion is pending.
    pub fn retrieve_selected_value(&mut self, cx: &mut Context<Self>) {
        self.retrieve_version(None, cx);
    }

    /// Retrieves `version` of the selected secret, the current value when
    /// `None`. One path for both, so every retrieval is audited the same way.
    /// The retrieved id is kept; whether it reads as an older value is decided
    /// against the latest current-version information when it is drawn.
    pub(crate) fn retrieve_version(&mut self, version: Option<String>, cx: &mut Context<Self>) {
        if !self.can_retrieve_value() {
            return;
        }

        // Retrieving replaces the value section and drops the open editor.
        if self.block_unsaved_value_edits(cx) {
            return;
        }

        let Some(name) = self.selected_summary().map(|summary| summary.name.clone()) else {
            return;
        };

        self.clear_value();
        self.value_version = version.clone();
        let generation = self.value_generation;
        self.value = ValueState::Loading;
        cx.notify();

        let Some(connection) = self.connection(cx) else {
            self.apply_value(
                generation,
                &name,
                version.as_deref(),
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
            let version = version.clone();
            async move {
                let started = Instant::now();
                let result = match connection.secret_store(&store_id) {
                    Some(store) => store.get_value(&name, version.as_deref()),
                    None => Err(store_unavailable(&store_id)),
                };

                (result, started.elapsed().as_millis())
            }
        });

        cx.spawn(async move |_this, cx| {
            let (result, millis) = task.await;

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_value(generation, &name, version.as_deref(), result, millis, cx)
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
        requested_version: Option<&str>,
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

                self.record_audit(
                    value_retrieval_event(&target, requested_version, Some(&failure)),
                    cx,
                );
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

/// Why the history is being listed: something the user did, or a reload the
/// document runs on its own after the connection changed.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistoryLoad {
    Requested,
    Automatic,
}

fn store_unavailable(store_id: &str) -> DbError {
    DbError::NotSupported(dbflux_i18n::t!(
        "document.secret_store.error.store_unavailable",
        store = store_id
    ))
}
