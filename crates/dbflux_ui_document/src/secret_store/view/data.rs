//! Store calls of one secret: its describe, its version history and its
//! value. Each one runs on the background executor, because the seam is
//! synchronous, and lands on the foreground through a generation check and a
//! check that the view still shows the secret, so a result that a newer call
//! or another selection replaced is dropped.

use super::{DetailsState, SecretView, SecretViewEvent, ValueState, VersionsState};
use crate::object_text::db_error_to_user_facing;
use crate::secret_store::data::{HistoryLoad, store_unavailable};
use crate::secret_store::value::{
    RetrievalTarget, RetrievedValue, ValueFailure, value_retrieval_event,
};
use crate::secret_store::{CallTiming, StoreCall};
use dbflux_core::{
    Connection, DbError, SecretDetails, SecretStoreCapabilities, SecretValue, SecretVersionInfo,
};
use dbflux_ui_base::user_error::{
    ErrorKind, UserFacingError, record_error, report_error, report_error_async,
};
use gpui::Context;
use std::sync::Arc;
use std::time::Instant;

impl SecretView {
    /// The live connection of the profile the view reads from.
    pub(crate) fn connection(&self, cx: &Context<Self>) -> Option<Arc<dyn Connection>> {
        self.live_profile(cx)
            .map(|connected| connected.connection.clone())
    }

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

        self.note_describe_issued(&name, generation);
        self.details_session = Some(connection.clone());
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
                entity.update(cx, |view, cx| {
                    view.apply_details(generation, &name, result, millis, cx)
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
        let is_current = generation == self.details_generation && self.is_for(name);

        if !is_current {
            return;
        }

        self.drop_confirmations_of_replaced_session(cx);

        // A describe read from a session the profile no longer holds says
        // nothing of the new one: its date, or its absence, must not reach the
        // new connection's state. Read again from the live session instead.
        if self.connection(cx).is_some() && !self.details_session_is_live(cx) {
            self.describe(name.to_string(), cx);
            cx.emit(SecretViewEvent::ReloadListing);
            cx.notify();
            return;
        }

        if let Ok(details) = &result {
            self.observe_described(&details.summary, generation);
        }

        let result = result.map(|mut details| {
            self.conform_details(&mut details);
            details
        });

        if let Ok(details) = &result {
            self.summary_cache = Some(details.summary.clone());
        }

        let pending = result
            .as_ref()
            .ok()
            .map(|details| self.is_deletion_pending(&details.summary));

        self.details = match result {
            Ok(details) => DetailsState::Loaded(Box::new(details)),
            Err(error) => DetailsState::Failed(error.to_string()),
        };
        self.reconcile_history_with_deletion(pending, cx);
        cx.emit(SecretViewEvent::StoreCalled(CallTiming {
            call: StoreCall::Describe,
            millis,
        }));
        cx.notify();
    }

    /// Whether the shown secret's deletion is scheduled, by the freshest
    /// word available: a loaded describe over the listing's row, which a write
    /// can outdate.
    pub(crate) fn deletion_pending(&self) -> bool {
        match &self.details {
            DetailsState::Loaded(details) => self.is_deletion_pending(&details.summary),
            _ => self
                .selected_summary()
                .is_some_and(|summary| self.is_deletion_pending(summary)),
        }
    }

    /// Brings the history in line with what a describe just said about the
    /// deletion: a scheduled one drops the history and stops asking for it, a
    /// restore (or any describe of an active secret) resumes it. `None` is a
    /// failed describe, which says nothing.
    fn reconcile_history_with_deletion(&mut self, pending: Option<bool>, cx: &mut Context<Self>) {
        let Some(name) = self.name.clone() else {
            return;
        };

        // A store without a history has nothing to schedule or resume: it
        // keeps saying so, pending or not.
        if !self.store.supports(SecretStoreCapabilities::VERSIONS) {
            return;
        }

        let scheduled = matches!(self.versions, VersionsState::ScheduledForDeletion);

        match pending {
            Some(true) if !scheduled => {
                self.versions_generation = self.versions_generation.wrapping_add(1);
                self.versions = VersionsState::ScheduledForDeletion;
                self.versions_session = None;
            }
            Some(false) if scheduled => {
                self.request_versions(name, HistoryLoad::Automatic, cx);
            }
            _ => {}
        }
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

        // The service refuses the history of a secret scheduled for
        // deletion, so it is not asked until the secret is restored.
        let pending = self.deletion_pending();

        if !pending {
            self.versions = VersionsState::Loading;
        }
        self.versions_session = None;

        let Some(connection) = self.connection(cx) else {
            // A reload nobody asked for finding no connection is a
            // disconnect, not a failure: nothing is asked and nothing is
            // reported, and the history loads again when it returns.
            if trigger == HistoryLoad::Automatic {
                self.versions = VersionsState::NotConnected;
                return;
            }

            let message = dbflux_i18n::t!("document.secret_store.error.connection_unavailable");

            // Only the first catch site reports: an editor path that found
            // the same loss already did.
            if !self.missing_connection_reported || trigger == HistoryLoad::Requested {
                self.missing_connection_reported = true;
                report_error(UserFacingError::new(ErrorKind::User, message.clone()), cx);
            }
            self.versions = VersionsState::Failed(message);
            return;
        };

        self.missing_connection_reported = false;

        if pending {
            self.versions = VersionsState::ScheduledForDeletion;
            return;
        }

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
                entity.update(cx, |view, cx| {
                    view.apply_versions(generation, &name, result, cx)
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
        let is_current = generation == self.versions_generation && self.is_for(name);

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
                // Shown inline in the section: a toast would cover the
                // pane's own controls, and the history never blocks them.
                // It is still recorded: audit trail, notification center
                // and the error badge.
                record_error(db_error_to_user_facing(&error), cx);
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
        let Some(name) = self.name.clone() else {
            return;
        };

        if matches!(
            self.versions,
            VersionsState::NotSupported | VersionsState::ScheduledForDeletion
        ) || self.history_session_is_live(cx)
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

        // A retrieval of the current value asked for no version, but the
        // store named the one it served: that id stays with the value, so a
        // rotation that moves the current version later still shows it as
        // the older one.
        let (shown, asked_for_it) = match self.value_version.as_deref() {
            Some(requested) => (requested, true),
            None => match &self.value {
                ValueState::Loaded(value) => (value.version.as_deref()?, false),
                _ => return None,
            },
        };

        // Without a known current version a version the retrieval asked for
        // stays named and returnable; one the store merely served is not
        // called older on a guess.
        match self.current_version_id() {
            Some(current) => (shown != current).then_some(shown),
            None => asked_for_it.then_some(shown),
        }
    }

    /// The version the Versions table highlights: the one the value section
    /// shows or is retrieving.
    pub(crate) fn highlighted_version(&self) -> Option<&str> {
        match &self.value {
            ValueState::Loading => self
                .value_version
                .as_deref()
                .or_else(|| self.current_version_id()),
            // The store named the version it served even when the retrieval
            // asked for the current one, and a rotation since may have moved
            // the current version on.
            ValueState::Loaded(value) => self
                .value_version
                .as_deref()
                .or(value.version.as_deref())
                .or_else(|| self.current_version_id()),
            _ => None,
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
    pub(crate) fn activate_version(&mut self, index: usize, cx: &mut Context<Self>) {
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
    pub(crate) fn can_return_to_current(&self) -> bool {
        self.shown_older_version().is_some()
    }

    /// Replaces a historical value with the current one.
    pub(crate) fn back_to_current_version(&mut self, cx: &mut Context<Self>) {
        if self.can_return_to_current() {
            self.retrieve_version(None, cx);
        }
    }

    /// Retrieve value: fetches the selected secret's current value. Every
    /// retrieval, successful or not, records one audit event without the
    /// value. Refused for a secret whose deletion is pending.
    pub(crate) fn retrieve_selected_value(&mut self, cx: &mut Context<Self>) {
        self.retrieve_version(self.opened_version.clone(), cx);
    }

    /// Retrieves `version` of the selected secret, the current value when
    /// `None`. One path for both, so every retrieval is audited the same way.
    /// The retrieved id is kept; whether it reads as an older value is decided
    /// against the latest current-version information when it is drawn.
    pub(crate) fn retrieve_version(&mut self, version: Option<String>, cx: &mut Context<Self>) {
        if !self.can_retrieve_value() {
            // The button is disabled without a connection; a command that
            // still reaches here says why nothing happened.
            if !self.connection_present
                && !matches!(self.value, ValueState::Loading)
                && self
                    .selected_summary()
                    .is_some_and(|summary| !self.is_deletion_pending(summary))
            {
                self.missing_connection_reported = true;
                report_error(
                    UserFacingError::new(
                        ErrorKind::User,
                        dbflux_i18n::t!("document.secret_store.error.connection_unavailable"),
                    ),
                    cx,
                );
            }

            return;
        }

        // The connection can be gone before the view heard of it. Nothing was
        // read, so nothing is recorded: the audit row names the driver of the
        // session that served the read.
        let Some(connection) = self.connection(cx) else {
            self.missing_connection_reported = true;
            self.sync_connection_presence(cx);
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.secret_store.error.connection_unavailable"),
                ),
                cx,
            );
            return;
        };

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

        let session = connection.clone();
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
                entity.update(cx, |view, cx| {
                    view.apply_value(
                        ValueAsk {
                            generation,
                            name: &name,
                            requested_version: version.as_deref(),
                            session,
                        },
                        result,
                        millis,
                        cx,
                    )
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
        ask: ValueAsk<'_>,
        result: Result<SecretValue, DbError>,
        millis: u128,
        cx: &mut Context<Self>,
    ) {
        let ValueAsk {
            generation,
            name,
            requested_version,
            session,
        } = ask;
        let asked = generation == self.value_generation && self.is_for(name);

        // The answer belongs to the session it was asked of. After the
        // connection was replaced it is not shown under the new one, and the
        // section does not stay loading for it.
        let live = self.connection(cx);
        let same_session = live
            .as_ref()
            .is_some_and(|live| Arc::ptr_eq(&session, live));
        let is_current = asked && same_session;

        if asked && !same_session {
            self.value = ValueState::NotLoaded;
            self.value_version = None;
            cx.notify();
        }

        // The store that was read is the originating session's, even when
        // the profile holds no connection any more.
        let driver_id = session.metadata().id.clone();

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
                    self.value_session = Some(session);
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
            cx.emit(SecretViewEvent::StoreCalled(CallTiming {
                call: StoreCall::Retrieve,
                millis,
            }));
            cx.notify();
        }
    }

    fn record_audit(&self, event: dbflux_core::observability::EventRecord, cx: &gpui::App) {
        if let Err(error) = self.app_state.read(cx).audit_service().record(event) {
            log::warn!("[secret store] failed to record value retrieval audit event: {error}");
        }
    }
}

/// What a retrieval was asked, to judge its answer against when it lands.
pub(crate) struct ValueAsk<'a> {
    pub generation: u64,
    pub name: &'a str,
    /// The version the retrieval named, `None` for the current value.
    pub requested_version: Option<&'a str>,
    /// The session it was asked of.
    pub session: Arc<dyn Connection>,
}
