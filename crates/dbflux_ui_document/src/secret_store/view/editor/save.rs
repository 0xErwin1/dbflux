//! Saving the edited text: the governed write, its session capture and the
//! landing of its answer. The editor's state and mode live in the parent
//! module; nothing here changes what Save writes, only how it is sent.

use std::sync::Arc;

use dbflux_core::chrono::Utc;
use dbflux_core::observability::EventSeverity;
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    Connection, DbError, MutationPolicy, ReadOnlyReason, SecretPayload, SecretPutRequest,
    SecretStoreCapabilities, SecretValue,
};
use dbflux_ui_base::notifications::ErrorNotification;
use dbflux_ui_base::toast::{Toast, ToastAction, copy_action, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::{App, AppContext as _, Context};

use super::{EditMode, parses_as_json};
use crate::object_text::db_error_to_user_facing;
use crate::secret_store::changes::SecretChangeKind;
use crate::secret_store::view::{DetailsState, SecretView, SecretViewEvent, ValueState};
use crate::secret_store::write::{
    SecretOperation, SecretWriteFailure, SecretWriteFailureKind, WriteTarget, put_secret_value,
    store_unavailable_failure,
};

/// What a save takes from the live connection, captured on the foreground so
/// the write can run on the background executor.
struct ValueWriteSession {
    connection: Arc<dyn Connection>,
    policy: MutationPolicy,
    read_only_reason: Option<ReadOnlyReason>,
    driver_id: String,
}

/// Lands the answer of a save on its view and, when the write may have
/// landed, announces it from the scope captured at send time. The
/// announcement does not go through the view: a tab closed mid-write has
/// nobody left to forward it.
pub(crate) fn land_save_answer(
    entity: &gpui::Entity<SecretView>,
    scope: &crate::secret_store::changes::ChangeScope,
    generation: u64,
    name: &str,
    result: Result<SecretValue, SecretWriteFailure>,
    saved_on: &Arc<dyn Connection>,
    cx: &mut App,
) {
    let may_have_landed = match &result {
        Ok(_) => true,
        Err(failure) => matches!(
            failure.kind,
            SecretWriteFailureKind::AuditLost | SecretWriteFailureKind::Indeterminate
        ),
    };

    entity.update(cx, |view, cx| {
        view.editor_save_in_flight = false;
        view.apply_value_write(generation, name, result, saved_on, cx);
    });

    if may_have_landed {
        scope.publish(name, SecretChangeKind::Changed, cx);
    }
}

impl SecretView {
    /// Saves the edited text as a new version through the governed seam. A
    /// save whose text is unchanged is a no-op, the secret's latest details
    /// and the session the value came from are re-checked, and a value the
    /// store kept as JSON must still parse as JSON — as any JSON, in the
    /// layout the user typed; the gate checks the type, not the shape.
    pub(crate) fn save_value_edit(&mut self, cx: &mut Context<Self>) {
        self.try_save_value_edit(cx);
    }

    /// [`Self::save_value_edit`], reporting whether a save is under way: one
    /// this call started, or one already in flight, whose answer the caller
    /// will see as [`SecretViewEvent::SaveFinished`]. `false` means nothing
    /// was sent (a gate refused it, or there was nothing to save), and no
    /// `SaveFinished` follows.
    pub(crate) fn try_save_value_edit(&mut self, cx: &mut Context<Self>) -> bool {
        let Some(edit) = &self.value_edit else {
            return false;
        };

        // A draft of a deleted secret cannot be written, and says nothing
        // beyond the notice its editor carries. One pending deletion is
        // refused below, with the message the other uneditable cases use.
        if edit.removed {
            return false;
        }

        if edit.saving {
            return true;
        }

        if self.save_waits_for_another_write(cx) {
            return false;
        }

        let text = edit.text_to_write(edit.editor.read(cx).value().as_str());

        if !edit.is_changed(cx) {
            return false;
        }

        let stored_is_json = edit.stored_is_json;
        let value_connection = edit.connection.clone();

        if let Some(reason) = self.value_edit_refusal_reason() {
            report_error(
                UserFacingError::new(ErrorKind::User, reason).with_severity(EventSeverity::Warn),
                cx,
            );
            return false;
        }

        let Some(session) = self.value_write_session(&value_connection, cx) else {
            return false;
        };

        if let Some(error) = stored_is_json
            .then_some(&text)
            .and_then(|text| serde_json::from_str::<serde_json::Value>(text).err())
        {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!(
                        "document.secret_store.edit.invalid_json",
                        error = error.to_string()
                    ),
                ),
                cx,
            );
            return false;
        }

        let Some(name) = self.selected_summary().map(|summary| summary.name.clone()) else {
            return false;
        };

        self.send_value_write(session, name, text, cx);
        true
    }

    /// Another write is in flight: a save out of an editor that is gone
    /// still answers, and a second one would let the first answer release
    /// the guard under it. Reports why.
    fn save_waits_for_another_write(&self, cx: &mut Context<Self>) -> bool {
        if !self.editor_save_in_flight && !self.write_in_flight {
            return false;
        }

        self.report_write_warning(
            dbflux_i18n::t!("document.secret_store.write.in_progress"),
            cx,
        );
        true
    }

    /// What the save takes from the live connection. The save lands on the
    /// session the value was read from: a profile whose connection was
    /// replaced — a region switch, a reconnect, a credentials refresh —
    /// refuses here, because the editor's value may not exist on the new
    /// session and writing there would put a value somewhere the user did not
    /// see.
    fn value_write_session(
        &mut self,
        value_connection: &Arc<dyn Connection>,
        cx: &mut Context<Self>,
    ) -> Option<ValueWriteSession> {
        // A region switch is about to replace the session this would write
        // to: the shared guard comes before the connection is read.
        if self.region_switch_blocks_writes(cx) || self.region_mismatch(cx) {
            return None;
        }

        let connected = self.live_profile(cx);

        let Some(connected) = connected else {
            self.missing_connection_reported = true;
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.secret_store.error.connection_unavailable"),
                ),
                cx,
            );
            return None;
        };

        if !Arc::ptr_eq(&connected.connection, value_connection) {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.secret_store.edit.connection_replaced"),
                )
                .with_severity(EventSeverity::Warn),
                cx,
            );
            return None;
        }

        Some(ValueWriteSession {
            connection: connected.connection.clone(),
            policy: connected.mutation_policy,
            read_only_reason: connected.read_only_reason,
            driver_id: connected.connection.metadata().id.clone(),
        })
    }

    /// Sends the edited text through the seam on the background executor and
    /// lands the answer on the view.
    fn send_value_write(
        &mut self,
        session: ValueWriteSession,
        name: String,
        text: String,
        cx: &mut Context<Self>,
    ) {
        let saved_on = session.connection.clone();
        let ValueWriteSession {
            connection,
            policy,
            read_only_reason,
            driver_id,
        } = session;
        let audit = self.app_state.read(cx).audit_service().clone();
        let store_id = self.store.id.clone();
        let store_label = self.store.label.clone();
        let profile_id = self.profile_id;
        let generation = self.value_write_generation;
        let entity = cx.entity().clone();
        let request = SecretPutRequest::text(name.clone(), text);
        // Captured now: the announcement of a write that may have landed must
        // not depend on the view or its tab being alive when the answer comes.
        let scope = self.change_scope(saved_on.current_region(), cx);

        if let Some(edit) = &mut self.value_edit {
            edit.saving = true;
            // Saving writes over whatever changed elsewhere before this point;
            // a change that arrives from here on is a new one.
            edit.changed_elsewhere = false;
        }
        self.editor_save_in_flight = true;
        cx.notify();

        let task = cx.background_executor().spawn(async move {
            match connection.secret_store(&store_id) {
                Some(store) => put_secret_value(
                    store,
                    profile_id,
                    driver_id.as_str(),
                    policy,
                    read_only_reason,
                    &audit,
                    &request,
                ),
                None => Err(store_unavailable_failure(
                    &audit,
                    SecretOperation::PutValue,
                    &WriteTarget {
                        profile_id,
                        driver_id: driver_id.as_str(),
                        store_id: store_id.as_str(),
                        store_label: store_label.as_str(),
                        name: request.name.as_str(),
                    },
                    DbError::NotSupported(dbflux_i18n::t!(
                        "document.secret_store.error.store_unavailable",
                        store = store_id
                    )),
                )),
            }
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                land_save_answer(&entity, &scope, generation, &name, result, &saved_on, cx);
            });
        })
        .detach();
    }

    /// Lands a governed write. A write the store accepted shows the value
    /// the seam returned — its new version id — masks everything again and
    /// refreshes the details; the editor closes when it still holds what was
    /// saved, and stays open over text typed while the write was in flight,
    /// with the saved text as its baseline. A refusal or a failure keeps the
    /// editor open so the text can be fixed or dropped. The audit event of
    /// every outcome was recorded by the seam itself, so the report below
    /// adds none.
    pub(crate) fn apply_value_write(
        &mut self,
        generation: u64,
        name: &str,
        result: Result<SecretValue, SecretWriteFailure>,
        saved_on: &Arc<dyn Connection>,
        cx: &mut Context<Self>,
    ) {
        let succeeded = result.is_ok();

        self.land_value_write(generation, name, result, saved_on, cx);
        cx.emit(SecretViewEvent::SaveFinished { succeeded });
    }

    fn land_value_write(
        &mut self,
        generation: u64,
        name: &str,
        result: Result<SecretValue, SecretWriteFailure>,
        saved_on: &Arc<dyn Connection>,
        cx: &mut Context<Self>,
    ) {
        let is_current = generation == self.value_write_generation && self.is_for(name);

        // The save ran on the session captured when it started. If the
        // connection was replaced since, the displayed value and the editor
        // belong to whatever the user sees now: nothing of them is touched,
        // the unsaved text stays, and the user is told where the write went.
        // A connection the view cannot see is a replaced session when the
        // profile is connected on another region (the view is bound to one);
        // a profile with no connection at all is a disconnect, which the
        // view reports on its own.
        let replaced = match self.connection(cx) {
            Some(live) => !Arc::ptr_eq(&live, saved_on),
            None => self.region_mismatch(cx),
        };

        if replaced {
            match result {
                Ok(_) => self.report_write_warning(
                    dbflux_i18n::t!("document.secret_store.write.landed_on_previous_session"),
                    cx,
                ),
                Err(failure) => {
                    self.report_seam_write_failure(write_failure_to_user_facing(&failure), cx)
                }
            }

            if let Some(edit) = &mut self.value_edit {
                edit.saving = false;
            }
            cx.notify();
            return;
        }

        match result {
            Ok(written) => {
                if is_current {
                    // Another view changed the secret while this save was in
                    // flight: the answer cannot say which write came last, so
                    // what it carries is not shown as current.
                    let stale = self
                        .value_edit
                        .as_ref()
                        .is_some_and(|edit| edit.changed_elsewhere);

                    if stale {
                        self.value = ValueState::NotLoaded;
                        self.value_session = None;
                    } else {
                        self.value = ValueState::Loaded(Box::new(
                            crate::secret_store::RetrievedValue::new(&written),
                        ));
                        self.value_session = Some(saved_on.clone());
                    }
                    // The saved text is the current version now, whatever
                    // version the pane was showing.
                    self.value_version = None;

                    // The saved text becomes the editor's new baseline: an
                    // editor whose text still matches what was saved closes,
                    // one holding text typed during the write stays open.
                    let saved_text = match &written.payload {
                        SecretPayload::Text(text) => text.expose_secret().to_string(),
                        // The editor exists for text values only.
                        SecretPayload::Binary(_) => String::new(),
                    };
                    let stored_is_json = parses_as_json(&saved_text);
                    let keep_editing = self.value_edit.as_ref().is_some_and(|edit| {
                        edit.text_to_write(edit.editor.read(cx).value().as_str()) != saved_text
                    });

                    if keep_editing {
                        if let Some(edit) = &mut self.value_edit {
                            edit.original = SecretString::from(saved_text);
                            edit.stored_is_json = stored_is_json;
                            if !stored_is_json {
                                edit.mode = EditMode::Raw;
                            }
                            edit.saving = false;
                        }
                    } else {
                        self.value_edit = None;
                    }

                    cx.notify();
                    // The details refresh, so the version the pane reports
                    // is the version the store now holds.
                    self.describe(name.to_string(), cx);
                }

                // The new version and the labels that moved with it. Listing
                // again also invalidates a history request still in flight,
                // which predates the write.
                if self.is_for(name) {
                    self.request_versions(
                        name.to_string(),
                        crate::secret_store::HistoryLoad::Automatic,
                        cx,
                    );
                }

                cx.emit(SecretViewEvent::ReloadListing);
            }
            Err(failure) => {
                self.report_seam_write_failure(write_failure_to_user_facing(&failure), cx);

                if is_current {
                    if let Some(edit) = &mut self.value_edit {
                        edit.saving = false;
                    }
                    cx.notify();
                }

                // The store may have taken the write: read the secret again
                // and let the listing follow. The editor keeps its text.
                if matches!(
                    failure.kind,
                    SecretWriteFailureKind::AuditLost | SecretWriteFailureKind::Indeterminate
                ) && self.is_for(name)
                {
                    self.describe(name.to_string(), cx);
                    self.request_versions(
                        name.to_string(),
                        crate::secret_store::HistoryLoad::Automatic,
                        cx,
                    );
                    cx.emit(SecretViewEvent::ReloadListing);
                }
            }
        }
    }

    /// Reports a failure the seam already audited. The toast and the
    /// notification-center entry go out through the same pieces
    /// [`report_error`] uses — same styling, same correlation details, same
    /// View-in-audit action — but without its tracing event: the audit
    /// bridge turns that event into a second, `user_error` row next to the
    /// one the seam recorded, and a failed write must not be audited twice.
    pub(in crate::secret_store) fn report_seam_write_failure(
        &self,
        failure: UserFacingError,
        cx: &mut Context<Self>,
    ) {
        let id = failure.correlation_id;
        let id_str = id.to_string();
        let summary = failure.summary.clone();

        let mut toast = match failure.severity {
            EventSeverity::Warn => Toast::warning(failure.summary.clone()),
            _ => Toast::error(failure.summary.clone()),
        };

        if let Some(cause) = &failure.cause {
            toast = toast.code_block(cause.clone());
        }
        if let Some(suggested) = &failure.suggested_action {
            toast = toast.body(suggested.clone());
        }

        let app_state = self.app_state.clone();
        toast
            .meta_right(now_hms())
            .details(dbflux_i18n::t!("errors.correlation.detail", id = id_str))
            .action(copy_action(dbflux_i18n::t!(
                "errors.correlation.clipboard",
                summary = summary.as_str(),
                id = id_str
            )))
            .action(
                ToastAction::new(
                    "view-in-audit",
                    dbflux_i18n::t!("errors.action.view_in_audit"),
                )
                .on_click(move |cx: &mut App| {
                    app_state.update(cx, |state, cx| {
                        state.request_open_audit(Some(id), cx);
                    });
                }),
            )
            .push(cx);

        self.app_state.update(cx, |state, cx| {
            state
                .notifications
                .record_error(ErrorNotification::from_user_error(&failure, Utc::now()));
            state.note_user_error(id, failure.severity, cx);
        });
    }
}

/// How a failed governed write reaches the user. The seam's own verdict
/// decides, never the error variant: a refusal — the policy's, or the
/// store's own write capability — is a decision, not a transient error, so
/// it reads as one, a warning of the `User` kind. A store failure keeps the
/// driver-facing error the retrieval path builds, even when its variant is
/// one a refusal also uses. A write that landed with its audit row lost is
/// neither: the message says the write stands and must not be retried, so
/// it reads as an audit-storage problem at warning severity, not as a
/// failure to repeat. The correlation id a store failure carries replaces
/// the fresh one, so the toast and the failure's audit row tell one story.
pub(in crate::secret_store) fn write_failure_to_user_facing(
    failure: &SecretWriteFailure,
) -> UserFacingError {
    let mut user_error = match failure.kind {
        SecretWriteFailureKind::Refused => {
            UserFacingError::new(ErrorKind::User, failure.error.to_string())
                .with_severity(EventSeverity::Warn)
        }
        SecretWriteFailureKind::StoreFailed => db_error_to_user_facing(&failure.error),
        SecretWriteFailureKind::Indeterminate => {
            db_error_to_user_facing(&failure.error).with_severity(EventSeverity::Warn)
        }
        SecretWriteFailureKind::AuditLost => {
            UserFacingError::new(ErrorKind::Storage, failure.error.to_string())
                .with_severity(EventSeverity::Warn)
        }
    };

    if let Some(correlation_id) = failure.correlation_id {
        user_error.correlation_id = correlation_id;
    }

    if failure.kind == SecretWriteFailureKind::Indeterminate {
        user_error.summary.push_str(&format!(
            " {}",
            dbflux_i18n::t!("document.secret_store.error.may_have_landed")
        ));
    }
    if failure.audit_unrecorded {
        user_error.summary.push_str(&format!(
            " {}",
            dbflux_i18n::t!("document.secret_store.error.audit_unrecorded")
        ));
    }

    user_error
}
