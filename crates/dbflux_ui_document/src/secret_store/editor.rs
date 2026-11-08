//! The edit mode of a plaintext value: the editor surface, the governed
//! write behind Save, and how a landed write lands in the pane.
//!
//! Save never calls the store directly: it goes through
//! [`put_secret_value`], the same governed seam the write path owns, with
//! the connection's resolved [`MutationPolicy`] and the app's audit service.
//! The seam refuses a read-only or approval-required connection and a store
//! that does not advertise `WRITE` before the store is called, and records
//! exactly one audit event per attempt without the value.
//!
//! The write is staged as the current version: a landed write replaces the
//! pane's value with the one the seam returned — its new version id —
//! masks everything again and refreshes the details, so the version the
//! pane reports is the version the store now holds. The editor closes when
//! it still holds what was saved; text typed while the write was in flight
//! stays, with the saved text as its new baseline, so the next Save writes
//! it as its own version.
//!
//! The save is pinned to the session the value was read from: a connection
//! replaced under the open editor — a region switch, a reconnect — refuses
//! the write instead of landing it somewhere the user did not see, and the
//! secret's latest details are re-checked, so a secret that has meanwhile
//! become managed or deletion-pending refuses with the vocabulary the
//! uneditable cases use.
//!
//! Nothing of the value reaches a log, an error, an audit field or an
//! element id: the editor element is marked redacted, so rendered-frame
//! observers withhold its text and its accesskit node carries no value, and
//! the failures it reports are the seam's own scrubbed messages. A save
//! whose text is unchanged writes nothing, and a failure the seam already
//! audited is reported without the error seam's tracing event, so it is not
//! audited a second time.
//!
//! The JSON gate is typed, not shaped: a value the store kept as JSON is
//! saved only as JSON — whatever JSON the user typed, in the layout they
//! typed it in, of any shape or type, from a compact object to a bare
//! number. It does not promise to preserve the stored layout or the
//! value's shape beyond what the user typed, and a stored value that is not
//! JSON has no gate at all.

use std::sync::Arc;

use dbflux_core::chrono::Utc;
use dbflux_core::observability::EventSeverity;
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    Connection, DbError, MutationPolicy, SecretPayload, SecretPutRequest, SecretStoreCapabilities,
    SecretValue,
};
use dbflux_ui_base::notifications::ErrorNotification;
use dbflux_ui_base::toast::{Toast, ToastAction, copy_action, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::{App, AppContext as _, Context, Entity, prelude::*};
use gpui_component::input::TextareaState;
use uuid::Uuid;

use super::write::{SecretWriteFailure, SecretWriteFailureKind, put_secret_value};
use super::{DetailsState, SecretRowKey, SecretStoreDocument, ValueState};
use crate::object_text::db_error_to_user_facing;
use crate::types::DocumentState;

/// The open editor of the loaded plaintext value.
pub(crate) struct ValueEditState {
    /// The stored text the editor opened with: the unchanged check and the
    /// JSON gate compare against it.
    pub(super) original: SecretString,

    /// Whether the stored text parses as JSON. A JSON value must be saved as
    /// JSON: a Save whose text no longer parses is refused before the seam.
    pub(super) stored_is_json: bool,

    /// The multi-line editor surface, built when the edit started.
    pub(super) editor: Entity<TextareaState>,

    /// True while a governed write is in flight, so a second Save waits and
    /// Discard does not drop what the landing still has to read.
    pub(super) saving: bool,

    /// The connection session the value was read from. A save names this
    /// session, so a connection replaced under the open editor refuses the
    /// write instead of landing it on a connection the value never came
    /// from.
    pub(super) connection: Arc<dyn Connection>,
}

impl ValueEditState {
    /// Whether the editor's text differs from the stored text it opened with.
    pub(super) fn is_changed(&self, cx: &App) -> bool {
        self.editor.read(cx).value().as_str() != self.original.expose_secret()
    }
}

impl SecretStoreDocument {
    /// Whether the details pane offers an editor for the loaded value: a
    /// plaintext value of a secret nobody else manages and whose deletion is
    /// not pending, in a store that advertises `WRITE`. A connection whose
    /// policy refuses writes keeps the editor — the refusal happens at Save,
    /// through the seam, where the decision is audited.
    pub fn can_edit_value(&self) -> bool {
        if self.value_edit.is_some() {
            return false;
        }

        let ValueState::Loaded(value) = &self.value else {
            return false;
        };

        if value.content.is_binary() || value.content.has_key_value_view() {
            return false;
        }

        let Some(summary) = self.selected_summary() else {
            return false;
        };

        summary.owning_service.is_none()
            && summary.deletion_scheduled_for.is_none()
            && self.store.supports(SecretStoreCapabilities::WRITE)
    }

    /// Whether the value editor is open.
    pub fn is_editing_value(&self) -> bool {
        self.value_edit.is_some()
    }

    /// Whether a governed write of this editor is in flight.
    pub fn is_saving_value_edit(&self) -> bool {
        self.value_edit.as_ref().is_some_and(|edit| edit.saving)
    }

    /// Whether the open editor holds text that no save has written: the
    /// dirty dot, the unsaved-changes prompt on close and the guards on the
    /// transitions that would drop the editor all read this.
    pub fn has_unsaved_value_edits(&self, cx: &App) -> bool {
        self.value_edit
            .as_ref()
            .is_some_and(|edit| edit.is_changed(cx))
    }

    /// The description the tab's dirty dot and the unsaved-changes prompt
    /// show while the editor holds unsaved text; `None` when there is
    /// nothing unsaved.
    pub fn unsaved_change_summary(&self, cx: &App) -> Option<String> {
        self.has_unsaved_value_edits(cx)
            .then(|| dbflux_i18n::t!("document.secret_store.edit.change_summary").to_string())
    }

    /// Opens the editor on the loaded value with the stored text. A value
    /// that may not be edited ignores the request; a connection that is gone
    /// is reported, as a save without one is.
    pub fn start_value_edit(&mut self, window: &mut gpui::Window, cx: &mut Context<Self>) {
        if !self.can_edit_value() {
            return;
        }

        let Some(connection) = self.connection(cx) else {
            self.missing_connection_reported = true;
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.secret_store.error.connection_unavailable"),
                ),
                cx,
            );
            return;
        };

        let ValueState::Loaded(value) = &self.value else {
            return;
        };

        let stored = value.content.full_text().expose_secret().to_string();
        let stored_is_json = serde_json::from_str::<serde_json::Value>(&stored).is_ok();

        let editor = cx.new(|cx| {
            let mut state = TextareaState::new(window, cx)
                .auto_grow(2, super::details::VIEWER_VISIBLE_LINES as usize)
                .soft_wrap(true);
            state.set_value(&stored, window, cx);
            state
        });
        editor.update(cx, |state, cx| state.focus(window, cx));

        self.value_edit = Some(ValueEditState {
            original: SecretString::from(stored),
            stored_is_json,
            editor,
            saving: false,
            connection,
        });
        // The single-line fields the value drew are not drawn in edit mode;
        // they are rebuilt from scratch when the editor closes.
        self.value_inputs.clear();
        self.value_input_masks.clear();
        self.value_inputs_view = None;
        cx.notify();
    }

    /// Closes the editor without writing. The value the pane holds is the
    /// one the store still has.
    ///
    /// A stale action must not drop an editor it was never aimed at: while a
    /// save is in flight the landing still has to read what was typed after
    /// it started, so Discard sits out, and the pane action is disabled so
    /// the queue cannot carry one into the landing.
    pub fn discard_value_edit(&mut self, cx: &mut Context<Self>) {
        if self.is_saving_value_edit() {
            return;
        }

        if self.value_edit.take().is_some() {
            cx.notify();
        }
    }

    /// The refusal the secret's latest state imposes on a save. The listing
    /// and the freshest describe agree with the cases the details pane
    /// refuses an editor for: a secret another service manages, and one
    /// whose deletion is scheduled. The store's write capability cannot
    /// change within a session — the descriptor is fixed at connect — and a
    /// replaced session is refused on its own check below.
    fn value_edit_refusal_reason(&self) -> Option<String> {
        let summary = self.selected_summary()?;

        if let Some(service) = summary.owning_service.as_deref() {
            return Some(dbflux_i18n::t!(
                "document.secret_store.edit.refused_managed",
                name = summary.name.as_str(),
                service = service
            ));
        }

        if summary.deletion_scheduled_for.is_some() {
            return Some(dbflux_i18n::t!(
                "document.secret_store.edit.refused_deletion",
                name = summary.name.as_str()
            ));
        }

        if let DetailsState::Loaded(details) = &self.details {
            let summary = &details.summary;

            if let Some(service) = summary.owning_service.as_deref() {
                return Some(dbflux_i18n::t!(
                    "document.secret_store.edit.refused_managed",
                    name = summary.name.as_str(),
                    service = service
                ));
            }

            if summary.deletion_scheduled_for.is_some() {
                return Some(dbflux_i18n::t!(
                    "document.secret_store.edit.refused_deletion",
                    name = summary.name.as_str()
                ));
            }
        }

        None
    }

    /// Blocks a transition that would drop the open editor while it holds
    /// unsaved text — another selection, a refresh, a navigation, a search —
    /// and says why. The editor keeps its text; Save and Discard are the
    /// ways out. Returns `true` when the transition was blocked.
    pub(crate) fn block_unsaved_value_edits(&self, cx: &mut Context<Self>) -> bool {
        if !self.has_unsaved_value_edits(cx) {
            return false;
        }

        let name = self
            .selected_summary()
            .map(|summary| summary.name.clone())
            .unwrap_or_default();

        report_error(
            UserFacingError::new(
                ErrorKind::User,
                dbflux_i18n::t!(
                    "document.secret_store.edit.unsaved_block",
                    name = name.as_str()
                ),
            )
            .with_severity(EventSeverity::Warn),
            cx,
        );
        true
    }

    /// Saves the edited text as a new version through the governed seam. A
    /// save whose text is unchanged is a no-op, the secret's latest details
    /// and the session the value came from are re-checked, and a value the
    /// store kept as JSON must still parse as JSON — as any JSON, in the
    /// layout the user typed; the gate checks the type, not the shape.
    pub fn save_value_edit(&mut self, cx: &mut Context<Self>) {
        let Some(edit) = &self.value_edit else {
            return;
        };

        if edit.saving {
            return;
        }

        let text = edit.editor.read(cx).value().to_string();

        if !edit.is_changed(cx) {
            return;
        }

        if let Some(reason) = self.value_edit_refusal_reason() {
            report_error(
                UserFacingError::new(ErrorKind::User, reason).with_severity(EventSeverity::Warn),
                cx,
            );
            return;
        }

        let connected = self.app_state.read(cx).connections().get(&self.profile_id);

        let Some(connected) = connected else {
            self.missing_connection_reported = true;
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.secret_store.error.connection_unavailable"),
                ),
                cx,
            );
            return;
        };

        // The save lands on the session the value was read from. A profile
        // whose connection was replaced — a region switch, a reconnect, a
        // credentials refresh — refuses here: the editor's value may not
        // exist on the new session, and writing there would put a value
        // somewhere the user did not see.
        if !Arc::ptr_eq(&connected.connection, &edit.connection) {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.secret_store.edit.connection_replaced"),
                )
                .with_severity(EventSeverity::Warn),
                cx,
            );
            return;
        }

        let connection = connected.connection.clone();
        let policy = connected.mutation_policy;
        let read_only_reason = connected.read_only_reason;
        let driver_id = connected.connection.metadata().id.clone();

        if let Some(error) = edit
            .stored_is_json
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
            return;
        }

        let Some(name) = self.selected_summary().map(|summary| summary.name.clone()) else {
            return;
        };

        let audit = self.app_state.read(cx).audit_service().clone();
        let store_id = self.store.id.clone();
        let profile_id = self.profile_id;
        let generation = self.value_write_generation;
        let entity = cx.entity().clone();
        let request = SecretPutRequest::text(name.clone(), text);

        if let Some(edit) = &mut self.value_edit {
            edit.saving = true;
        }
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
                None => Err(SecretWriteFailure {
                    error: DbError::NotSupported(dbflux_i18n::t!(
                        "document.secret_store.error.store_unavailable",
                        store = store_id
                    )),
                    correlation_id: None,
                    // The connection has no such store — a lost connection,
                    // not a gate's decision.
                    kind: SecretWriteFailureKind::StoreFailed,
                }),
            }
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_value_write(generation, &name, result, cx);
                });
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
        cx: &mut Context<Self>,
    ) {
        let is_current = generation == self.value_write_generation
            && self.selected == Some(SecretRowKey::Secret(name.to_string()));

        match result {
            Ok(written) => {
                if is_current {
                    self.value = ValueState::Loaded(Box::new(super::RetrievedValue::new(&written)));
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
                    let stored_is_json =
                        serde_json::from_str::<serde_json::Value>(&saved_text).is_ok();
                    let keep_editing = self.value_edit.as_ref().is_some_and(|edit| {
                        edit.editor.read(cx).value().as_str() != saved_text.as_str()
                    });

                    if keep_editing {
                        if let Some(edit) = &mut self.value_edit {
                            edit.original = SecretString::from(saved_text);
                            edit.stored_is_json = stored_is_json;
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
                if self.selected == Some(SecretRowKey::Secret(name.to_string())) {
                    self.load_versions(name.to_string(), cx);
                }
            }
            Err(failure) => {
                self.report_seam_write_failure(write_failure_to_user_facing(&failure), cx);

                if is_current {
                    if let Some(edit) = &mut self.value_edit {
                        edit.saving = false;
                    }
                    cx.notify();
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
    fn report_seam_write_failure(&self, failure: UserFacingError, cx: &mut Context<Self>) {
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
fn write_failure_to_user_facing(failure: &SecretWriteFailure) -> UserFacingError {
    let mut user_error = match failure.kind {
        SecretWriteFailureKind::Refused => {
            UserFacingError::new(ErrorKind::User, failure.error.to_string())
                .with_severity(EventSeverity::Warn)
        }
        SecretWriteFailureKind::StoreFailed => db_error_to_user_facing(&failure.error),
        SecretWriteFailureKind::AuditLost => {
            UserFacingError::new(ErrorKind::Storage, failure.error.to_string())
                .with_severity(EventSeverity::Warn)
        }
    };

    if let Some(correlation_id) = failure.correlation_id {
        user_error.correlation_id = correlation_id;
    }

    user_error
}

/// The audit-loss failure the seam builds for a landed write whose audit
/// event the sink refused says the write stands, and the editor reports it
/// as an audit-storage problem at warning severity — never as a store
/// failure a user might answer by retrying.
#[test]
fn an_audit_loss_after_a_landed_write_says_the_write_landed() {
    let failure = SecretWriteFailure {
        error: DbError::query_failed(
            "The write to \"prod/notify/webhook-token\" landed in Secrets Manager, but its \
             audit event could not be recorded (sink closed). The value cannot be un-written; \
             do not retry the write to repair the audit trail.",
        ),
        correlation_id: None,
        kind: SecretWriteFailureKind::AuditLost,
    };

    let user_error = write_failure_to_user_facing(&failure);
    assert!(
        user_error.summary.contains("landed"),
        "{}",
        user_error.summary
    );
    assert!(!user_error.summary.contains("rotated webhook token 77b1"));
    assert_eq!(user_error.kind, ErrorKind::Storage);
    assert_eq!(user_error.severity, EventSeverity::Warn);
}

/// The correlation id a store failure carries replaces the fresh one the
/// user-facing error mints, so the toast points at the audit row.
#[test]
fn a_reported_write_failure_keeps_the_correlation_id_its_event_recorded() {
    let id = Uuid::now_v7();
    let failure = SecretWriteFailure {
        error: DbError::permission_denied("put was denied"),
        correlation_id: Some(id),
        kind: SecretWriteFailureKind::StoreFailed,
    };

    assert_eq!(write_failure_to_user_facing(&failure).correlation_id, id);
}

/// A refusal — here the store's own write capability, the same verdict the
/// policy refusals carry — reads as a decision: the `User` kind at warning
/// severity.
#[test]
fn a_refused_write_reports_as_a_decision() {
    let failure = SecretWriteFailure {
        error: DbError::NotSupported(
            "Secret store \"Secrets Manager\" (secrets-manager) does not support writing \
             values; refused the write to \"prod/notify/webhook-token\"."
                .to_string(),
        ),
        correlation_id: None,
        kind: SecretWriteFailureKind::Refused,
    };

    let user_error = write_failure_to_user_facing(&failure);
    assert_eq!(user_error.kind, ErrorKind::User);
    assert_eq!(user_error.severity, EventSeverity::Warn);
    assert!(user_error.summary.contains("does not support writing"));
}

/// A store failure reports through the driver-facing path even when its
/// error variant is one a refusal also uses: "the connection has no store
/// X" — reachable by disconnecting while the editor is open — is a lost
/// connection, not a gate's decision. This probe fails again if the
/// mapping ever keys on the variant instead of the seam's verdict.
#[test]
fn a_store_failure_reports_as_a_driver_error_even_when_its_variant_looks_like_a_refusal() {
    let failure = SecretWriteFailure {
        error: DbError::NotSupported("The connection has no store secrets-manager".to_string()),
        correlation_id: None,
        kind: SecretWriteFailureKind::StoreFailed,
    };

    let user_error = write_failure_to_user_facing(&failure);
    assert_eq!(user_error.kind, ErrorKind::Driver);
    assert_eq!(user_error.severity, EventSeverity::Error);
}
