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
//! drops the editor, masks everything again and refreshes the details, so
//! the version the pane reports is the version the store now holds.
//!
//! Nothing of the value reaches a log, an error, an audit field or an
//! element id: the editor element is marked redacted, so rendered-frame
//! observers withhold its text and its accesskit node carries no value, and
//! the failures it reports are the seam's own scrubbed messages. A save
//! whose text is unchanged writes nothing.
//!
//! The JSON gate is typed, not shaped: a value the store kept as JSON is
//! saved only as JSON — whatever JSON the user typed, in the layout they
//! typed it in, of any shape or type, from a compact object to a bare
//! number. It does not promise to preserve the stored layout or the
//! value's shape beyond what the user typed, and a stored value that is not
//! JSON has no gate at all.

use dbflux_core::observability::EventSeverity;
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    DbError, MutationPolicy, SecretPutRequest, SecretStoreCapabilities, SecretValue,
};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::{AppContext as _, prelude::*};
use gpui::{Context, Entity};
use gpui_component::input::TextareaState;
use uuid::Uuid;

use super::write::{SecretWriteFailure, put_secret_value};
use super::{SecretRowKey, SecretStoreDocument, ValueState};
use crate::object_text::db_error_to_user_facing;

/// The open editor of the loaded plaintext value.
#[derive(Debug)]
pub(crate) struct ValueEditState {
    /// The stored text the editor opened with: the unchanged check and the
    /// JSON gate compare against it.
    pub(super) original: SecretString,

    /// Whether the stored text parses as JSON. A JSON value must be saved as
    /// JSON: a Save whose text no longer parses is refused before the seam.
    pub(super) stored_is_json: bool,

    /// The multi-line editor surface, built when the edit started.
    pub(super) editor: Entity<TextareaState>,

    /// True while a governed write is in flight, so a second Save waits.
    pub(super) saving: bool,
}

impl ValueEditState {
    /// Whether the editor's text differs from the stored text it opened with.
    pub(super) fn is_changed(&self, cx: &gpui::App) -> bool {
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

    /// Opens the editor on the loaded value with the stored text. A value
    /// that may not be edited ignores the request.
    pub fn start_value_edit(&mut self, window: &mut gpui::Window, cx: &mut Context<Self>) {
        if !self.can_edit_value() {
            return;
        }

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
    pub fn discard_value_edit(&mut self, cx: &mut Context<Self>) {
        if self.value_edit.take().is_some() {
            cx.notify();
        }
    }

    /// Saves the edited text as a new version through the governed seam. A
    /// save whose text is unchanged is a no-op, and a value the store kept
    /// as JSON must still parse as JSON — as any JSON, in the layout the
    /// user typed; the gate checks the type, not the shape.
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

        let connected = self.app_state.read(cx).connections().get(&self.profile_id);

        let (connection, policy, read_only_reason, driver_id) = match connected {
            Some(connected) => (
                Some(connected.connection.clone()),
                connected.mutation_policy,
                connected.read_only_reason,
                connected.connection.metadata().id.clone(),
            ),
            None => (None, MutationPolicy::Allowed, None, String::new()),
        };

        let Some(connection) = connection else {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.secret_store.error.connection_unavailable"),
                ),
                cx,
            );
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
                    refused: false,
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
    /// the seam returned — its new version id — closes the editor, masks
    /// everything again and refreshes the details; a refusal or a failure
    /// keeps the editor open so the text can be fixed or dropped. The audit
    /// event of every outcome was recorded by the seam itself.
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
                    self.value_edit = None;
                    self.value = ValueState::Loaded(Box::new(super::RetrievedValue::new(&written)));
                    cx.notify();
                    // The details refresh, so the version the pane reports
                    // is the version the store now holds.
                    self.describe(name.to_string(), cx);
                }
            }
            Err(failure) => {
                report_error(write_failure_to_user_facing(&failure), cx);

                if is_current {
                    if let Some(edit) = &mut self.value_edit {
                        edit.saving = false;
                    }
                    cx.notify();
                }
            }
        }
    }
}

/// How a failed governed write reaches the user. The seam's own verdict
/// decides, never the error variant: a refusal — the policy's, or the
/// store's own write capability — is a decision, not a transient error, so
/// it reads as one, a warning of the `User` kind; a store failure keeps the
/// driver-facing error the retrieval path builds, even when its variant is
/// one a refusal also uses. The correlation id the failure carries replaces
/// the fresh one, so the toast and the failure's audit row tell one story.
fn write_failure_to_user_facing(failure: &SecretWriteFailure) -> UserFacingError {
    let mut user_error = if failure.refused {
        UserFacingError::new(ErrorKind::User, failure.error.to_string())
            .with_severity(EventSeverity::Warn)
    } else {
        db_error_to_user_facing(&failure.error)
    };

    if let Some(correlation_id) = failure.correlation_id {
        user_error.correlation_id = correlation_id;
    }

    user_error
}

/// The audit-loss failure the seam builds for a landed write whose audit
/// event the sink refused says the write stands, and the editor reports it
/// verbatim through the store-facing path: no retry can repair the audit.
#[test]
fn an_audit_loss_after_a_landed_write_says_the_write_landed() {
    let failure = SecretWriteFailure {
        error: DbError::query_failed(
            "The write to \"prod/notify/webhook-token\" landed in Secrets Manager, but its \
             audit event could not be recorded (sink closed). The value cannot be un-written; \
             do not retry the write to repair the audit trail.",
        ),
        correlation_id: None,
        refused: false,
    };

    let user_error = write_failure_to_user_facing(&failure);
    assert!(
        user_error.summary.contains("landed"),
        "{}",
        user_error.summary
    );
    assert!(!user_error.summary.contains("rotated webhook token 77b1"));
    assert_eq!(user_error.kind, ErrorKind::Driver);
    assert_eq!(user_error.severity, EventSeverity::Error);
}

/// The correlation id a store failure carries replaces the fresh one the
/// user-facing error mints, so the toast points at the audit row.
#[test]
fn a_reported_write_failure_keeps_the_correlation_id_its_event_recorded() {
    let id = Uuid::now_v7();
    let failure = SecretWriteFailure {
        error: DbError::permission_denied("put was denied"),
        correlation_id: Some(id),
        refused: false,
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
        refused: true,
    };

    let user_error = write_failure_to_user_facing(&failure);
    assert_eq!(user_error.kind, ErrorKind::User);
    assert_eq!(user_error.severity, EventSeverity::Warn);
    assert!(user_error.summary.contains("does not support writing"));
}

/// A store failure reports through the driver-facing path even when its
/// error variant is one a refusal also uses: \"the connection has no store
/// X\" — reachable by disconnecting while the editor is open — is a lost
/// connection, not a gate's decision. This probe fails again if the
/// mapping ever keys on the variant instead of the seam's verdict.
#[test]
fn a_store_failure_reports_as_a_driver_error_even_when_its_variant_looks_like_a_refusal() {
    let failure = SecretWriteFailure {
        error: DbError::NotSupported("The connection has no store secrets-manager".to_string()),
        correlation_id: None,
        refused: false,
    };

    let user_error = write_failure_to_user_facing(&failure);
    assert_eq!(user_error.kind, ErrorKind::Driver);
    assert_eq!(user_error.severity, EventSeverity::Error);
}
