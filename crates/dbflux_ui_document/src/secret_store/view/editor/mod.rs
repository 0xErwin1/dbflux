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
//! saved only as JSON — whatever JSON the user typed, of any shape or type,
//! from a compact object to a bare number — in both modes. A stored value
//! that is not JSON has no gate at all.
//!
//! A value stored as JSON opens in one of two modes, and Save writes the text
//! of the mode being edited:
//!
//! - **Formatted** (the default) shows the value indented for editing. When
//!   the stored text was a single line, Save puts it back on one line by
//!   removing the whitespace between tokens ([`json_layout::minify`]); when
//!   the stored text already spanned lines, Save writes the text as edited.
//!   Strings and numbers are never re-parsed, so only whitespace between
//!   tokens can differ from what the user sees.
//! - **Raw** shows the stored text verbatim and Save writes it verbatim.
//!
//! What Save would write, compared with the stored text, decides whether
//! anything changed: an untouched value, or one whose only change is the
//! indentation Formatted added, saves as a no-op in either mode. Switching
//! modes re-lays the current text out only when it parses; text that does not
//! parse stays as typed.

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
use gpui::{App, AppContext as _, Context, Entity, Subscription, Window};
use gpui_component::input::TextareaState;

pub(crate) mod json_layout;
mod line_diff;
mod view;

use super::{DetailsState, SecretView, ValueState};
use crate::object_text::db_error_to_user_facing;
use crate::secret_store::write::{
    SecretOperation, SecretWriteFailure, SecretWriteFailureKind, WriteTarget, put_secret_value,
    store_unavailable_failure,
};

/// How the editor lays out a value stored as JSON.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EditMode {
    /// The value indented for editing; Save restores the stored layout.
    Formatted,
    /// The stored text, saved verbatim.
    Raw,
}

/// What Formatted shows for a stored text: a single-line JSON value
/// indented, anything else in its own layout.
fn formatted_layout(stored: &str, stored_is_json: bool) -> String {
    if stored_is_json && !stored.contains('\n') {
        json_layout::pretty(stored)
    } else {
        stored.to_string()
    }
}

fn parses_as_json(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text).is_ok()
}

/// The height a gutter row takes before the textarea has laid out.
pub(in crate::secret_store) fn fallback_line_height(cx: &App) -> gpui::Pixels {
    (dbflux_components::fonts::ui_px(
        cx,
        dbflux_components::tokens::DocumentMetrics::DETAIL_BLOCK_FONT,
    ) * dbflux_components::tokens::DocumentMetrics::DETAIL_BLOCK_LINE_HEIGHT)
        .round()
}

/// The open editor of the loaded plaintext value.
pub(crate) struct ValueEditState {
    /// The stored text the editor opened with: the unchanged check and the
    /// JSON gate compare against it.
    pub(in crate::secret_store) original: SecretString,

    /// Whether the stored text parses as JSON. A JSON value must be saved as
    /// JSON: a Save whose text no longer parses is refused before the seam.
    pub(in crate::secret_store) stored_is_json: bool,

    /// The multi-line editor surface, built when the edit started.
    pub(in crate::secret_store) editor: Entity<TextareaState>,

    /// True while a governed write is in flight, so a second Save waits and
    /// Discard does not drop what the landing still has to read.
    pub(in crate::secret_store) saving: bool,

    /// The layout the editor text is in. A value that is not JSON is always
    /// [`EditMode::Raw`].
    pub(in crate::secret_store) mode: EditMode,

    /// Redraws the pane when the editor's text or scroll position changes,
    /// so the gutter and the Save state follow it.
    _observer: Subscription,

    /// Whether the editor holds the keyboard, fed by its focus handle's
    /// focus-in and focus-out events. Save's chord resolves in the
    /// text-input context only while this is true.
    pub(in crate::secret_store) focused: bool,

    /// Wraps the textarea and its Find panel, so focus inside either one is
    /// focus inside the editor.
    pub(in crate::secret_store) focus: gpui::FocusHandle,

    /// Where the row area holding the gutter, the tint and the text starts,
    /// in window coordinates, as the last frame painted it. The text sits
    /// lower than that when the Find panel pushes it down, and the gutter
    /// follows by the difference.
    pub(in crate::secret_store) rows_origin: std::rc::Rc<std::cell::Cell<gpui::Pixels>>,
    _focus_subscriptions: [Subscription; 2],

    /// The connection session the value was read from. A save names this
    /// session, so a connection replaced under the open editor refuses the
    /// write instead of landing it on a connection the value never came
    /// from.
    pub(in crate::secret_store) connection: Arc<dyn Connection>,
}

impl ValueEditState {
    /// Whether Save would write something other than the stored text.
    pub(in crate::secret_store) fn is_changed(&self, cx: &App) -> bool {
        self.text_to_write(self.editor.read(cx).value().as_str()) != self.original.expose_secret()
    }

    /// Whether the stored text spans more than one line. Valid JSON holds no
    /// raw line break inside a string, so any line break is layout.
    fn stored_is_multiline(&self) -> bool {
        self.original.expose_secret().contains('\n')
    }

    /// What Formatted shows for the stored text: a single-line value
    /// indented, a multi-line one in its own layout.
    pub(in crate::secret_store) fn formatted_baseline(&self) -> String {
        formatted_layout(self.original.expose_secret(), self.stored_is_json)
    }

    /// The bytes Save writes for `text` in the current mode.
    pub(in crate::secret_store) fn text_to_write(&self, text: &str) -> String {
        if self.mode == EditMode::Raw {
            return text.to_string();
        }

        if text == self.formatted_baseline() {
            return self.original.expose_secret().to_string();
        }

        if self.stored_is_multiline() || !parses_as_json(text) {
            text.to_string()
        } else {
            json_layout::minify(text)
        }
    }

    /// The text `text` becomes when the editor moves to `mode`. Text that
    /// does not parse stays as typed.
    fn switched_text(&self, text: &str, mode: EditMode) -> String {
        if !parses_as_json(text) {
            return text.to_string();
        }

        match mode {
            EditMode::Raw => self.text_to_write(text),
            EditMode::Formatted => {
                if text == self.original.expose_secret() {
                    self.formatted_baseline()
                } else if self.stored_is_multiline() {
                    text.to_string()
                } else {
                    json_layout::pretty(text)
                }
            }
        }
    }
}

/// What a save takes from the live connection, captured on the foreground so
/// the write can run on the background executor.
struct ValueWriteSession {
    connection: Arc<dyn Connection>,
    policy: MutationPolicy,
    read_only_reason: Option<ReadOnlyReason>,
    driver_id: String,
}

impl SecretView {
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

        if value.content.is_binary() {
            return false;
        }

        let Some(summary) = self.selected_summary() else {
            return false;
        };

        summary.owning_service.is_none()
            && summary.deletion_scheduled_for.is_none()
            && self.store.supports(SecretStoreCapabilities::WRITE)
    }

    /// The mode of the open editor; `None` when no editor is open or the
    /// stored value is not JSON, which has no modes.
    pub(crate) fn edit_mode(&self) -> Option<EditMode> {
        self.value_edit
            .as_ref()
            .filter(|edit| edit.stored_is_json)
            .map(|edit| edit.mode)
    }

    /// Whether the open editor offers the Formatted / Raw toggle.
    pub fn can_toggle_edit_mode(&self) -> bool {
        self.edit_mode().is_some() && !self.is_saving_value_edit()
    }

    /// Moves the open editor to `mode`, re-laying the current text out when
    /// it parses.
    pub(crate) fn set_edit_mode(
        &mut self,
        mode: EditMode,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.can_toggle_edit_mode() {
            return;
        }

        let Some(edit) = &mut self.value_edit else {
            return;
        };

        if edit.mode == mode {
            return;
        }

        let text = edit.editor.read(cx).value().to_string();
        let switched = edit.switched_text(&text, mode);
        edit.mode = mode;
        edit.editor
            .update(cx, |state, cx| state.set_value(&switched, window, cx));
        cx.notify();
    }

    /// Opens the editor component's find panel over the text. The panel is
    /// owned by `gpui-component` and reachable only through its `Search`
    /// action, so the button focuses the editor first and then dispatches the
    /// action its `ctrl-f` / `cmd-f` binding raises.
    pub fn find_in_value(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(edit) = &self.value_edit else {
            return;
        };

        edit.editor.update(cx, |state, cx| state.focus(window, cx));
        window.dispatch_action(Box::new(gpui_component::input::Search), cx);
        cx.notify();
    }

    /// Switches to the other mode, for the pane action.
    pub fn cycle_edit_mode(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let next = match self.edit_mode() {
            Some(EditMode::Formatted) => EditMode::Raw,
            Some(EditMode::Raw) => EditMode::Formatted,
            None => return,
        };

        self.set_edit_mode(next, window, cx);
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
        if !self.can_edit_value() || self.region_switch_blocks_writes(cx) {
            return;
        }

        if self.write_in_flight {
            self.report_write_warning(
                dbflux_i18n::t!("document.secret_store.write.in_progress"),
                cx,
            );
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
        let stored_is_json = parses_as_json(&stored);
        let mode = if stored_is_json {
            EditMode::Formatted
        } else {
            EditMode::Raw
        };
        let opening = formatted_layout(&stored, stored_is_json);

        let editor = cx.new(|cx| {
            let mut state = TextareaState::new(window, cx)
                .auto_grow(2, super::details::VIEWER_VISIBLE_LINES as usize)
                .soft_wrap(false)
                .searchable(true);
            state.set_value(&opening, window, cx);
            state
        });
        editor.update(cx, |state, cx| state.focus(window, cx));
        let observer = cx.observe(&editor, |_, _, cx| cx.notify());
        // Whether the editor holds the keyboard follows its focus handle,
        // so a click or a focus move that edits nothing still changes the
        // context the document reports.
        let handle = cx.focus_handle();
        let focus_in = cx.on_focus_in(&handle, window, |this, _, cx| {
            if let Some(edit) = &mut this.value_edit {
                edit.focused = true;
            }
            cx.notify();
        });
        let focus_out = cx.on_focus_out(&handle, window, |this, _, _, cx| {
            if let Some(edit) = &mut this.value_edit {
                edit.focused = false;
            }
            cx.notify();
        });

        self.value_edit = Some(ValueEditState {
            original: SecretString::from(stored),
            stored_is_json,
            editor,
            saving: false,
            mode,
            focused: true,
            focus: handle,
            rows_origin: Default::default(),
            _observer: observer,
            _focus_subscriptions: [focus_in, focus_out],
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

        if self.save_waits_for_another_write(cx) {
            return;
        }

        let text = edit.text_to_write(edit.editor.read(cx).value().as_str());

        if !edit.is_changed(cx) {
            return;
        }

        let stored_is_json = edit.stored_is_json;
        let value_connection = edit.connection.clone();

        if let Some(reason) = self.value_edit_refusal_reason() {
            report_error(
                UserFacingError::new(ErrorKind::User, reason).with_severity(EventSeverity::Warn),
                cx,
            );
            return;
        }

        let Some(session) = self.value_write_session(&value_connection, cx) else {
            return;
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
            return;
        }

        let Some(name) = self.selected_summary().map(|summary| summary.name.clone()) else {
            return;
        };

        self.send_value_write(session, name, text, cx);
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

        if let Some(edit) = &mut self.value_edit {
            edit.saving = true;
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
                entity.update(cx, |view, cx| {
                    view.editor_save_in_flight = false;
                    view.apply_value_write(generation, &name, result, cx);
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
        let is_current = generation == self.value_write_generation && self.is_for(name);

        match result {
            Ok(written) => {
                if is_current {
                    self.value = ValueState::Loaded(Box::new(
                        crate::secret_store::RetrievedValue::new(&written),
                    ));
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

#[cfg(test)]
mod tests;
