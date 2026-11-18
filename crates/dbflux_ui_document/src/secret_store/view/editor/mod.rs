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

use dbflux_components::vim::VimBinding;
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
use gpui_component::input::EditorState;

pub(crate) mod json_layout;
mod line_diff;
mod save;
mod view;

use super::{DetailsState, SecretView, SecretViewEvent, ValueState};
use crate::object_text::db_error_to_user_facing;
use crate::secret_store::write::{
    SecretOperation, SecretWriteFailure, SecretWriteFailureKind, WriteTarget, put_secret_value,
    store_unavailable_failure,
};
pub(crate) use save::land_save_answer;
pub(in crate::secret_store) use save::write_failure_to_user_facing;

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

pub(super) fn parses_as_json(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text).is_ok()
}

/// The open editor of the loaded plaintext value.
pub(crate) struct ValueEditState {
    /// The stored text the editor opened with: the unchanged check and the
    /// JSON gate compare against it.
    pub(in crate::secret_store) original: SecretString,

    /// Whether the stored text parses as JSON. A JSON value must be saved as
    /// JSON: a Save whose text no longer parses is refused before the seam.
    pub(in crate::secret_store) stored_is_json: bool,

    /// The editor surface, built when the edit started: the component the
    /// code documents and the object editor use, with its own line numbers,
    /// scrolling and find panel.
    pub(in crate::secret_store) editor: Entity<EditorState>,

    /// Vim mode for this editor, off unless the setting turns it on.
    pub(in crate::secret_store) vim: VimBinding,

    /// True while a governed write is in flight, so a second Save waits and
    /// Discard does not drop what the landing still has to read.
    pub(in crate::secret_store) saving: bool,

    /// The secret changed in another view while this editor held unsaved
    /// text. The text is kept and Save still writes a new version; the
    /// editor says so until it is saved.
    pub(in crate::secret_store) changed_elsewhere: bool,

    /// The secret was deleted while this editor held unsaved text. The text
    /// stays, read-only, until it is discarded; nothing can be saved.
    pub(in crate::secret_store) removed: bool,

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

    /// Wraps the editor and its Find panel, so focus inside either one is
    /// focus inside the editor.
    pub(in crate::secret_store) focus: gpui::FocusHandle,
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

impl SecretView {
    /// Whether the details pane offers an editor for the loaded value: a
    /// plaintext value of a secret nobody else manages and whose deletion is
    /// not pending, in a store that advertises `WRITE`. A connection whose
    /// policy refuses writes keeps the editor — the refusal happens at Save,
    /// through the seam, where the decision is audited.
    pub(crate) fn can_edit_value(&self) -> bool {
        if self.value_edit.is_some() || !self.connection_present {
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
            && !self.is_deletion_pending(summary)
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

    /// Whether the open draft is read-only: its secret is deleted, or its
    /// deletion is scheduled. A restore lifts it with the draft intact.
    pub(crate) fn draft_is_locked(&self) -> bool {
        self.value_edit
            .as_ref()
            .is_some_and(|edit| edit.removed || self.deletion_pending())
    }

    /// Whether the open editor offers the Formatted / Raw toggle.
    pub(crate) fn can_toggle_edit_mode(&self) -> bool {
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
    pub(crate) fn find_in_value(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(edit) = &self.value_edit else {
            return;
        };

        crate::object_text::open_find_panel(&edit.editor, window, cx);
        cx.notify();
    }

    /// Gives the keyboard to the open editor, keeping its text.
    pub(crate) fn focus_value_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(edit) = &self.value_edit {
            edit.editor.update(cx, |state, cx| state.focus(window, cx));
        }
    }

    /// Replaces the editor's text, as a long paste would.
    #[cfg(test)]
    pub(crate) fn replace_editor_text(
        &mut self,
        text: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(edit) = &self.value_edit {
            edit.editor
                .update(cx, |state, cx| state.set_value(text, window, cx));
        }
    }

    /// The height of the text area the editor laid out, for the tab tests.
    #[cfg(test)]
    pub(crate) fn editor_viewport_height(&self, cx: &App) -> Option<gpui::Pixels> {
        self.value_edit
            .as_ref()
            .map(|edit| edit.editor.read(cx).input_bounds().size.height)
    }

    /// The text the open editor holds, for the tests.
    #[cfg(test)]
    pub(crate) fn editor_text_for_test(&self, cx: &App) -> Option<String> {
        self.value_edit
            .as_ref()
            .map(|edit| edit.editor.read(cx).value().to_string())
    }

    /// The caret's 1-based line, for the tab tests.
    #[cfg(test)]
    pub(crate) fn editor_caret_line(&self, cx: &App) -> Option<u32> {
        self.value_edit
            .as_ref()
            .map(|edit| edit.editor.read(cx).cursor_position().line + 1)
    }

    /// Switches to the other mode, for the pane action.
    pub(crate) fn cycle_edit_mode(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let next = match self.edit_mode() {
            Some(EditMode::Formatted) => EditMode::Raw,
            Some(EditMode::Raw) => EditMode::Formatted,
            None => return,
        };

        self.set_edit_mode(next, window, cx);
    }

    /// Whether the value editor is open.
    pub(crate) fn is_editing_value(&self) -> bool {
        self.value_edit.is_some()
    }

    /// Whether a governed write of this editor is in flight.
    pub(crate) fn is_saving_value_edit(&self) -> bool {
        self.value_edit.as_ref().is_some_and(|edit| edit.saving)
    }

    /// Whether the open editor holds text that no save has written: the
    /// dirty dot, the unsaved-changes prompt on close and the guards on the
    /// transitions that would drop the editor all read this.
    pub(crate) fn has_unsaved_value_edits(&self, cx: &App) -> bool {
        self.value_edit
            .as_ref()
            .is_some_and(|edit| edit.is_changed(cx))
    }

    /// The description the tab's dirty dot and the unsaved-changes prompt
    /// show while the editor holds unsaved text; `None` when there is
    /// nothing unsaved.
    pub(crate) fn unsaved_change_summary(&self, cx: &App) -> Option<String> {
        self.has_unsaved_value_edits(cx)
            .then(|| dbflux_i18n::t!("document.secret_store.edit.change_summary").to_string())
    }

    /// The session the editor binds to, when an edit may start: the value
    /// is editable, nothing else is writing, and the session the value was
    /// read from is the live one. A value of a replaced session is dropped
    /// instead of edited.
    fn session_for_edit(&mut self, cx: &mut Context<Self>) -> Option<Arc<dyn Connection>> {
        if !self.can_edit_value() || self.region_switch_blocks_writes(cx) {
            return None;
        }

        if self.write_in_flight {
            self.report_write_warning(
                dbflux_i18n::t!("document.secret_store.write.in_progress"),
                cx,
            );
            return None;
        }

        if self.region_mismatch(cx) {
            return None;
        }

        let Some(live) = self.connection(cx) else {
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

        // The editor is bound to the session the value was read from, not to
        // whichever one is live now: a value of a replaced session is dropped
        // instead of edited.
        let connection = self.value_session.clone()?;

        if !Arc::ptr_eq(&connection, &live) {
            self.clear_value();
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.secret_store.edit.connection_replaced"),
                )
                .with_severity(EventSeverity::Warn),
                cx,
            );
            cx.notify();
            return None;
        }

        Some(connection)
    }

    /// Opens the editor on the loaded value with the stored text. A value
    /// that may not be edited ignores the request; a connection that is gone
    /// is reported, as a save without one is.
    pub(crate) fn start_value_edit(&mut self, window: &mut gpui::Window, cx: &mut Context<Self>) {
        let Some(connection) = self.session_for_edit(cx) else {
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

        let editor = build_editor(&opening, stored_is_json, window, cx);
        let vim = VimBinding::new(editor.clone(), window, cx);
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
            editor: editor.clone(),
            vim,
            saving: false,
            changed_elsewhere: false,
            removed: false,
            mode,
            focused: true,
            focus: handle,
            _observer: observer,
            _focus_subscriptions: [focus_in, focus_out],
            connection,
        });
        // The single-line fields the value drew are not drawn in edit mode;
        // they are rebuilt from scratch when the editor closes.
        self.value_inputs.clear();
        self.value_input_masks.clear();
        self.value_inputs_view = None;
        VimBinding::follow_setting(self, editor.entity_id(), cx);
        cx.notify();
    }

    /// Follows a write another view made to the shown secret: what this view
    /// retrieved is stale, so it goes back to "Value not loaded" and reads
    /// again on demand, through the audited path. An editor holding unsaved
    /// text keeps it and says the secret changed; a clean one closes.
    /// Follows a write of another view by its kind. A scheduled deletion is
    /// taken as confirmed first, so nothing below asks the store for what it
    /// refuses on a secret pending deletion; a draft stays, read-only, until
    /// the secret is restored or the draft is discarded.
    pub(crate) fn follow_external_write(
        &mut self,
        name: &str,
        kind: super::super::changes::SecretChangeKind,
        cx: &mut Context<Self>,
    ) {
        use super::super::changes::SecretChangeKind;

        match kind {
            SecretChangeKind::Scheduled { date } => self.confirm_deletion_scheduled(name, date, cx),
            SecretChangeKind::Restored => self.confirm_restored(name, cx),
            _ => {}
        }

        self.follow_external_change(cx);
        self.describe(name.to_string(), cx);
        self.load_versions(name.to_string(), cx);
    }

    pub(crate) fn follow_external_change(&mut self, cx: &mut Context<Self>) {
        if self.has_unsaved_value_edits(cx) {
            if let Some(edit) = &mut self.value_edit {
                edit.changed_elsewhere = true;
            }

            // The text stays, but the value it was opened from is stale:
            // Discard returns to "Value not loaded" and Copy value waits for
            // a new audited retrieval.
            self.value_generation = self.value_generation.wrapping_add(1);
            self.value = ValueState::NotLoaded;
            self.value_version = None;
            self.value_inputs.clear();
            self.value_input_masks.clear();
            self.value_inputs_view = None;
            cx.notify();
            return;
        }

        if matches!(self.value, ValueState::NotLoaded) && self.value_edit.is_none() {
            return;
        }

        self.clear_value();
        cx.notify();
    }

    /// Closes the editor without writing. The value the pane holds is the
    /// one the store still has.
    ///
    /// A stale action must not drop an editor it was never aimed at: while a
    /// save is in flight the landing still has to read what was typed after
    /// it started, so Discard sits out, and the pane action is disabled so
    /// the queue cannot carry one into the landing.
    pub(crate) fn discard_value_edit(&mut self, cx: &mut Context<Self>) {
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

        if self.is_deletion_pending(summary) {
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

            if self.is_deletion_pending(summary) {
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
}

impl dbflux_components::vim::VimHost for SecretView {
    fn vim(&self, input: gpui::EntityId) -> Option<&VimBinding> {
        self.value_edit
            .as_ref()
            .and_then(|edit| edit.vim.for_input(input))
    }

    fn vim_mut(&mut self, input: gpui::EntityId) -> Option<&mut VimBinding> {
        self.value_edit
            .as_mut()
            .and_then(|edit| edit.vim.for_input_mut(input))
    }

    /// A save in flight keeps the text as it was sent.
    fn vim_read_only(&self, _input: gpui::EntityId, _cx: &App) -> bool {
        self.is_saving_value_edit()
    }
}

/// The editor component for the opened text: line numbers, no wrapping, its
/// find panel, and JSON highlighting for a value stored as JSON.
fn build_editor(
    opening: &str,
    stored_is_json: bool,
    window: &mut Window,
    cx: &mut Context<SecretView>,
) -> Entity<EditorState> {
    let editor = cx.new(|cx| {
        let state = EditorState::new(window, cx)
            .line_number(true)
            .soft_wrap(false)
            .searchable(true);

        if stored_is_json {
            state.language("json")
        } else {
            state
        }
    });
    editor.update(cx, |state, cx| {
        state.set_value(opening, window, cx);
        state.focus(window, cx);
    });
    editor
}

#[cfg(test)]
mod tests;
