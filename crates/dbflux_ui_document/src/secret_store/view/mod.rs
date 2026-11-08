//! The view of ONE secret: its details, its value, its version history,
//! the editor of its value and every governed write that acts on it.
//!
//! A host (the listing document today, a document per secret later) owns an
//! `Entity<SecretView>` and tells it which secret to show. The view reads and
//! writes the store itself, drops every response that belongs to another
//! secret than the one it shows, and reports what only the host can act on
//! (reloading its listing, the call timing, a removed secret, a dialog that
//! opened) as [`SecretViewEvent`]s, so no write-safety logic lives in a host.

mod actions;
mod data;
pub(super) mod details;
pub(super) mod editor;
mod inputs;
mod menu;
mod pane_actions;

pub(super) use pane_actions::callback_action;

#[cfg(test)]
pub(super) use actions::{DeleteKind, Landing};
pub(super) use actions::{DialogHost, DialogShell, FooterSpec, dialog_footer, dialog_modal};
pub(super) use actions::{DialogPin, TagRow, WriteDialog, WriteEffect, new_tag_row};

use super::value::{ValueState, ValueView};
use crate::secret_store::CallTiming;
use actions::RotationNotice;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::controls::InputState;
use dbflux_components::modals::ModalFocus;
use dbflux_core::secrecy::ExposeSecret;
use dbflux_core::{
    Connection, SecretDetails, SecretStoreCapabilities, SecretStoreDescriptor, SecretSummary,
    SecretVersionInfo,
};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;
use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use uuid::Uuid;

/// The describe of the selected secret.
#[derive(Clone, Debug, Default)]
pub enum DetailsState {
    #[default]
    Idle,
    Loading,
    Loaded(Box<SecretDetails>),
    Failed(String),
}

/// The version history of the selected secret, loaded apart from its
/// describe so a slow or refused history never hides the details.
#[derive(Clone, Debug, Default)]
pub enum VersionsState {
    #[default]
    Idle,
    Loading,
    Loaded(Vec<SecretVersionInfo>),
    Failed(String),
    /// The store does not advertise `SecretStoreCapabilities::VERSIONS`.
    NotSupported,
    /// The secret is scheduled for deletion: the service refuses its
    /// history, so it is not asked until the secret is restored.
    ScheduledForDeletion,
}

/// What a view tells its host.
pub(super) enum SecretViewEvent {
    /// A write or a follow-up read changed what the listing shows for the
    /// secret: the host reloads its listing in place.
    ReloadListing,

    /// A store call finished, for the host's status bar.
    StoreCalled(CallTiming),

    /// The shown secret no longer exists: the host drops its selection.
    Removed,

    /// A write dialog opened over the host, which closes its own menus.
    DialogOpened,

    /// The pane's Close button was pressed.
    CloseRequested,

    /// The open dialog went away, however it closed: the host drops what it
    /// kept for a dialog of its own.
    DialogClosed,

    /// The create the host started landed (`stands` when it did not report a
    /// clean success but may have written).
    Created { name: String, stands: bool },
}

pub struct SecretView {
    profile_id: Uuid,
    pub(super) store: SecretStoreDescriptor,
    app_state: Entity<AppStateEntity>,

    /// The secret the view shows, `None` while the host selected something
    /// else. A response for any other name is dropped.
    pub(super) name: Option<String>,

    /// The host's listing row of the secret, as of the host's last push.
    pub(super) listing_row: Option<SecretSummary>,

    /// The secret's last known summary, kept for the moment a reload leaves
    /// its row off the pages the host loaded so far.
    pub(super) summary_cache: Option<SecretSummary>,
    pub(super) details: DetailsState,
    pub(super) details_generation: u64,
    pub(super) versions: VersionsState,
    pub(super) versions_generation: u64,
    #[cfg(test)]
    pub(super) value_view_sets: usize,
    /// The connection the history was listed from, so ids of a replaced
    /// session are never retrieved against the new one.
    pub(super) versions_session: Option<Arc<dyn Connection>>,
    /// Some path of this view already reported the missing connection for the
    /// current selection, so the next one to find the same loss stays quiet.
    /// Cleared when a connection is found again or the selection is reset.
    pub(super) missing_connection_reported: bool,
    /// The version whose value the value section shows or is retrieving,
    /// `None` for the current value.
    pub(super) value_version: Option<String>,
    pub(super) value: ValueState,
    pub(super) value_generation: u64,
    /// One read-only, secret-marked input per drawn value field, built on
    /// the render pass after a retrieval (an input needs a `Window`).
    pub(super) value_inputs: Vec<Entity<InputState>>,
    /// The masked flag last applied to each of `value_inputs`.
    value_input_masks: Vec<bool>,
    value_inputs_view: Option<ValueView>,
    /// Scroll position of the multi-line viewer.
    value_viewer_scroll: ScrollHandle,
    /// The open editor of a plaintext value, if any.
    pub(super) value_edit: Option<editor::ValueEditState>,
    /// Bumped when the value or the selection is dropped, so a write that
    /// lands after the user moved on does not touch the pane.
    pub(super) value_write_generation: u64,
    /// The write dialog open over the host, if any.
    pub(super) dialog: Option<WriteDialog>,
    /// Moves the keyboard into the dialog when it opens and hands it back
    /// when it closes.
    dialog_focus: ModalFocus,
    /// The connection session the details were loaded from.
    pub(super) details_session: Option<Arc<dyn Connection>>,
    /// Scroll position of the details pane's sections.
    details_scroll: ScrollHandle,
    /// The session and the secret the open dialog was opened on.
    dialog_pin: Option<DialogPin>,
    /// A governed write is in flight. Another write waits for it, so a
    /// double click cannot send the request twice.
    pub(super) write_in_flight: bool,
    /// An editor save is in flight. Kept apart from the editor, which a
    /// refresh or a reselection clears while the save is still out, so an
    /// action write waits for the answer whatever became of the editor.
    pub(super) editor_save_in_flight: bool,
    /// Holds the generation of the latest rotation follow-up of a secret (keyed
    /// by name; the store is constant within a view); an older schedule of the same secret stops at its next tick,
    /// so overlapping rotations of one secret share the latest one.
    pub(super) rotation_follow_up_generations: HashMap<String, u64>,
    /// Source of the generations above; never reused, so a finished
    /// schedule can tell whether the entry is still its own.
    rotation_follow_up_counter: u64,
    /// A rotation cancelled before it finished, shown in the Rotation
    /// section while its secret stays shown.
    pub(super) rotation_notice: Option<RotationNotice>,
    /// The bottom bar's Edit value was pressed before the value was
    /// retrieved: the editor opens when the retrieval lands.
    edit_after_retrieve: bool,
    /// The keyboard context of the host as of the last frame it drew, which
    /// decides the chord a keycap of the pane shows.
    host_context: ContextId,
    /// Set while the host's region switch reconnects: the session a write
    /// would land on is about to be replaced.
    region_switching: Rc<Cell<bool>>,
}

impl EventEmitter<SecretViewEvent> for SecretView {}

impl SecretView {
    pub(super) fn new(
        profile_id: Uuid,
        store: SecretStoreDescriptor,
        app_state: Entity<AppStateEntity>,
        region_switching: Rc<Cell<bool>>,
        cx: &mut Context<Self>,
    ) -> Self {
        Self {
            profile_id,
            store,
            app_state,
            name: None,
            listing_row: None,
            summary_cache: None,
            details: DetailsState::Idle,
            details_generation: 0,
            versions: VersionsState::Idle,
            versions_generation: 0,
            #[cfg(test)]
            value_view_sets: 0,
            versions_session: None,
            missing_connection_reported: false,
            value_version: None,
            value: ValueState::NotLoaded,
            value_generation: 0,
            value_inputs: Vec::new(),
            value_input_masks: Vec::new(),
            value_inputs_view: None,
            value_viewer_scroll: ScrollHandle::new(),
            value_edit: None,
            value_write_generation: 0,
            dialog: None,
            dialog_focus: ModalFocus::new(cx),
            details_session: None,
            details_scroll: ScrollHandle::new(),
            dialog_pin: None,
            write_in_flight: false,
            editor_save_in_flight: false,
            rotation_follow_up_generations: HashMap::new(),
            rotation_follow_up_counter: 0,
            rotation_notice: None,
            edit_after_retrieve: false,
            host_context: ContextId::Results,
            region_switching,
        }
    }

    /// Runs what the view needs a window for before it draws: the inputs
    /// that show a retrieved value, and the editor a pending Edit value
    /// opens once its retrieval landed.
    pub(super) fn prepare_frame(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        // The editor of an open edit replaces the value's fields, so the
        // read-only inputs of the closed presentation are not rebuilt while
        // it holds a second copy of the value.
        if !self.is_editing_value() {
            self.sync_value_inputs(window, cx);
        }

        // The bottom bar's Edit value retrieved the value first; the editor
        // opens now that it landed.
        self.open_pending_edit(window, cx);
    }

    /// Whether the open editor holds the keyboard.
    pub(super) fn value_editor_focused(&self) -> bool {
        self.value_edit.as_ref().is_some_and(|edit| edit.focused)
    }

    pub fn details(&self) -> &DetailsState {
        &self.details
    }

    pub fn versions(&self) -> &VersionsState {
        &self.versions
    }

    /// The version the value section shows, `None` for the current value.
    pub fn value_version(&self) -> Option<&str> {
        self.value_version.as_deref()
    }

    pub fn value(&self) -> &ValueState {
        &self.value
    }

    /// The name of the secret the view shows.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Whether a response about `name` belongs to the secret the view shows.
    pub(super) fn is_for(&self, name: &str) -> bool {
        self.name.as_deref() == Some(name)
    }

    pub(super) fn hierarchical(&self) -> bool {
        self.store.supports(SecretStoreCapabilities::HIERARCHICAL)
    }

    /// The shown secret's summary: the host's listing row while it holds one,
    /// else the last one the view saw.
    pub fn selected_summary(&self) -> Option<&SecretSummary> {
        let name = self.name.as_deref()?;

        self.listing_row.as_ref().or_else(|| {
            self.summary_cache
                .as_ref()
                .filter(|summary| summary.name == name)
        })
    }

    /// The host's listing row of the shown secret, if the listing holds it.
    pub(super) fn listed_summary(&self) -> Option<&SecretSummary> {
        self.listing_row.as_ref()
    }

    /// Records the host's listing row of the shown secret. `refresh_cache`
    /// is false when the listing that produced it failed, so a failed page
    /// does not replace what a describe learned.
    pub(super) fn set_listing_row(
        &mut self,
        row: Option<SecretSummary>,
        refresh_cache: bool,
        cx: &mut Context<Self>,
    ) {
        if refresh_cache && let Some(summary) = &row {
            self.summary_cache = Some(summary.clone());
        }

        self.listing_row = row;

        // A restore that reached the listing makes the history available.
        if matches!(self.versions, VersionsState::ScheduledForDeletion)
            && let Some(name) = self.name.clone()
            && !self.deletion_pending()
        {
            self.request_versions(name, crate::secret_store::HistoryLoad::Automatic, cx);
        }
    }

    /// Whether Retrieve value may run for the shown secret: one whose
    /// deletion is not pending, with no retrieval in flight.
    pub fn can_retrieve_value(&self) -> bool {
        self.selected_summary()
            .is_some_and(|summary| !summary.is_pending_deletion())
            && !matches!(self.value, ValueState::Loading)
    }

    // -- Selection ---------------------------------------------------------

    /// Shows `name`: drops what the previous selection left (dialog, value,
    /// details, history) and describes the new secret. The caller has
    /// already made sure no unsaved edit is lost.
    pub(super) fn show_secret(
        &mut self,
        name: String,
        listing_row: Option<SecretSummary>,
        cx: &mut Context<Self>,
    ) {
        self.reset_selection(true, cx);
        self.summary_cache = listing_row.clone();
        self.listing_row = listing_row;
        self.name = Some(name.clone());
        self.describe(name.clone(), cx);
        self.load_versions(name, cx);
    }

    /// Shows nothing of a secret: the host selected a row that is not one.
    pub(super) fn show_other_row(&mut self, cx: &mut Context<Self>) {
        self.reset_selection(true, cx);
        self.name = None;
        self.listing_row = None;
    }

    /// Stops showing a secret, the way moving to another one drops it.
    pub(super) fn clear_selection(&mut self, cx: &mut Context<Self>) {
        self.reset_selection(false, cx);
        self.name = None;
        self.listing_row = None;
    }

    fn reset_selection(&mut self, scroll_to_top: bool, cx: &mut Context<Self>) {
        self.close_dialog_for_navigation(cx);

        if scroll_to_top {
            self.details_scroll.set_offset(point(px(0.0), px(0.0)));
        }

        self.clear_value();
        self.details_generation = self.details_generation.wrapping_add(1);
        self.details = DetailsState::Idle;
        self.reset_versions();
    }

    /// Drops the shown secret's details, history and value after a deletion
    /// that removed it, and tells the host.
    pub(in crate::secret_store) fn forget_removed_secret(
        &mut self,
        name: &str,
        cx: &mut Context<Self>,
    ) {
        if !self.is_for(name) {
            return;
        }

        self.clear_value();
        self.details_generation = self.details_generation.wrapping_add(1);
        self.details = DetailsState::Idle;
        self.reset_versions();
        self.name = None;
        self.listing_row = None;
        cx.emit(SecretViewEvent::Removed);
    }

    // -- Value -------------------------------------------------------------

    /// Drops the retrieved value, the inputs that draw it and any open
    /// editor of it.
    pub fn clear_value(&mut self) {
        self.value_generation = self.value_generation.wrapping_add(1);
        self.value_write_generation = self.value_write_generation.wrapping_add(1);
        self.edit_after_retrieve = false;
        self.value = ValueState::NotLoaded;
        self.value_version = None;
        self.value_edit = None;
        self.value_inputs.clear();
        self.value_input_masks.clear();
        self.value_inputs_view = None;
        self.value_viewer_scroll.set_offset(point(px(0.0), px(0.0)));
    }

    pub fn toggle_reveal(&mut self, index: usize, cx: &mut Context<Self>) {
        if let ValueState::Loaded(value) = &mut self.value {
            value.toggle_reveal(index);
            cx.notify();
        }
    }

    pub fn toggle_reveal_all(&mut self, cx: &mut Context<Self>) {
        if let ValueState::Loaded(value) = &mut self.value {
            value.toggle_reveal_all();
            cx.notify();
        }
    }

    pub fn hide_all(&mut self, cx: &mut Context<Self>) {
        if let ValueState::Loaded(value) = &mut self.value {
            value.hide_all();
            cx.notify();
        }
    }

    pub fn set_value_view(&mut self, view: ValueView, cx: &mut Context<Self>) {
        #[cfg(test)]
        {
            self.value_view_sets += 1;
        }

        if let ValueState::Loaded(value) = &mut self.value {
            value.set_view(view);
            cx.notify();
        }
    }

    fn cycle_value_view(&mut self, cx: &mut Context<Self>) {
        let next = match &self.value {
            ValueState::Loaded(value) => match value.view {
                ValueView::KeyValue => ValueView::Plaintext,
                ValueView::Plaintext => ValueView::KeyValue,
            },
            _ => return,
        };

        self.set_value_view(next, cx);
    }

    /// Copies one field of the retrieved value, on an explicit action only.
    pub fn copy_value_field(&mut self, index: usize, cx: &mut Context<Self>) {
        let ValueState::Loaded(value) = &self.value else {
            return;
        };

        let Some((_, field)) = value.fields().get(index).copied() else {
            return;
        };

        cx.write_to_clipboard(ClipboardItem::new_string(field.expose_secret().to_string()));
        copied_toast(
            dbflux_i18n::t!("document.secret_store.toast.value_copied"),
            cx,
        );
    }

    /// Copies the whole value: the JSON of a key/value secret, the text of a
    /// plaintext one, the base64 of a binary one.
    pub fn copy_full_value(&mut self, cx: &mut Context<Self>) {
        let ValueState::Loaded(value) = &self.value else {
            return;
        };

        let text = value.content.full_text().expose_secret().to_string();
        cx.write_to_clipboard(ClipboardItem::new_string(text));
        copied_toast(
            dbflux_i18n::t!("document.secret_store.toast.value_copied"),
            cx,
        );
    }

    /// Copies the selected secret's name. The one implementation behind the
    /// context menu's Copy name entry and the pane action `m` reaches.
    pub fn copy_selected_name(&mut self, cx: &mut Context<Self>) {
        let Some(name) = self.selected_summary().map(|summary| summary.name.clone()) else {
            return;
        };

        cx.write_to_clipboard(ClipboardItem::new_string(name));
        copied_toast(
            dbflux_i18n::t!("document.secret_store.toast.name_copied"),
            cx,
        );
    }

    pub fn copy_resource_id(&mut self, cx: &mut Context<Self>) {
        let resource_id = match &self.details {
            DetailsState::Loaded(details) => details.resource_id.clone(),
            _ => None,
        };

        let Some(resource_id) = resource_id else {
            // The shortcut is advertised in the footer, so a press that
            // cannot copy says why, in the words the menu entry uses.
            if self.name.is_some() {
                report_error(
                    UserFacingError::new(ErrorKind::User, self.resource_id_unavailable_reason())
                        .with_severity(dbflux_core::observability::EventSeverity::Warn),
                    cx,
                );
            }
            return;
        };

        cx.write_to_clipboard(ClipboardItem::new_string(resource_id));
        copied_toast(
            dbflux_i18n::t!("document.secret_store.toast.resource_id_copied"),
            cx,
        );
    }

    /// Copies the text of the failed retrieval's error.
    pub fn copy_value_error(&mut self, cx: &mut Context<Self>) {
        let ValueState::Failed(failure) = &self.value else {
            return;
        };

        cx.write_to_clipboard(ClipboardItem::new_string(failure.message.clone()));
        copied_toast(
            dbflux_i18n::t!("document.secret_store.toast.error_copied"),
            cx,
        );
    }

    /// Opens the audit viewer on the failed retrieval.
    pub fn view_value_error_in_audit(&mut self, cx: &mut Context<Self>) {
        let ValueState::Failed(failure) = &self.value else {
            return;
        };

        let correlation_id = failure.correlation_id;
        self.app_state.update(cx, |state, cx| {
            state.request_open_audit(Some(correlation_id), cx);
        });
    }

    // -- Command dispatch --------------------------------------------------

    /// Runs the commands that act on the shown secret. Returns whether the
    /// command was one of them.
    pub(super) fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match cmd {
            Command::ToggleValuePanel => self.retrieve_selected_value(cx),
            Command::SaveQuery if self.value_edit.is_some() => self.save_value_edit(cx),
            Command::CycleResultView if self.value_edit.is_some() => {
                self.cycle_edit_mode(window, cx)
            }
            Command::ExpandCollapse => self.toggle_reveal_all(cx),
            Command::ResultsCopyCell => self.copy_resource_id(cx),
            Command::ResultsCopyRow => self.copy_full_value(cx),
            Command::CycleResultView => self.cycle_value_view(cx),
            Command::EditValue if self.can_request_edit_value() => {
                self.request_edit_value(window, cx);
            }
            Command::RollBackVersion if self.supports_roll_back() => {
                self.roll_back_to_previous(window, cx);
            }
            Command::Delete if self.supports_delete() && self.name.is_some() => {
                self.open_delete(window, cx);
            }
            Command::FocusSearch if self.value_editor_focused() => self.find_in_value(window, cx),
            _ => return false,
        }

        true
    }
}

pub(in crate::secret_store) fn copied_toast(message: String, cx: &mut App) {
    dbflux_ui_base::toast::Toast::success(message)
        .meta_right(dbflux_ui_base::toast::now_hms())
        .push(cx);
}
