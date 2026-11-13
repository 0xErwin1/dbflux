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
mod deletion;
pub(super) mod details;
pub(super) mod editor;
mod inputs;
mod menu;
mod pane_actions;

#[cfg(test)]
pub(super) use data::ValueAsk;
pub(super) use pane_actions::callback_action;

pub(crate) use actions::WriteEffect;
#[cfg(test)]
pub(super) use actions::{DeleteKind, Landing};
pub(super) use actions::{DialogHost, DialogShell, FooterSpec, dialog_footer, dialog_modal};
pub(super) use actions::{DialogPin, TagRow, WriteDialog, new_tag_row};
pub(crate) use editor::land_save_answer;

use super::value::{ValueState, ValueView};
use crate::secret_store::CallTiming;
use actions::RotationNotice;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::controls::InputState;
use dbflux_components::modals::ModalFocus;
use dbflux_core::secrecy::ExposeSecret;
use dbflux_core::{
    ConnectedProfile, Connection, SecretDetails, SecretStoreCapabilities, SecretStoreDescriptor,
    SecretSummary, SecretVersionInfo,
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
pub(crate) enum DetailsState {
    #[default]
    Idle,
    Loading,
    Loaded(Box<SecretDetails>),
    Failed(String),
}

/// The version history of the selected secret, loaded apart from its
/// describe so a slow or refused history never hides the details.
#[derive(Clone, Debug, Default)]
pub(crate) enum VersionsState {
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
    /// The profile has no live connection: nothing is asked until it returns.
    NotConnected,
}

/// What a view tells its host.
pub(crate) enum SecretViewEvent {
    /// A write or a follow-up read changed what the listing shows for the
    /// secret: the host reloads its listing in place. Telling the other
    /// views of the secret is the view's own job, done where a write lands
    /// (see [`SecretView::announce_change`]): a re-read after a session
    /// change wrote nothing, so it announces nothing.
    ReloadListing,

    /// The write that was sent from the editor finished. `succeeded` says
    /// whether the store took it; the host reports it to the workspace's close
    /// and quit flows.
    SaveFinished { succeeded: bool },

    /// A store call finished, for the host's status bar.
    StoreCalled(CallTiming),

    /// The shown secret no longer exists: the host drops its selection.
    Removed,

    /// A write dialog opened over the host, which closes its own menus.
    DialogOpened,

    /// The pane's Close button was pressed.
    CloseRequested,

    /// The pane's expand button, or an edit the pane hands over, was pressed:
    /// the host opens the secret in its own tab, with the editor active when
    /// `edit` is set.
    OpenInTab { name: String, edit: bool },

    /// The open dialog went away, however it closed: the host drops what it
    /// kept for a dialog of its own.
    DialogClosed,

    /// The create the host started landed (`stands` when it did not report a
    /// clean success but may have written).
    Created { name: String, stands: bool },
}

pub(crate) struct SecretView {
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
    /// The deletion state this view's own writes confirmed, by secret name.
    confirmed_deletions: HashMap<String, deletion::Confirmation>,
    /// Whether the profile held a live connection when the view last heard of
    /// a state change. The actions that need one are disabled while it is
    /// false; a path that still reaches the store reports the loss.
    connection_present: bool,
    /// The version whose value the value section shows or is retrieving,
    /// `None` for the current value.
    pub(super) value_version: Option<String>,
    pub(super) value: ValueState,
    pub(super) value_generation: u64,
    /// The connection session the shown value was retrieved from. A value is
    /// accepted only for that session and is dropped when the profile's
    /// connection is replaced; the editor built on it keeps this session.
    pub(super) value_session: Option<Arc<dyn Connection>>,
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
    /// The region the host is bound to. While the profile's live connection
    /// is on another one, the view sees no connection: it neither reads nor
    /// writes, whatever the profile's session would serve.
    region_pin: Option<String>,
    /// The host whose own news the view's announcements are.
    change_origin: Option<gpui::EntityId>,
    /// The version Retrieve value fetches: the one a document was opened at,
    /// `None` for the current value.
    opened_version: Option<String>,
    host: ViewHost,
    /// The width the user dragged the details pane to; the default until then.
    details_width: Option<Pixels>,
    /// Where the pane's right edge was painted, which a drag measures from.
    details_right: Rc<Cell<Pixels>>,
    /// Focus of the pane's gutter, which enables its arrow keys.
    details_grip_focus: gpui::FocusHandle,
    /// A pane hands editing to the secret's tab instead of editing in the
    /// side pane. Cleared only by the view-level tests, which edit wherever
    /// they mount the view.
    edit_in_tab: bool,
}

impl EventEmitter<SecretViewEvent> for SecretView {}

/// Where a view is hosted. It decides which actions the view draws itself
/// (the pane has a bottom bar, the tab the artboard's header and section
/// buttons, so no action is drawn twice) and where a value is edited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ViewHost {
    /// The details pane beside a listing.
    Pane,
    /// The tab of one secret.
    Tab,
}

impl SecretView {
    pub(crate) fn new(
        profile_id: Uuid,
        store: SecretStoreDescriptor,
        app_state: Entity<AppStateEntity>,
        region_switching: Rc<Cell<bool>>,
        host: ViewHost,
        cx: &mut Context<Self>,
    ) -> Self {
        let connection_present = app_state.read(cx).connections().contains_key(&profile_id);

        Self {
            region_pin: None,
            change_origin: None,
            opened_version: None,
            host,
            details_width: None,
            details_right: Rc::new(Cell::new(px(0.0))),
            details_grip_focus: cx.focus_handle(),
            edit_in_tab: host == ViewHost::Pane,
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
            confirmed_deletions: HashMap::new(),
            connection_present,
            value_version: None,
            value: ValueState::NotLoaded,
            value_generation: 0,
            value_session: None,
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

    /// Lets a pane edit in place, which only the view-level tests do.
    #[cfg(test)]
    pub(crate) fn set_edit_in_place(&mut self, in_place: bool) {
        self.edit_in_tab = !in_place;
    }

    /// Whether the view is hosted as a secret's tab.
    pub(crate) fn in_tab(&self) -> bool {
        self.host == ViewHost::Tab
    }

    /// The menu entries of `name`, as names, for the tests.
    #[cfg(test)]
    pub(crate) fn menu_actions_for_test(&self, name: &str) -> Vec<String> {
        self.secret_menu_items(name)
            .iter()
            .map(|item| format!("{:?}", item.action))
            .collect()
    }

    /// Lands a scheduled delete of `name` the way the delete dialog does,
    /// after the test sent it to the store itself.
    #[cfg(test)]
    pub(crate) fn land_scheduled_delete_for_test(
        &mut self,
        name: &str,
        deletion: dbflux_core::SecretDeletion,
        cx: &mut Context<Self>,
    ) {
        self.land_delete(
            name,
            actions::DeleteKind::Scheduled { window_days: 7 },
            actions::Landing::Done(deletion),
            cx,
        );
    }

    /// Puts the details in the loading state with nothing known of the
    /// secret, as a new tab is before its describe lands.
    #[cfg(test)]
    pub(crate) fn load_details_for_test(&mut self, cx: &mut Context<Self>) {
        self.details = DetailsState::Loading;
        self.summary_cache = None;
        self.listing_row = None;
        cx.notify();
    }

    /// Puts the details in the failed state with nothing known of the secret,
    /// as a describe that never succeeded leaves a new tab.
    #[cfg(test)]
    pub(crate) fn fail_details_for_test(&mut self, error: &str, cx: &mut Context<Self>) {
        self.details = DetailsState::Failed(error.to_string());
        self.summary_cache = None;
        self.listing_row = None;
        cx.notify();
    }

    /// Names the host the view's announcements belong to, so the host does not
    /// follow its own news.
    pub(crate) fn set_change_origin(&mut self, origin: gpui::EntityId) {
        self.change_origin = Some(origin);
    }

    /// Where an announcement of this view goes: the connection and store it
    /// reads, `region` as the write was sent to it.
    pub(crate) fn change_scope(
        &self,
        region: Option<String>,
        cx: &Context<Self>,
    ) -> super::changes::ChangeScope {
        super::changes::ChangeScope {
            profile_id: self.profile_id,
            store_id: self.store.id.clone(),
            region,
            origin: self.change_origin.unwrap_or_else(|| cx.entity_id()),
        }
    }

    /// The region the view reads under now.
    fn scope_region(&self, cx: &App) -> Option<String> {
        self.live_profile(cx)
            .and_then(|connected| connected.connection.current_region())
            .or_else(|| self.region_pin.clone())
    }

    /// Tells the other views of the store that `name` changed here. Called
    /// where a write landed, with the target the write carried: the
    /// selection may have moved on since.
    pub(crate) fn announce_change(
        &self,
        name: &str,
        kind: super::changes::SecretChangeKind,
        cx: &mut Context<Self>,
    ) {
        let scope = self.change_scope(self.scope_region(cx), cx);
        scope.publish(name, kind, cx);
    }

    /// The generation a write started now would carry, for the tests that
    /// land a write by hand.
    #[cfg(test)]
    pub(crate) fn write_generation_for_test(&self) -> u64 {
        self.value_write_generation
    }

    /// Whether the profile had a live connection at the last state change.
    pub(crate) fn is_connected(&self) -> bool {
        self.connection_present
    }

    /// The profile's connection, unless the view is bound to a region the
    /// connection is not on. The one lookup every read and write goes
    /// through.
    pub(crate) fn live_profile<'a>(&self, cx: &'a App) -> Option<&'a ConnectedProfile> {
        let connected = self
            .app_state
            .read(cx)
            .connections()
            .get(&self.profile_id)?;

        match &self.region_pin {
            Some(pin) if connected.connection.current_region().as_deref() != Some(pin.as_str()) => {
                None
            }
            _ => Some(connected),
        }
    }

    /// Whether the profile is connected on another region than the one the
    /// view is bound to.
    pub(crate) fn region_mismatch(&self, cx: &App) -> bool {
        self.region_pin.is_some()
            && self.live_profile(cx).is_none()
            && self
                .app_state
                .read(cx)
                .connections()
                .contains_key(&self.profile_id)
    }

    /// Makes Retrieve value fetch `version` rather than the current value.
    pub(crate) fn set_opened_version(&mut self, version: Option<String>) {
        self.opened_version = version;
    }

    /// Reads the shown secret again: its details and history, dropping the
    /// retrieved value the way selecting it afresh does. Refused while the
    /// editor holds unsaved text.
    pub(crate) fn refresh_shown(&mut self, cx: &mut Context<Self>) {
        let Some(name) = self.name.clone() else {
            return;
        };

        if self.block_unsaved_value_edits(cx) {
            return;
        }

        let row = self.listing_row.clone();
        self.show_secret(name, row, cx);
    }

    /// Binds the view to `region` (or releases it). The actions that need a
    /// connection follow at once.
    pub(crate) fn set_region_pin(&mut self, region: Option<String>, cx: &mut Context<Self>) {
        self.region_pin = region;
        self.sync_connection_presence(cx);
        cx.notify();
    }

    /// Follows the profile's connection coming and going, so the actions that
    /// need it enable and disable with it.
    pub(super) fn sync_connection_presence(&mut self, cx: &mut Context<Self>) {
        if self.drop_confirmations_of_replaced_session(cx) {
            cx.notify();
        }

        let present = self.connection(cx).is_some();

        if present != self.connection_present {
            self.connection_present = present;
            cx.notify();
        }
    }

    /// The first half of following a state change of the app: presence and
    /// the dialog's session. A host with something of its own to adopt (the
    /// listing's region) does it between the halves.
    pub(crate) fn begin_connection_change(&mut self, cx: &mut Context<Self>) {
        self.sync_connection_presence(cx);
        self.close_dialog_if_session_replaced(cx);
    }

    /// The second half: the history, the details and the value follow the
    /// connection the profile holds now.
    pub(crate) fn finish_connection_change(&mut self, cx: &mut Context<Self>) {
        self.adopt_connection_session(crate::secret_store::HistoryLoad::Automatic, cx);
        self.adopt_details_session(cx);
        self.drop_value_of_replaced_session(cx);
    }

    /// Runs what the view needs a window for before it draws: the inputs
    /// that show a retrieved value, and the editor a pending Edit value
    /// opens once its retrieval landed.
    pub(crate) fn prepare_frame(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.sync_connection_presence(cx);

        // An editor that kept unsaved text across a session change may have
        // closed since; its value must not outlive the session either.
        self.drop_value_of_replaced_session(cx);

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
    pub(crate) fn value_editor_focused(&self) -> bool {
        self.value_edit.as_ref().is_some_and(|edit| edit.focused)
    }

    pub(crate) fn details(&self) -> &DetailsState {
        &self.details
    }

    #[cfg(test)]
    pub(crate) fn versions(&self) -> &VersionsState {
        &self.versions
    }

    /// The version the value section shows, owned, `None` for the current
    /// value.
    pub(crate) fn shown_version(&self) -> Option<String> {
        self.value_version.clone()
    }

    /// The version the value section shows, `None` for the current value.
    #[cfg(test)]
    pub(crate) fn value_version(&self) -> Option<&str> {
        self.value_version.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn value(&self) -> &ValueState {
        &self.value
    }

    /// The name of the secret the view shows.
    pub(crate) fn name(&self) -> Option<&str> {
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
    pub(crate) fn selected_summary(&self) -> Option<&SecretSummary> {
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
    /// does not replace what a describe learned. `observed_by` is the
    /// generation of the listing request that read the row, `None` for a copy
    /// the selection brings along or a row a failed page retained: those are
    /// no observation of the store.
    pub(super) fn set_listing_row(
        &mut self,
        row: Option<SecretSummary>,
        refresh_cache: bool,
        observed_by: Option<u64>,
        cx: &mut Context<Self>,
    ) {
        self.drop_confirmations_of_replaced_session(cx);

        // A failed page keeps rows it did not just read: they are no
        // observation.
        let mut row = row;
        if let Some(summary) = row.as_mut() {
            if refresh_cache && let Some(generation) = observed_by {
                self.observe_listed(summary, generation);
            }
            self.conform_summary(summary);
        }

        if refresh_cache && let Some(summary) = &row {
            self.summary_cache = Some(summary.clone());
        }

        self.listing_row = row;

        // Without a confirmed state the listing is the later word: a row
        // that shows the secret active lifts the describe it outdates, or
        // the guards would keep reading the old deletion, whatever the
        // store's history support.
        if refresh_cache
            && let Some(name) = self.name.clone()
            && self
                .listing_row
                .as_ref()
                .is_some_and(|row| row.name == name && !self.is_deletion_pending(row))
        {
            self.lift_stale_deletion(&name);

            if matches!(self.versions, VersionsState::ScheduledForDeletion) {
                self.request_versions(name, crate::secret_store::HistoryLoad::Automatic, cx);
            }
        }
    }

    /// Drops a shown value that came from a connection the profile no longer
    /// holds, unless the editor still has unsaved text: that text stays, bound
    /// to its own session, and its save is refused against the new one.
    pub(super) fn drop_value_of_replaced_session(&mut self, cx: &mut Context<Self>) {
        let Some(origin) = &self.value_session else {
            return;
        };

        // No connection at all is as much a change of session as another one.
        let replaced = self
            .connection(cx)
            .is_none_or(|live| !Arc::ptr_eq(origin, &live));

        if replaced && !self.has_unsaved_value_edits(cx) {
            self.clear_value();
            cx.notify();
        }
    }

    /// Whether Retrieve value may run for the shown secret: one whose
    /// deletion is not pending, with no retrieval in flight.
    pub(crate) fn can_retrieve_value(&self) -> bool {
        self.connection_present
            && self
                .selected_summary()
                .is_some_and(|summary| !self.is_deletion_pending(summary))
            && !matches!(self.value, ValueState::Loading)
    }

    // -- Selection ---------------------------------------------------------

    /// Shows `name`: drops what the previous selection left (dialog, value,
    /// details, history) and describes the new secret. The caller has
    /// already made sure no unsaved edit is lost.
    pub(crate) fn show_secret(
        &mut self,
        name: String,
        listing_row: Option<SecretSummary>,
        cx: &mut Context<Self>,
    ) {
        self.reset_selection(true, cx);
        self.summary_cache = None;
        self.listing_row = None;
        self.name = Some(name.clone());
        self.set_listing_row(listing_row, true, None, cx);
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
    pub(crate) fn forget_removed_secret(&mut self, name: &str, cx: &mut Context<Self>) {
        if !self.is_for(name) {
            return;
        }

        // Unsaved text outlives the secret: it stays, read-only, until the
        // user discards it. Nothing of it can be written any more.
        let draft = self
            .has_unsaved_value_edits(cx)
            .then(|| self.value_edit.take())
            .flatten();

        self.clear_value();
        self.details_generation = self.details_generation.wrapping_add(1);
        self.details = DetailsState::Idle;
        self.reset_versions();
        self.name = None;
        self.listing_row = None;

        if let Some(mut draft) = draft {
            draft.removed = true;
            draft.saving = false;
            self.value_edit = Some(draft);
        }

        cx.emit(SecretViewEvent::Removed);
    }

    // -- Value -------------------------------------------------------------

    /// Drops the retrieved value, the inputs that draw it and any open
    /// editor of it.
    pub(crate) fn clear_value(&mut self) {
        self.value_generation = self.value_generation.wrapping_add(1);
        self.value_write_generation = self.value_write_generation.wrapping_add(1);
        self.edit_after_retrieve = false;
        self.value = ValueState::NotLoaded;
        self.value_version = None;
        self.value_session = None;
        self.value_edit = None;
        self.value_inputs.clear();
        self.value_input_masks.clear();
        self.value_inputs_view = None;
        self.value_viewer_scroll.set_offset(point(px(0.0), px(0.0)));
    }

    pub(crate) fn toggle_reveal(&mut self, index: usize, cx: &mut Context<Self>) {
        if let ValueState::Loaded(value) = &mut self.value {
            value.toggle_reveal(index);
            cx.notify();
        }
    }

    pub(crate) fn toggle_reveal_all(&mut self, cx: &mut Context<Self>) {
        if let ValueState::Loaded(value) = &mut self.value {
            value.toggle_reveal_all();
            cx.notify();
        }
    }

    pub(crate) fn hide_all(&mut self, cx: &mut Context<Self>) {
        if let ValueState::Loaded(value) = &mut self.value {
            value.hide_all();
            cx.notify();
        }
    }

    pub(crate) fn set_value_view(&mut self, view: ValueView, cx: &mut Context<Self>) {
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
    pub(crate) fn copy_value_field(&mut self, index: usize, cx: &mut Context<Self>) {
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
    pub(crate) fn copy_full_value(&mut self, cx: &mut Context<Self>) {
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
    pub(crate) fn copy_selected_name(&mut self, cx: &mut Context<Self>) {
        let Some(name) = self.selected_summary().map(|summary| summary.name.clone()) else {
            return;
        };

        cx.write_to_clipboard(ClipboardItem::new_string(name));
        copied_toast(
            dbflux_i18n::t!("document.secret_store.toast.name_copied"),
            cx,
        );
    }

    pub(crate) fn copy_resource_id(&mut self, cx: &mut Context<Self>) {
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
    pub(crate) fn copy_value_error(&mut self, cx: &mut Context<Self>) {
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
    pub(crate) fn view_value_error_in_audit(&mut self, cx: &mut Context<Self>) {
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
    pub(crate) fn dispatch_command(
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
