//! Browser of one secret store of a connection
//! (`DatabaseCategory::SecretStore`), such as a cloud secrets manager or a
//! hierarchical parameter store.
//!
//! The listing is paged on the server side and holds metadata only. Selecting
//! a secret describes it, still without its value. A value is fetched only by
//! the explicit Retrieve value action, which records an audit event, and it
//! is dropped when the selection changes, the listing is refreshed or the tab
//! closes. Behavior that differs between stores follows the store's
//! `SecretStoreCapabilities`, never a driver id.

mod actions;
mod context_menu;
mod data;
mod details;
mod editor;
mod list;
mod listing;
mod pane;
mod region;
mod render;
mod value;
mod write;

use actions::{RotationNotice, WriteDialog};
use data::HistoryLoad;
pub use listing::{PathMode, SecretListing};
pub use region::RegionListState;
pub use value::{
    RetrievalTarget, RetrievedValue, VALUE_RETRIEVE_FAILED_ACTION, VALUE_RETRIEVED_ACTION,
    ValueContent, ValueFailure, ValueState, ValueView, encode_base64, value_retrieval_event,
};
pub use write::{
    OperationActions, SECRET_VALUE_WRITE_FAILED_ACTION, SECRET_VALUE_WRITE_REFUSED_ACTION,
    SECRET_VALUE_WRITTEN_ACTION, SECRET_WRITE_CLASSIFICATION, SecretOperation, SecretWriteFailure,
    SecretWriteFailureKind, SecretWriteOutcome, WriteContext, WriteTarget, create_secret,
    delete_secret, put_secret_value, restore_secret, roll_back_secret, rotate_secret_now,
    secret_write_event, update_secret_description, update_secret_rotation, update_secret_tags,
};

use super::handle::DocumentEvent;
use super::types::{DocumentId, DocumentState};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::controls::{Dropdown, DropdownSelectionChanged, InputEvent, InputState};
use dbflux_components::modals::ModalFocus;
use dbflux_core::secrecy::ExposeSecret;
use dbflux_core::{
    Connection, DriverCapabilities, RefreshPolicy, SecretDetails, SecretStoreCapabilities,
    SecretStoreDescriptor, SecretSummary, SecretVersionInfo,
};
use dbflux_ui_base::AppStateChanged;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;
use std::sync::Arc;
use uuid::Uuid;

/// Which part of the document owns keyboard input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecretFocusMode {
    Table,
    Search,
}

/// One row of the listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecretRowKey {
    /// A sub-path of a hierarchical store's current level.
    Path(String),
    Secret(String),
    /// The row that loads the next page.
    LoadMore,
}

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
}

/// The last store call and how long it took, for the status bar.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreCall {
    /// `hierarchical` picks the noun the status bar uses for the entries.
    List {
        count: usize,
        hierarchical: bool,
    },
    Describe,
    Retrieve,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallTiming {
    pub call: StoreCall,
    pub millis: u128,
}

pub struct SecretStoreDocument {
    id: DocumentId,
    title: String,
    profile_id: Uuid,
    store: SecretStoreDescriptor,
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,
    is_active_tab: bool,
    refresh_policy: RefreshPolicy,
    listing: SecretListing,
    /// Hierarchical stores: the path being listed, `None` for the root.
    path: Option<String>,
    path_mode: PathMode,
    /// The name filter of the current listing, sent to the store.
    name_filter: String,
    search_input: Entity<InputState>,
    focus_mode: SecretFocusMode,
    selected: Option<SecretRowKey>,
    details: DetailsState,
    details_generation: u64,
    versions: VersionsState,
    versions_generation: u64,
    #[cfg(test)]
    value_view_sets: usize,
    /// The connection the history was listed from, so ids of a replaced
    /// session are never retrieved against the new one.
    versions_session: Option<Arc<dyn Connection>>,
    /// Some path of this document already reported the missing connection
    /// for the current selection, so the next one to find the same loss
    /// stays quiet. Cleared when a connection is found again or the
    /// selection is reset.
    missing_connection_reported: bool,
    /// The version whose value the value section shows or is retrieving,
    /// `None` for the current value.
    value_version: Option<String>,
    value: ValueState,
    value_generation: u64,
    /// One read-only, secret-marked input per drawn value field, built on
    /// the render pass after a retrieval (an input needs a `Window`).
    value_inputs: Vec<Entity<InputState>>,
    /// The masked flag last applied to each of `value_inputs`.
    value_input_masks: Vec<bool>,
    value_inputs_view: Option<ValueView>,
    /// Scroll position of the multi-line viewer.
    value_viewer_scroll: ScrollHandle,
    /// The open editor of a plaintext value, if any.
    value_edit: Option<editor::ValueEditState>,
    /// Bumped when the value or the selection is dropped, so a write that
    /// lands after the user moved on does not touch the pane.
    value_write_generation: u64,
    last_call: Option<CallTiming>,
    /// Whether the connection's driver advertises `REGION_SWITCH`. Never a
    /// driver id: the capability gates the control.
    region_switchable: bool,
    /// The region the listing is loaded under, read from the connection at
    /// construction and updated after a successful switch.
    region: Option<String>,
    /// The regions the connection offers.
    regions: RegionListState,
    /// What the footer measured of itself, to decide which key hints fit.
    footer_fit: std::rc::Rc<render::FooterFit>,
    /// Bumped by every region discovery, so a result that a newer discovery or
    /// a replaced connection made stale is dropped instead of shown.
    regions_generation: u64,
    /// A region switch is reconnecting on the background executor.
    region_switch_in_flight: bool,
    /// The toolbar region picker, built only when the driver can switch.
    region_dropdown: Option<Entity<Dropdown>>,
    /// The open right-click menu of the listing, if any.
    context_menu: Option<context_menu::SecretContextMenu>,
    /// A chosen menu entry, queued by its click and run on the next render,
    /// which has the window the action needs.
    pending_menu_action: Option<context_menu::SecretMenuAction>,
    /// The focus handle of the open menu's overlay, which takes the keyboard
    /// while the menu is up.
    context_menu_focus: FocusHandle,
    /// The write dialog open over the document, if any.
    dialog: Option<WriteDialog>,
    /// Moves the keyboard into the dialog when it opens and hands it back
    /// when it closes.
    dialog_focus: ModalFocus,
    /// A governed write is in flight. Another write waits for it, so a
    /// double click cannot send the request twice.
    write_in_flight: bool,
    /// A rotation cancelled before it finished, shown in the Rotation
    /// section while its secret stays selected.
    rotation_notice: Option<RotationNotice>,
    /// The bottom bar's Edit value was pressed before the value was
    /// retrieved: the editor opens when the retrieval lands.
    edit_after_retrieve: bool,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<DocumentEvent> for SecretStoreDocument {}

impl SecretStoreDocument {
    /// Opens the store `store_id` of the connection `profile_id`, at `path`
    /// when the store is hierarchical. A connection without that store opens
    /// an empty document with an error.
    pub fn new(
        profile_id: Uuid,
        store_id: String,
        path: Option<String>,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let (connection_name, descriptor, region, region_switchable) = {
            let state = app_state.read(cx);
            let connected = state.connections().get(&profile_id);

            let name = connected
                .map(|connected| connected.profile.name.clone())
                .unwrap_or_default();

            let descriptor = connected
                .and_then(|connected| {
                    connected
                        .connection
                        .secret_store(&store_id)
                        .map(|store| store.descriptor().clone())
                })
                .unwrap_or_else(|| {
                    SecretStoreDescriptor::new(
                        store_id.clone(),
                        store_id.clone(),
                        SecretStoreCapabilities::empty(),
                    )
                });

            let region = connected.and_then(|connected| connected.connection.current_region());
            let region_switchable = connected.is_some_and(|connected| {
                connected
                    .connection
                    .metadata()
                    .capabilities
                    .contains(DriverCapabilities::REGION_SWITCH)
            });

            (name, descriptor, region, region_switchable)
        };

        let search_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("document.secret_store.search_placeholder"))
        });

        let search_subscription = cx.subscribe_in(
            &search_input,
            window,
            |this, input, event: &InputEvent, _window, cx| match event {
                InputEvent::PressEnter { .. } => {
                    let filter = input.read(cx).value().to_string();
                    this.apply_name_filter(filter, cx);
                }
                InputEvent::Focus => {
                    this.focus_mode = SecretFocusMode::Search;
                    cx.notify();
                }
                InputEvent::Blur => {
                    this.focus_mode = SecretFocusMode::Table;
                    cx.notify();
                }
                InputEvent::Change => {}
            },
        );

        let path = path.filter(|_| descriptor.supports(SecretStoreCapabilities::HIERARCHICAL));

        // The picker exists only when the driver can switch regions; its
        // items arrive once the background listing lands.
        let (region_dropdown, region_subscription) = if region_switchable {
            let dropdown = cx.new(|_cx| {
                Dropdown::new("secret-region")
                    .placeholder(dbflux_i18n::t!("document.secret_store.region.label"))
                    .aria_label(dbflux_i18n::t!("document.secret_store.region.label"))
                    .toolbar_style(true)
                    .mono_label(true)
            });

            let subscription = cx.subscribe_in(
                &dropdown,
                window,
                |this, _, event: &DropdownSelectionChanged, _window, cx| {
                    this.switch_region(event.item.value.to_string(), cx);
                },
            );

            (Some(dropdown), Some(subscription))
        } else {
            (None, None)
        };

        let mut subscriptions = vec![search_subscription];
        subscriptions.extend(region_subscription);
        // The live connection can be replaced (or moved to another region)
        // outside this document; the subscription keeps the region and the
        // listing on what the connection actually reaches.
        subscriptions.push(
            cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
                this.adopt_connection_region(cx);
                this.adopt_connection_session(HistoryLoad::Automatic, cx);
            }),
        );

        let mut doc = Self {
            id: DocumentId::new(),
            title: dbflux_i18n::t!(
                "document.secret_store.title",
                store = descriptor.label.as_str(),
                connection = connection_name.as_str()
            ),
            profile_id,
            store: descriptor,
            app_state,
            focus_handle: cx.focus_handle(),
            is_active_tab: true,
            refresh_policy: RefreshPolicy::Manual,
            listing: SecretListing::default(),
            path,
            path_mode: PathMode::Tree,
            name_filter: String::new(),
            search_input,
            focus_mode: SecretFocusMode::Table,
            selected: None,
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
            last_call: None,
            footer_fit: Default::default(),
            region_switchable,
            region,
            regions: RegionListState::Idle,
            regions_generation: 0,
            region_switch_in_flight: false,
            region_dropdown,
            context_menu: None,
            pending_menu_action: None,
            context_menu_focus: cx.focus_handle(),
            dialog: None,
            dialog_focus: ModalFocus::new(cx),
            write_in_flight: false,
            rotation_notice: None,
            edit_after_retrieve: false,
            _subscriptions: subscriptions,
        };

        if region_switchable {
            doc.load_regions(cx);
        }

        doc.load_first_page(cx);
        doc
    }

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        self.title.clone()
    }

    /// The tab's state. The dirty dot rides on unsaved value edits first:
    /// the open editor holding text no save has written is the one change a
    /// close must ask about.
    pub fn state(&self, cx: &App) -> DocumentState {
        if self.has_unsaved_value_edits(cx) {
            DocumentState::Modified
        } else if self.listing.loading {
            DocumentState::Loading
        } else if self.listing.error.is_some() {
            DocumentState::Error
        } else {
            DocumentState::Clean
        }
    }

    pub fn can_close(&self) -> bool {
        true
    }

    pub fn connection_id(&self) -> Option<Uuid> {
        Some(self.profile_id)
    }

    pub fn store_id(&self) -> &str {
        &self.store.id
    }

    pub fn store(&self) -> &SecretStoreDescriptor {
        &self.store
    }

    pub fn listing(&self) -> &SecretListing {
        &self.listing
    }

    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    pub fn path_mode(&self) -> PathMode {
        self.path_mode
    }

    pub fn selected(&self) -> Option<&SecretRowKey> {
        self.selected.as_ref()
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

    pub fn last_call(&self) -> Option<CallTiming> {
        self.last_call
    }

    /// Whether the connection's driver can switch regions.
    pub fn supports_region_switch(&self) -> bool {
        self.region_switchable
    }

    /// The region the listing is loaded under.
    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }

    /// The regions the connection offers.
    pub fn regions(&self) -> &RegionListState {
        &self.regions
    }

    /// Whether a region switch is reconnecting.
    pub fn region_switch_in_flight(&self) -> bool {
        self.region_switch_in_flight
    }

    pub fn refresh_policy(&self) -> RefreshPolicy {
        self.refresh_policy
    }

    pub fn set_active_tab(&mut self, active: bool) {
        self.is_active_tab = active;

        // A menu belongs to the document being looked at: a tab that stopped
        // being active closes it and drops an entry queued but not yet run;
        // reactivating the same tab keeps what was open.
        if !active {
            self.drop_context_menu();
        }
    }

    pub fn set_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        self.refresh_policy = policy;
        cx.notify();
    }

    pub fn active_context(&self) -> ContextId {
        // A dialog owns the keyboard: its inputs keep every letter the
        // listing would otherwise read as a command, and its buttons answer
        // Enter and Escape through the dialog itself.
        if self.dialog.is_some() {
            return ContextId::TextInput;
        }

        if self.context_menu.is_some() {
            return ContextId::ContextMenu;
        }

        if self.focus_mode == SecretFocusMode::Search
            || self.value_edit.as_ref().is_some_and(|edit| edit.focused)
        {
            return ContextId::TextInput;
        }

        ContextId::Results
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_handle.focus(window, cx);
        self.focus_mode = SecretFocusMode::Table;
        cx.notify();
    }

    fn hierarchical(&self) -> bool {
        self.store.supports(SecretStoreCapabilities::HIERARCHICAL)
    }

    /// What the footer says about the loaded rows: the footer and the status
    /// bar draw the same counts from here.
    pub(crate) fn loaded_summary(&self) -> String {
        list::loaded_summary(
            self.hierarchical(),
            self.path_mode,
            self.path.as_deref(),
            self.listing.child_paths.len(),
            self.listing.entries.len(),
        )
    }

    /// The selected secret's listing row, when a secret is selected.
    pub fn selected_summary(&self) -> Option<&SecretSummary> {
        match &self.selected {
            Some(SecretRowKey::Secret(name)) => self.listing.find(name),
            _ => None,
        }
    }

    /// Whether Retrieve value may run for the selection: a secret whose
    /// deletion is not pending, with no retrieval in flight.
    pub fn can_retrieve_value(&self) -> bool {
        self.selected_summary()
            .is_some_and(|summary| !summary.is_pending_deletion())
            && !matches!(self.value, ValueState::Loading)
    }

    // -- Rows and selection ----------------------------------------------

    /// The rows in display order: the sub-paths of a tree level, the
    /// entries, and the Load next page row while more pages remain.
    pub fn rows(&self) -> Vec<SecretRowKey> {
        let mut rows: Vec<SecretRowKey> = Vec::new();

        if self.hierarchical() && self.path_mode == PathMode::Tree {
            rows.extend(
                self.listing
                    .child_paths
                    .iter()
                    .cloned()
                    .map(SecretRowKey::Path),
            );
        }

        rows.extend(
            self.listing
                .entries
                .iter()
                .map(|entry| SecretRowKey::Secret(entry.name.clone())),
        );

        if self.listing.has_more() {
            rows.push(SecretRowKey::LoadMore);
        }

        rows
    }

    /// Moves the selection to `row`. Selecting another secret drops the
    /// retrieved value and describes the new one.
    pub fn select_row(&mut self, row: SecretRowKey, cx: &mut Context<Self>) {
        self.focus_mode = SecretFocusMode::Table;

        if self.selected.as_ref() == Some(&row) {
            cx.notify();
            return;
        }

        // The open editor belongs to the selection's value: moving to
        // another secret with unsaved text would drop it, so the move is
        // refused and the editor keeps its text until Save or Discard.
        if self.block_unsaved_value_edits(cx) {
            return;
        }

        self.clear_value();
        self.details_generation = self.details_generation.wrapping_add(1);
        self.details = DetailsState::Idle;
        self.reset_versions();

        if let SecretRowKey::Secret(name) = &row {
            let name = name.clone();
            self.selected = Some(row);
            self.describe(name.clone(), cx);
            self.load_versions(name, cx);
        } else {
            self.selected = Some(row);
        }

        cx.notify();
    }

    /// Closes the details pane: deselects the secret and drops its value,
    /// the way moving to another secret does. Refused while the editor holds
    /// unsaved text.
    pub fn close_details(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.selected.is_none() || self.block_unsaved_value_edits(cx) {
            return;
        }

        // The editor goes away with the value; focus it holds would go with
        // it and leave the document's shortcuts without a target.
        if self.value_edit.as_ref().is_some_and(|edit| edit.focused) {
            self.focus_handle.focus(window, cx);
        }

        self.clear_value();
        self.details_generation = self.details_generation.wrapping_add(1);
        self.details = DetailsState::Idle;
        self.reset_versions();
        self.selected = None;
        cx.notify();
    }

    fn move_selection(&mut self, delta: isize, cx: &mut Context<Self>) {
        let rows = self.rows();

        if rows.is_empty() {
            return;
        }

        let current = self
            .selected
            .as_ref()
            .and_then(|selected| rows.iter().position(|row| row == selected));

        let last = rows.len() as isize - 1;
        let next = match current {
            Some(index) => (index as isize + delta).clamp(0, last) as usize,
            None if delta >= 0 => 0,
            None => last as usize,
        };

        if let Some(row) = rows.get(next).cloned() {
            self.select_row(row, cx);
        }
    }

    fn select_edge(&mut self, last: bool, cx: &mut Context<Self>) {
        let rows = self.rows();
        let row = if last { rows.last() } else { rows.first() };

        if let Some(row) = row.cloned() {
            self.select_row(row, cx);
        }
    }

    /// Enter: a sub-path opens that level, the Load next page row loads it.
    fn activate_selected(&mut self, cx: &mut Context<Self>) {
        match self.selected.clone() {
            Some(SecretRowKey::Path(path)) => self.navigate_to(Some(path), cx),
            Some(SecretRowKey::LoadMore) => self.load_more(cx),
            Some(SecretRowKey::Secret(_)) | None => {}
        }
    }

    // -- Navigation --------------------------------------------------------

    /// Lists `path` of a hierarchical store, from its first page. A flat
    /// store ignores it.
    pub fn navigate_to(&mut self, path: Option<String>, cx: &mut Context<Self>) {
        if !self.hierarchical() || self.path == path {
            return;
        }

        if self.block_unsaved_value_edits(cx) {
            return;
        }

        self.path = path;
        self.load_first_page(cx);
    }

    fn navigate_up(&mut self, cx: &mut Context<Self>) {
        let Some(path) = self.path.as_deref() else {
            return;
        };

        let parent = listing::parent_path(path);
        self.navigate_to(parent, cx);
    }

    /// Switches a hierarchical store between one level and every entry
    /// below the path.
    pub fn toggle_path_mode(&mut self, cx: &mut Context<Self>) {
        if !self.hierarchical() {
            return;
        }

        self.path_mode = match self.path_mode {
            PathMode::Tree => PathMode::Flat,
            PathMode::Flat => PathMode::Tree,
        };
        self.load_first_page(cx);
    }

    pub fn set_path_mode(&mut self, mode: PathMode, cx: &mut Context<Self>) {
        if self.path_mode != mode {
            self.toggle_path_mode(cx);
        }
    }

    /// Lists again with `filter` as the store-side name filter.
    pub fn apply_name_filter(&mut self, filter: String, cx: &mut Context<Self>) {
        self.name_filter = filter.trim().to_string();
        self.focus_mode = SecretFocusMode::Table;
        self.load_first_page(cx);
    }

    fn focus_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_mode = SecretFocusMode::Search;
        self.search_input
            .update(cx, |input, cx| input.focus(window, cx));
        cx.notify();
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

    /// Copies the selected sub-path. The one implementation behind the
    /// context menu's Copy path entry and the pane action `m` reaches.
    pub fn copy_selected_path(&mut self, cx: &mut Context<Self>) {
        let Some(SecretRowKey::Path(path)) = self.selected.as_ref() else {
            return;
        };

        let path = path.clone();
        cx.write_to_clipboard(ClipboardItem::new_string(path));
        copied_toast(
            dbflux_i18n::t!("document.secret_store.toast.path_copied"),
            cx,
        );
    }

    /// Copies the selected secret's provider-wide identifier (an ARN).
    pub fn copy_resource_id(&mut self, cx: &mut Context<Self>) {
        let resource_id = match &self.details {
            DetailsState::Loaded(details) => details.resource_id.clone(),
            _ => None,
        };

        let Some(resource_id) = resource_id else {
            // The shortcut is advertised in the footer, so a press that
            // cannot copy says why, in the words the menu entry uses.
            if matches!(self.selected, Some(SecretRowKey::Secret(_))) {
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

    // -- Pane actions --------------------------------------------------------

    /// The controls of the toolbar and the details pane, for the pane
    /// actions menu `m` opens.
    pub(crate) fn pane_actions(&self, this: &Entity<Self>) -> Vec<crate::pane::PaneAction> {
        use crate::pane::PaneAction;
        use dbflux_components::icons::AppIcon;

        let context = ContextId::Results;
        let target = this.downgrade();
        let value_loaded = matches!(self.value, ValueState::Loaded(_));
        let value_failed = matches!(self.value, ValueState::Failed(_));
        let has_resource_id = matches!(
            &self.details,
            DetailsState::Loaded(details) if details.resource_id.is_some()
        );

        let command = |id: &'static str, key: &str, command: Command| {
            PaneAction::command(id, dbflux_i18n::t!(key), command, context)
        };

        let mut actions = vec![
            command(
                "secret-retrieve-value",
                "document.secret_store.value.retrieve",
                Command::ToggleValuePanel,
            )
            .icon(AppIcon::Eye)
            .enabled(self.can_retrieve_value()),
            command(
                "secret-copy-resource-id",
                "document.secret_store.action.copy_resource_id",
                Command::ResultsCopyCell,
            )
            .enabled(has_resource_id),
            command(
                "secret-copy-value",
                "document.secret_store.value.copy_all",
                Command::ResultsCopyRow,
            )
            .enabled(value_loaded),
            command(
                "secret-reveal",
                "document.secret_store.value.reveal_all",
                Command::ExpandCollapse,
            )
            .enabled(value_loaded),
            callback_action(
                &target,
                "secret-hide-all",
                "document.secret_store.value.hide_all",
                Self::hide_all,
            )
            .enabled(value_loaded),
            callback_action(
                &target,
                "secret-copy-error",
                "document.secret_store.value.copy_error",
                Self::copy_value_error,
            )
            .enabled(value_failed),
            callback_action(
                &target,
                "secret-view-in-audit",
                "document.secret_store.value.view_in_audit",
                Self::view_value_error_in_audit,
            )
            .enabled(value_failed),
            callback_action(
                &target,
                "secret-editor-save",
                "document.secret_store.edit.save",
                Self::save_value_edit,
            )
            .enabled(self.is_editing_value() && !self.is_saving_value_edit()),
            callback_action(
                &target,
                "secret-editor-discard",
                "document.secret_store.edit.discard",
                Self::discard_value_edit,
            )
            .enabled(self.is_editing_value() && !self.is_saving_value_edit()),
            command(
                "secret-load-more",
                "document.secret_store.load_more",
                Command::ResultsNextPage,
            )
            .enabled(self.listing.has_more()),
            command(
                "secret-refresh",
                "document.secret_store.toolbar.refresh",
                Command::RefreshSchema,
            )
            .icon(AppIcon::RefreshCcw),
        ];

        // Each copy entry names the selection it acts on: Copy name for a
        // secret, Copy path for a sub-path. The entry is only offered when a
        // selection of its kind exists.
        if self.selected_summary().is_some() {
            actions.push(callback_action(
                &target,
                "secret-copy-name",
                "document.secret_store.context_menu.copy_name",
                Self::copy_selected_name,
            ));
        }

        if self.selected.is_some() {
            let target = target.clone();
            actions.push(PaneAction::callback(
                "secret-details-close",
                dbflux_i18n::t!("document.secret_store.details.close"),
                move |window, cx| {
                    if let Some(doc) = target.upgrade() {
                        doc.update(cx, |doc, cx| doc.close_details(window, cx));
                    }
                },
            ));
        }

        if matches!(self.selected, Some(SecretRowKey::Path(_))) {
            actions.push(callback_action(
                &target,
                "secret-copy-path",
                "document.secret_store.context_menu.copy_path",
                Self::copy_selected_path,
            ));
        }

        if self.hierarchical() {
            actions.push(
                command(
                    "secret-path-mode",
                    "document.secret_store.path_mode.toggle",
                    Command::CycleDocumentView,
                )
                .icon(AppIcon::Folder),
            );
            actions.push(
                command(
                    "secret-path-up",
                    "document.secret_store.path.up",
                    Command::ColumnLeft,
                )
                .enabled(self.path.is_some()),
            );
        }

        // Each version of the history is a clickable row, and Back to current
        // a button of the value section: both reachable from the menu.
        if let VersionsState::Loaded(versions) = &self.versions {
            for (index, version) in versions.iter().enumerate() {
                let retrieve_target = target.clone();
                let history_generation = self.versions_generation;
                actions.push(
                    PaneAction::callback(
                        format!("secret-version-retrieve-{index}"),
                        dbflux_i18n::t!(
                            "document.secret_store.versions.retrieve",
                            version = details::short_version_id(&version.id)
                        ),
                        move |_window, cx| {
                            if let Some(doc) = retrieve_target.upgrade() {
                                doc.update(cx, |doc, cx| {
                                    // The menu was built over another history.
                                    if doc.versions_generation == history_generation {
                                        doc.activate_version(index, cx);
                                    }
                                });
                            }
                        },
                    )
                    .icon(AppIcon::History)
                    .enabled(self.can_retrieve_value()),
                );

                // Roll back is a button on every version that is not
                // current, so each one has its own entry.
                if self.supports_roll_back() && !self.is_current_version(&version.id) {
                    let roll_back_target = target.clone();
                    let history_generation = self.versions_generation;
                    let mut roll_back = PaneAction::callback(
                        format!("secret-version-rollback-{index}"),
                        dbflux_i18n::t!(
                            "document.secret_store.rollback.action",
                            version = details::short_version_id(&version.id)
                        ),
                        move |window, cx| {
                            if let Some(doc) = roll_back_target.upgrade() {
                                doc.update(cx, |doc, cx| {
                                    if doc.versions_generation == history_generation {
                                        doc.open_roll_back(index, window, cx);
                                    }
                                });
                            }
                        },
                    )
                    .icon(AppIcon::RotateCcw)
                    .enabled(self.can_roll_back_version(index));
                    roll_back.shortcut = dbflux_ui_base::keymap::shortcut_label(
                        ContextId::Results,
                        Command::RollBackVersion,
                    )
                    .filter(|_| self.previous_version_index() == Some(index));
                    actions.push(roll_back);
                }
            }
        }

        // Registered whatever the history is doing: the banner's button is
        // drawn from the value, not from the history, so it needs its
        // keyboard path even while the history loads or fails.
        if self.store.supports(SecretStoreCapabilities::VERSIONS) {
            actions.push(
                callback_action(
                    &target,
                    "secret-version-current",
                    "document.secret_store.versions.back_to_current",
                    Self::back_to_current_version,
                )
                .icon(AppIcon::RotateCcw)
                .enabled(self.can_return_to_current()),
            );
        }

        // The Formatted / Raw switch is a clickable control of the editor, so
        // the pane actions are its keyboard path too.
        if self.can_toggle_edit_mode() {
            let target = target.clone();
            actions.push(
                crate::pane::PaneAction::callback(
                    "secret-editor-mode",
                    dbflux_i18n::t!("document.secret_store.edit.mode.toggle"),
                    move |window, cx| {
                        if let Some(doc) = target.upgrade() {
                            doc.update(cx, |doc, cx| doc.cycle_edit_mode(window, cx));
                        }
                    },
                )
                .enabled(self.can_toggle_edit_mode()),
            );
        }

        if self.is_editing_value() {
            let target = target.clone();
            actions.push(
                crate::pane::PaneAction::callback(
                    "secret-editor-find",
                    dbflux_i18n::t!("document.secret_store.edit.find"),
                    move |window, cx| {
                        if let Some(doc) = target.upgrade() {
                            doc.update(cx, |doc, cx| doc.find_in_value(window, cx));
                        }
                    },
                )
                .icon(AppIcon::Search),
            );
        }

        // Edit value opens the editor of a plaintext value, retrieving the
        // value first when it is not loaded; the action needs the window the
        // editor's state is built with. The bottom bar's button and the row
        // menu's entry are covered by it.
        if self.can_request_edit_value() || self.can_edit_value() {
            let target = target.clone();
            let mut edit_value = crate::pane::PaneAction::callback(
                "secret-edit-value",
                dbflux_i18n::t!("document.secret_store.edit.open"),
                move |window, cx| {
                    if let Some(doc) = target.upgrade() {
                        doc.update(cx, |doc, cx| doc.request_edit_value(window, cx));
                    }
                },
            )
            .icon(AppIcon::Pencil)
            .enabled(self.can_request_edit_value());
            edit_value.shortcut =
                dbflux_ui_base::keymap::shortcut_label(ContextId::Results, Command::EditValue);
            actions.push(edit_value);
        }

        // Delete asks first; Restore cancels a scheduled deletion. Both are
        // buttons of the details pane, so both have an entry here.
        if self.supports_delete() && matches!(self.selected, Some(SecretRowKey::Secret(_))) {
            let target = target.clone();
            let mut delete = PaneAction::callback(
                "secret-delete",
                dbflux_i18n::t!("document.secret_store.delete.button"),
                move |window, cx| {
                    if let Some(doc) = target.upgrade() {
                        doc.update(cx, |doc, cx| doc.open_delete(window, cx));
                    }
                },
            )
            .icon(AppIcon::Delete)
            .enabled(self.can_delete_selected());
            delete.shortcut =
                dbflux_ui_base::keymap::shortcut_label(ContextId::Results, Command::Delete);
            actions.push(delete);
        }

        // New secret / New parameter is a button of the toolbar.
        if self.supports_create() {
            let create_target = target.clone();
            actions.push(
                PaneAction::callback(
                    "secret-new",
                    dbflux_i18n::t!(if self.hierarchical() {
                        "document.secret_store.create.button_parameter"
                    } else {
                        "document.secret_store.create.button_secret"
                    }),
                    move |window, cx| {
                        if let Some(doc) = create_target.upgrade() {
                            doc.update(cx, |doc, cx| doc.open_create(window, cx));
                        }
                    },
                )
                .icon(AppIcon::Plus)
                .enabled(self.can_open_create()),
            );
        }

        // Edit tags and Edit description are buttons of the Tags and Details
        // sections (the description's of the bottom bar on a store that
        // rewrites the value for it).
        if matches!(self.selected, Some(SecretRowKey::Secret(_))) {
            if self.supports_tags() {
                let tags_target = target.clone();
                actions.push(
                    PaneAction::callback(
                        "secret-edit-tags",
                        dbflux_i18n::t!("document.secret_store.tags_edit.button"),
                        move |window, cx| {
                            if let Some(doc) = tags_target.upgrade() {
                                doc.update(cx, |doc, cx| doc.open_edit_tags(window, cx));
                            }
                        },
                    )
                    .icon(AppIcon::Tag)
                    .enabled(self.can_edit_tags()),
                );
            }

            if self.supports_description_edit() {
                let description_target = target.clone();
                actions.push(
                    PaneAction::callback(
                        "secret-edit-description",
                        dbflux_i18n::t!("document.secret_store.description_edit.button"),
                        move |window, cx| {
                            if let Some(doc) = description_target.upgrade() {
                                doc.update(cx, |doc, cx| doc.open_edit_description(window, cx));
                            }
                        },
                    )
                    .icon(AppIcon::Pencil)
                    .enabled(self.can_edit_description()),
                );
            }
        }

        // Rotate now and Edit rotation are buttons of the Rotation section,
        // the bottom bar and the row menu.
        if self.supports_rotation() && matches!(self.selected, Some(SecretRowKey::Secret(_))) {
            actions.push(
                callback_action(
                    &target,
                    "secret-rotate-now",
                    "document.secret_store.rotate.button",
                    Self::rotate_selected_now,
                )
                .icon(AppIcon::Zap)
                .enabled(self.can_start_rotation_action()),
            );

            let edit_target = target.clone();
            actions.push(
                PaneAction::callback(
                    "secret-edit-rotation",
                    dbflux_i18n::t!("document.secret_store.rotation_edit.button"),
                    move |window, cx| {
                        if let Some(doc) = edit_target.upgrade() {
                            doc.update(cx, |doc, cx| doc.open_edit_rotation(window, cx));
                        }
                    },
                )
                .icon(AppIcon::Pencil)
                .enabled(self.can_start_rotation_action()),
            );
        }

        if self.supports_restore()
            && self
                .selected_summary()
                .is_some_and(|summary| summary.is_pending_deletion())
        {
            actions.push(
                callback_action(
                    &target,
                    "secret-restore",
                    "document.secret_store.restore.button",
                    Self::restore_selected,
                )
                .icon(AppIcon::RotateCcw)
                .enabled(self.can_restore_selected()),
            );
        }

        // The region picker is a clickable toolbar control, so the pane
        // actions are its keyboard path too.
        if self.region_switchable
            && let Some(dropdown) = self.region_dropdown.clone()
        {
            actions.push(
                PaneAction::callback(
                    "secret-region",
                    dbflux_i18n::t!("document.secret_store.region.label"),
                    move |window, cx| {
                        dropdown.update(cx, |dropdown, cx| dropdown.focus_and_open(window, cx));
                    },
                )
                .icon(AppIcon::Globe)
                .enabled(
                    matches!(&self.regions, RegionListState::Loaded(regions) if !regions.is_empty()),
                ),
            );
        }

        actions
    }

    // -- Command dispatch ------------------------------------------------

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        // The dialog handles its own keys; nothing reaches the listing under
        // it.
        if self.dialog.is_some() {
            return true;
        }

        if self.context_menu.is_some() {
            return self.dispatch_menu_command(cmd, window, cx);
        }

        match cmd {
            Command::SelectNext => self.move_selection(1, cx),
            Command::SelectPrev => self.move_selection(-1, cx),
            Command::SelectFirst => self.select_edge(false, cx),
            Command::SelectLast => self.select_edge(true, cx),
            Command::Execute => self.activate_selected(cx),
            Command::ToggleValuePanel => self.retrieve_selected_value(cx),
            Command::SaveQuery if self.value_edit.is_some() => self.save_value_edit(cx),
            Command::CycleResultView if self.value_edit.is_some() => {
                self.cycle_edit_mode(window, cx)
            }
            Command::ExpandCollapse => self.toggle_reveal_all(cx),
            Command::ResultsCopyCell => self.copy_resource_id(cx),
            Command::ResultsCopyRow => self.copy_full_value(cx),
            Command::CycleResultView => self.cycle_value_view(cx),
            Command::CycleDocumentView if self.hierarchical() => self.toggle_path_mode(cx),
            Command::ColumnLeft if self.hierarchical() => self.navigate_up(cx),
            Command::ResultsNextPage => self.load_more(cx),
            Command::EditValue if self.can_edit_value() => {
                self.start_value_edit(window, cx);
            }
            Command::RollBackVersion if self.supports_roll_back() => {
                self.roll_back_to_previous(window, cx);
            }
            Command::Delete
                if self.supports_delete()
                    && matches!(self.selected, Some(SecretRowKey::Secret(_))) =>
            {
                self.open_delete(window, cx);
            }
            Command::RefreshSchema => self.refresh(cx),
            Command::FocusSearch if self.value_edit.as_ref().is_some_and(|edit| edit.focused) => {
                self.find_in_value(window, cx)
            }
            Command::FocusSearch | Command::FocusToolbar => self.focus_search(window, cx),
            Command::Cancel => {
                self.focus_mode = SecretFocusMode::Table;
                self.focus_handle.focus(window, cx);
                cx.notify();
            }
            _ => return false,
        }

        true
    }
}

/// A pane action that runs `run` on the document, for an action no command
/// covers.
fn callback_action(
    target: &WeakEntity<SecretStoreDocument>,
    id: &'static str,
    key: &str,
    run: fn(&mut SecretStoreDocument, &mut Context<SecretStoreDocument>),
) -> crate::pane::PaneAction {
    let target = target.clone();

    crate::pane::PaneAction::callback(id, dbflux_i18n::t!(key), move |_window, cx| {
        if let Some(doc) = target.upgrade() {
            doc.update(cx, run);
        }
    })
}

fn copied_toast(message: String, cx: &mut App) {
    dbflux_ui_base::toast::Toast::success(message)
        .meta_right(dbflux_ui_base::toast::now_hms())
        .push(cx);
}

#[cfg(test)]
mod tests;
