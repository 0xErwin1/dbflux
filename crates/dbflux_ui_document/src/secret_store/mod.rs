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

pub(crate) mod changes;
mod context_menu;
mod create;
mod data;
mod list;
mod listing;
mod pane;
mod pane_actions;
mod region;
mod render;
mod sync;
mod value;
mod view;
mod write;

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
use dbflux_core::{
    DriverCapabilities, RefreshPolicy, SecretStoreCapabilities, SecretStoreDescriptor,
};
use dbflux_ui_base::AppStateChanged;
use dbflux_ui_base::AppStateEntity;
use gpui::*;
use std::cell::Cell;
use std::rc::Rc;
use uuid::Uuid;
pub(crate) use view::{DetailsState, SecretView, VersionsState};
pub(crate) use view::{SecretViewEvent, ViewHost, WriteEffect, land_save_answer};

/// A request to open one secret in its own tab, raised by the listing and
/// drained by the workspace. The region is the one the listing is loaded
/// under: names are region-scoped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretOpenRequest {
    pub store_id: String,
    pub name: String,
    pub version: Option<String>,
    pub region: Option<String>,
    /// Open with the value editor active: the tab retrieves the value first
    /// when it is not loaded, and never only reveals it.
    pub edit: bool,
}

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
    /// Everything that belongs to the selected secret: details, value,
    /// history, the editor and the writes. The document tells it which row
    /// is selected and reacts to what it reports.
    view: Entity<SecretView>,
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
    /// A region switch is reconnecting on the background executor. Shared
    /// with the view, whose writes wait for the switch.
    region_switch_in_flight: Rc<Cell<bool>>,
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
    /// Clears the search field on the next render, which has the window.
    clear_search_input: bool,
    /// The New secret dialog's fields, while its slot in the view is open.
    create_dialog: Option<create::CreateDialog>,
    /// What the listing showed when the create in flight was sent, so its
    /// landing can tell that the user moved on.
    create_origin: Option<create::ListingView>,
    /// The secret the user asked to open in its own tab, drained by the
    /// workspace on its next render.
    pending_open_secret: Option<SecretOpenRequest>,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<DocumentEvent> for SecretStoreDocument {}

/// What the document reads from the live connection when it opens.
pub(crate) struct ConnectionFacts {
    pub(crate) name: String,
    pub(crate) descriptor: SecretStoreDescriptor,
    pub(crate) region: Option<String>,
    pub(crate) region_switchable: bool,
}

impl ConnectionFacts {
    /// A connection without the store yields a descriptor that supports
    /// nothing, so the document opens empty with an error.
    pub(crate) fn read(
        app_state: &Entity<AppStateEntity>,
        profile_id: Uuid,
        store_id: &str,
        cx: &App,
    ) -> Self {
        let state = app_state.read(cx);
        let connected = state.connections().get(&profile_id);

        let name = connected
            .map(|connected| connected.profile.name.clone())
            .unwrap_or_default();

        let descriptor = connected
            .and_then(|connected| {
                connected
                    .connection
                    .secret_store(store_id)
                    .map(|store| store.descriptor().clone())
            })
            .unwrap_or_else(|| {
                SecretStoreDescriptor::new(
                    store_id.to_string(),
                    store_id.to_string(),
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

        Self {
            name,
            descriptor,
            region,
            region_switchable,
        }
    }
}

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
        let ConnectionFacts {
            name: connection_name,
            descriptor,
            region,
            region_switchable,
        } = ConnectionFacts::read(&app_state, profile_id, &store_id, cx);

        let (search_input, search_subscription) = Self::build_search_input(window, cx);
        let path = path.filter(|_| descriptor.supports(SecretStoreCapabilities::HIERARCHICAL));

        // The picker exists only when the driver can switch regions; its
        // items arrive once the background listing lands.
        let (region_dropdown, region_subscription) = if region_switchable {
            let (dropdown, subscription) = Self::build_region_dropdown(window, cx);

            (Some(dropdown), Some(subscription))
        } else {
            (None, None)
        };

        let mut subscriptions = vec![search_subscription];
        subscriptions.extend(region_subscription);
        subscriptions.push(Self::watch_app_state(&app_state, cx));

        let region_switch_in_flight = Rc::new(Cell::new(false));
        let origin = cx.entity_id();
        let view = cx.new(|cx| {
            let mut view = SecretView::new(
                profile_id,
                descriptor.clone(),
                app_state.clone(),
                region_switch_in_flight.clone(),
                ViewHost::Pane,
                cx,
            );
            view.set_change_origin(origin);
            view
        });
        // The view redraws itself on its own state; the document draws it.
        subscriptions.push(cx.observe(&view, |_, _, cx| cx.notify()));
        subscriptions.push(cx.subscribe_in(&view, window, Self::on_view_event));
        subscriptions.push(changes::follow(cx, Self::on_secret_change));

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
            view,
            last_call: None,
            footer_fit: Default::default(),
            region_switchable,
            region,
            regions: RegionListState::Idle,
            regions_generation: 0,
            region_switch_in_flight,
            region_dropdown,
            context_menu: None,
            pending_menu_action: None,
            context_menu_focus: cx.focus_handle(),
            clear_search_input: false,
            create_dialog: None,
            create_origin: None,
            pending_open_secret: None,
            _subscriptions: subscriptions,
        };

        if region_switchable {
            doc.load_regions(cx);
        }

        doc.load_first_page(cx);
        doc
    }

    fn build_search_input(
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> (Entity<InputState>, Subscription) {
        let search_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("document.secret_store.search_placeholder"))
        });

        let subscription = cx.subscribe_in(
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

        (search_input, subscription)
    }

    fn build_region_dropdown(
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> (Entity<Dropdown>, Subscription) {
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

        (dropdown, subscription)
    }

    /// The live connection can be replaced (or moved to another region)
    /// outside this document; the subscription keeps the region and the
    /// listing on what the connection actually reaches.
    fn watch_app_state(app_state: &Entity<AppStateEntity>, cx: &mut Context<Self>) -> Subscription {
        cx.subscribe(app_state, |this, _, _: &AppStateChanged, cx| {
            this.view
                .update(cx, |view, cx| view.begin_connection_change(cx));
            this.adopt_connection_region(cx);
            this.view
                .update(cx, |view, cx| view.finish_connection_change(cx));
        })
    }

    /// The view of the selected secret, for the cross-host tests.
    #[cfg(test)]
    pub(crate) fn view(&self) -> &Entity<SecretView> {
        &self.view
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
        if self.view.read(cx).has_unsaved_value_edits(cx) {
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
        self.region_switch_in_flight.get()
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

    pub fn active_context(&self, cx: &App) -> ContextId {
        let view = self.view.read(cx);

        // A dialog owns the keyboard: its inputs keep every letter the
        // listing would otherwise read as a command, and its buttons answer
        // Enter and Escape through the dialog itself.
        if view.dialog_is_open() {
            return ContextId::TextInput;
        }

        if self.context_menu.is_some() {
            return ContextId::ContextMenu;
        }

        if self.focus_mode == SecretFocusMode::Search || view.value_editor_focused() {
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
    /// Whether the open editor holds unsaved text, which a move of the
    /// selection or of the listing would drop. Refuses and says why.
    fn block_unsaved_value_edits(&self, cx: &mut Context<Self>) -> bool {
        self.view
            .update(cx, |view, cx| view.block_unsaved_value_edits(cx))
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

        if let SecretRowKey::Secret(name) = &row {
            let name = name.clone();
            let listing_row = self.listing.find(&name).cloned();
            self.selected = Some(row);
            self.view
                .update(cx, |view, cx| view.show_secret(name, listing_row, cx));
        } else {
            self.view.update(cx, |view, cx| view.show_other_row(cx));
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
        if self.view.read(cx).value_editor_focused() {
            self.focus_handle.focus(window, cx);
        }

        self.view.update(cx, |view, cx| view.clear_selection(cx));
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

    /// Copies the selected sub-path. The one implementation behind the
    /// context menu's Copy path entry and the pane action `m` reaches.
    pub fn copy_selected_path(&mut self, cx: &mut Context<Self>) {
        let Some(SecretRowKey::Path(path)) = self.selected.as_ref() else {
            return;
        };

        let path = path.clone();
        cx.write_to_clipboard(ClipboardItem::new_string(path));
        view::copied_toast(
            dbflux_i18n::t!("document.secret_store.toast.path_copied"),
            cx,
        );
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
        if self.view.read(cx).dialog_is_open() {
            return true;
        }

        if self.context_menu.is_some() {
            return self.dispatch_menu_command(cmd, window, cx);
        }

        let editor_focused = self.view.read(cx).value_editor_focused();

        match cmd {
            Command::SelectNext => self.move_selection(1, cx),
            Command::SelectPrev => self.move_selection(-1, cx),
            Command::SelectFirst => self.select_edge(false, cx),
            Command::SelectLast => self.select_edge(true, cx),
            Command::Execute => self.activate_selected(cx),
            Command::CycleDocumentView if self.hierarchical() => self.toggle_path_mode(cx),
            Command::ColumnLeft if self.hierarchical() => self.navigate_up(cx),
            Command::ResultsNextPage => self.load_more(cx),
            Command::RefreshSchema => self.refresh(cx),
            Command::FocusSearch if !editor_focused => self.focus_search(window, cx),
            Command::FocusToolbar => self.focus_search(window, cx),
            Command::Cancel => {
                self.focus_mode = SecretFocusMode::Table;
                self.focus_handle.focus(window, cx);
                cx.notify();
            }
            _ => {
                return self
                    .view
                    .update(cx, |view, cx| view.dispatch_command(cmd, window, cx));
            }
        }

        true
    }

    /// Reacts to what the selected secret's view reports.
    fn on_view_event(
        &mut self,
        _view: &Entity<SecretView>,
        event: &SecretViewEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            SecretViewEvent::ReloadListing => self.reload_listing_in_place(cx),
            // The listing has no close or quit flow of a secret to report.
            SecretViewEvent::SaveFinished { .. } => {}
            SecretViewEvent::StoreCalled(timing) => self.last_call = Some(*timing),
            SecretViewEvent::Removed => self.selected = None,
            SecretViewEvent::DialogOpened => self.drop_context_menu(),
            SecretViewEvent::DialogClosed => {
                // The event is delivered after the update that closed the
                // dialog; a New secret form opened since then is not the one
                // it reports.
                if !self.view.read(cx).create_slot_is_open() {
                    self.drop_create_dialog();
                }
            }
            SecretViewEvent::CloseRequested => self.close_details(window, cx),
            SecretViewEvent::OpenInTab { name, edit } => {
                self.queue_open_secret(name.clone(), *edit, cx)
            }
            SecretViewEvent::Created { name, stands } => {
                self.land_create(name, *stands, cx);
                self.announce(name.clone(), changes::SecretChangeKind::Created, cx);
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
