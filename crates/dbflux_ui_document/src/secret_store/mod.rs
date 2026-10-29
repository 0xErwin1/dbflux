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

mod data;
mod details;
mod listing;
mod pane;
mod region;
mod render;
mod value;

pub use listing::{PathMode, SecretListing};
pub use region::RegionListState;
pub use value::{
    RetrievalTarget, RetrievedValue, VALUE_RETRIEVE_FAILED_ACTION, VALUE_RETRIEVED_ACTION,
    ValueContent, ValueFailure, ValueState, ValueView, encode_base64, value_retrieval_event,
};

use super::handle::DocumentEvent;
use super::types::{DocumentId, DocumentState};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::controls::{Dropdown, DropdownSelectionChanged, InputEvent, InputState};
use dbflux_core::secrecy::ExposeSecret;
use dbflux_core::{
    Connection, DriverCapabilities, RefreshPolicy, SecretDetails, SecretStoreCapabilities,
    SecretStoreDescriptor, SecretSummary,
};
use dbflux_ui_base::AppStateEntity;
use gpui::*;
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

/// The last store call and how long it took, for the status bar.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreCall {
    List { count: usize },
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
    value: ValueState,
    value_generation: u64,
    /// One read-only, secret-marked input per drawn value field, built on
    /// the render pass after a retrieval (an input needs a `Window`).
    value_inputs: Vec<Entity<InputState>>,
    /// The masked flag last applied to each of `value_inputs`.
    value_input_masks: Vec<bool>,
    value_inputs_view: Option<ValueView>,
    last_call: Option<CallTiming>,
    /// Whether the connection's driver advertises `REGION_SWITCH`. Never a
    /// driver id: the capability gates the control.
    region_switchable: bool,
    /// The region the listing is loaded under, read from the connection at
    /// construction and updated after a successful switch.
    region: Option<String>,
    /// The regions the connection offers.
    regions: RegionListState,
    /// A region switch is reconnecting on the background executor.
    region_switch_in_flight: bool,
    /// The toolbar region picker, built only when the driver can switch.
    region_dropdown: Option<Entity<Dropdown>>,
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
            value: ValueState::NotLoaded,
            value_generation: 0,
            value_inputs: Vec::new(),
            value_input_masks: Vec::new(),
            value_inputs_view: None,
            last_call: None,
            region_switchable,
            region,
            regions: RegionListState::Idle,
            region_switch_in_flight: false,
            region_dropdown,
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

    pub fn state(&self) -> DocumentState {
        if self.listing.loading {
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
    }

    pub fn set_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        self.refresh_policy = policy;
        cx.notify();
    }

    pub fn active_context(&self) -> ContextId {
        if self.focus_mode == SecretFocusMode::Search {
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

        self.clear_value();
        self.details_generation = self.details_generation.wrapping_add(1);
        self.details = DetailsState::Idle;

        if let SecretRowKey::Secret(name) = &row {
            let name = name.clone();
            self.selected = Some(row);
            self.describe(name, cx);
        } else {
            self.selected = Some(row);
        }

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

    /// Drops the retrieved value and the inputs that draw it.
    pub fn clear_value(&mut self) {
        self.value_generation = self.value_generation.wrapping_add(1);
        self.value = ValueState::NotLoaded;
        self.value_inputs.clear();
        self.value_input_masks.clear();
        self.value_inputs_view = None;
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

    /// Copies the selected secret's provider-wide identifier (an ARN).
    pub fn copy_resource_id(&mut self, cx: &mut Context<Self>) {
        let DetailsState::Loaded(details) = &self.details else {
            return;
        };

        let Some(resource_id) = details.resource_id.clone() else {
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

        actions
    }

    // -- Command dispatch ------------------------------------------------

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match cmd {
            Command::SelectNext => self.move_selection(1, cx),
            Command::SelectPrev => self.move_selection(-1, cx),
            Command::SelectFirst => self.select_edge(false, cx),
            Command::SelectLast => self.select_edge(true, cx),
            Command::Execute => self.activate_selected(cx),
            Command::ToggleValuePanel => self.retrieve_selected_value(cx),
            Command::ExpandCollapse => self.toggle_reveal_all(cx),
            Command::ResultsCopyCell => self.copy_resource_id(cx),
            Command::ResultsCopyRow => self.copy_full_value(cx),
            Command::CycleResultView => self.cycle_value_view(cx),
            Command::CycleDocumentView if self.hierarchical() => self.toggle_path_mode(cx),
            Command::ColumnLeft if self.hierarchical() => self.navigate_up(cx),
            Command::ResultsNextPage => self.load_more(cx),
            Command::RefreshSchema => self.refresh(cx),
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
