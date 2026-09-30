mod bulk_delete;
mod collection_panes;
mod commands;
mod console;
mod context_menu;
mod copy_command;
pub(super) mod decode;
mod document_view;
mod expiry;
mod key_tree;
mod metadata;
mod mutations;
mod pagination;
mod pane;
pub(super) mod parsing;
mod render;
mod render_list;
mod render_value;
pub(super) mod view;

#[cfg(test)]
mod keyboard_tests;

// Re-export sibling `document/` modules so submodules can use `super::*_modal`.
use super::add_member_modal;
use super::handle::DocumentEvent;
use super::new_key_modal;

use super::add_member_modal::{AddMemberEvent, AddMemberModal};
use super::new_key_modal::{NewKeyCreatedEvent, NewKeyModal};
use super::task_runner::DocumentTaskRunner;
use super::types::{DocumentId, DocumentState};
use bulk_delete::BulkDeleteState;
use collection_panes::{StreamPane, ZSetPane};
use context_menu::KvContextMenu;
use dbflux_app::keymap::ContextId;
use dbflux_components::components::document_tree::{DocumentTree, DocumentTreeState};
use dbflux_components::controls::{
    ButtonVariant, Dropdown, DropdownItem, DropdownSelectionChanged,
};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_core::{
    DriverCapabilities, KeyEntry, KeyGetResult, KeyMetadata, KeyType, KeyValueFeatures,
    RefreshPolicy,
};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use decode::{Compression, RenderedValue, ValueDetection, ViewAs};
use expiry::ExpiryEditor;
use gpui::*;
use key_tree::{KeyListLayout, KeyListRow};
use metadata::CachedKeyMetadata;
use pagination::ScanMode;
use parsing::{MemberEntry, parse_database_name};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Main document
// ---------------------------------------------------------------------------

pub struct KeyValueDocument {
    id: DocumentId,
    title: String,
    profile_id: Uuid,
    database: String,
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,

    // Filter / navigation
    filter_input: Entity<InputState>,
    members_filter_input: Entity<InputState>,
    focus_mode: KeyValueFocusMode,

    // Task runner (reads: auto-cancel-previous, mutations: independent)
    runner: DocumentTaskRunner,
    refresh_policy: RefreshPolicy,
    refresh_dropdown: Entity<Dropdown>,
    _refresh_timer: Option<Task<()>>,
    _refresh_subscriptions: Vec<Subscription>,

    is_active_tab: bool,

    // Live TTL countdown
    ttl_state: TtlState,
    ttl_display: String,
    _ttl_countdown_timer: Option<Task<()>>,

    // Loaded keys: every page the scan returned so far, without duplicates.
    keys: Vec<KeyEntry>,
    loaded_key_names: HashSet<String>,
    selected_index: Option<usize>,
    selected_value: Option<KeyGetResult>,
    last_error: Option<String>,

    // Optional operations of this connection's key-value API.
    key_features: KeyValueFeatures,

    // Key list layout: rows built from `keys`, the namespace tree state and
    // the keyboard cursor over the rows.
    list_layout: KeyListLayout,
    key_delimiter: String,
    expanded_folders: HashSet<String>,
    key_rows: Vec<KeyListRow>,
    list_cursor: Option<usize>,
    key_list_scroll: UniformListScrollHandle,
    type_filter: Option<KeyType>,

    // Scan progress. `scan_cursor` is where the next page resumes; it is
    // `None` before the first page and once the keyspace is fully read.
    scan_cursor: Option<String>,
    scan_started: bool,
    scanned_keys: u64,
    scan_mode: ScanMode,
    /// Bumped on every reload so late pages and metadata are dropped.
    scan_generation: u64,
    /// Keys in the whole keyspace (`DBSIZE`), when the driver can count.
    key_total: Option<u64>,

    // TTL and size of listed keys, fetched for the rows on screen only.
    key_metadata: HashMap<String, CachedKeyMetadata>,
    metadata_in_flight: HashSet<String>,
    /// Encoding and memory size of the open key.
    value_metadata: Option<KeyMetadata>,

    // Size gate and "View as" state of the open string value.
    /// One-shot override for the next `reload_selected_value` call: fetches
    /// the value unbounded instead of applying the configured size limit.
    kv_load_anyway: bool,
    value_view_as: ViewAs,
    value_compression: Compression,
    value_detection: ValueDetection,
    rendered_value: Option<RenderedValue>,
    /// Bumped every time the open value or its view changes, so a background
    /// render that finishes after the user moved on is dropped.
    value_render_generation: u64,
    compression_dropdown: Entity<Dropdown>,
    _compression_dropdown_subscription: Subscription,

    // Ranged panes for sorted sets and streams.
    zset_pane: Option<ZSetPane>,
    stream_pane: Option<StreamPane>,
    /// A stream key was opened where no window was at hand; the pane is
    /// built on the next render.
    pending_stream_pane: bool,

    // Expiry editor, bulk delete and console.
    expiry_editor: Option<ExpiryEditor>,
    bulk_delete: Option<BulkDeleteState>,
    bulk_delete_generation: u64,
    pending_bulk_delete_input: bool,
    bulk_actions_open: bool,
    console: Option<Entity<crate::console::NativeConsole>>,
    _console_subscription: Option<Subscription>,

    // Inline rename
    rename_input: Option<Entity<InputState>>,
    renaming_index: Option<usize>,

    // Inline member editing
    editing_member_index: Option<usize>,
    member_edit_input: Option<Entity<InputState>>,
    member_edit_score_input: Option<Entity<InputState>>,

    // Member navigation (when ValuePanel is focused)
    selected_member_index: Option<usize>,

    // Cached members for optimistic UI (parsed from selected_value, updated locally)
    cached_members: Vec<MemberEntry>,

    // Inline string/JSON value editing
    string_edit_input: Option<Entity<InputState>>,

    // Delete confirmations
    pending_key_delete: Option<PendingKeyDelete>,
    pending_member_delete: Option<PendingMemberDelete>,

    // New Key modal
    new_key_modal: Entity<NewKeyModal>,
    pending_open_new_key_modal: bool,

    // Add Member modal (Hash/Stream multi-field)
    add_member_modal: Entity<AddMemberModal>,
    pending_open_add_member_modal: Option<KeyType>,

    // Document view mode for Hash/Stream
    value_view_mode: KvValueViewMode,
    document_tree_state: Option<Entity<DocumentTreeState>>,
    document_tree: Option<Entity<DocumentTree>>,
    _document_tree_subscription: Option<Subscription>,

    // Context menu
    context_menu: Option<KvContextMenu>,
    /// Menu item chosen with the mouse, run on the next render with a window.
    pending_menu_action: Option<(context_menu::KvMenuAction, context_menu::KvMenuTarget)>,
    context_menu_focus: FocusHandle,
    panel_origin: Point<Pixels>,

    _subscriptions: Vec<Subscription>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyValueFocusMode {
    /// Key list navigation (vim keys active).
    List,
    /// Right-side value/members panel (vim keys active).
    ValuePanel,
    /// Any text input is focused (filter, rename, member edit, add member).
    TextInput,
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub(super) enum KvValueViewMode {
    #[default]
    Table,
    Document,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum TtlState {
    NoLimit,
    Remaining { deadline: Instant },
    Expired,
    Missing,
}

pub(super) struct PendingKeyDelete {
    pub key: String,
    pub index: usize,
}

pub(super) struct PendingMemberDelete {
    pub member_index: usize,
    pub member_display: String,
}

impl EventEmitter<DocumentEvent> for KeyValueDocument {}

// ---------------------------------------------------------------------------
// Lifecycle, public API, and navigation helpers
// ---------------------------------------------------------------------------

impl KeyValueDocument {
    pub fn new(
        profile_id: Uuid,
        database: String,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let filter_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.key_value.render.filter.keys_placeholder"
            ))
        });
        let members_filter_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.key_value.render.filter.members_placeholder"
            ))
        });
        let mut subscriptions = Vec::new();

        subscriptions.push(cx.subscribe_in(
            &filter_input,
            window,
            |this, _, event: &InputEvent, _window, cx| {
                if let InputEvent::PressEnter { .. } = event {
                    this.reload_keys(cx);
                }
            },
        ));

        subscriptions.push(cx.subscribe_in(
            &members_filter_input,
            window,
            |_, _, event: &InputEvent, _, cx| {
                if matches!(event, InputEvent::Change) {
                    cx.notify();
                }
            },
        ));

        let new_key_modal = cx.new(|cx| NewKeyModal::new(window, cx));

        subscriptions.push(cx.subscribe(
            &new_key_modal,
            |this: &mut Self, _, event: &NewKeyCreatedEvent, cx| {
                this.handle_new_key_created(event.clone(), cx);
            },
        ));

        let add_member_modal = cx.new(AddMemberModal::new);

        let default_refresh = app_state
            .read(cx)
            .effective_settings_for_connection(Some(profile_id))
            .resolve_refresh_policy();

        let refresh_dropdown = cx.new(|_cx| {
            let items = RefreshPolicy::ALL
                .iter()
                .map(|policy| DropdownItem::new(crate::labels::refresh_policy_label(*policy)))
                .collect();

            Dropdown::new("kv-auto-refresh")
                .items(items)
                .selected_index(Some(default_refresh.index()))
                .chevron_trigger(ButtonVariant::Secondary)
        });

        let refresh_policy_sub = cx.subscribe_in(
            &refresh_dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, _window, cx| {
                let policy = RefreshPolicy::from_index(event.index);
                this.set_refresh_policy(policy, cx);
            },
        );

        subscriptions.push(cx.subscribe(
            &add_member_modal,
            |this: &mut Self, _, event: &AddMemberEvent, cx| {
                this.handle_add_member_event(event.clone(), cx);
            },
        ));

        let compression_dropdown = cx.new(|_cx| {
            let items = Compression::ALL
                .iter()
                .map(|compression| DropdownItem::new(compression.label()))
                .collect();

            Dropdown::new("kv-compression")
                .items(items)
                .selected_index(Some(Compression::None.index()))
        });

        let compression_dropdown_subscription = cx.subscribe(
            &compression_dropdown,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.set_value_compression(Compression::from_index(event.index), cx);
            },
        );

        let key_delimiter = key_tree::key_delimiter_from_setting(
            app_state
                .read(cx)
                .effective_settings_for_connection(Some(profile_id))
                .driver_values
                .get("key_delimiter")
                .map(String::as_str),
        );

        let key_features = app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .and_then(|connected| {
                connected
                    .connection
                    .key_value_api()
                    .map(|api| api.features())
            })
            .unwrap_or_default();

        let (console, console_subscription) =
            Self::build_console(profile_id, &database, app_state.clone(), window, cx).unzip();

        let title = {
            let state = app_state.read(cx);
            let profile_name = state
                .profiles()
                .iter()
                .find(|profile| profile.id == profile_id)
                .map(|profile| profile.name.clone());

            parsing::document_title(profile_name.as_deref(), &database)
        };

        let mut doc = Self {
            id: DocumentId::new(),
            title,
            profile_id,
            database,
            app_state: app_state.clone(),
            focus_handle: cx.focus_handle(),
            runner: {
                let mut r = DocumentTaskRunner::new(app_state);
                r.set_profile_id(profile_id);
                r
            },
            refresh_policy: default_refresh,
            refresh_dropdown,
            _refresh_timer: None,
            _refresh_subscriptions: vec![refresh_policy_sub],
            is_active_tab: true,
            ttl_state: TtlState::NoLimit,
            ttl_display: String::new(),
            _ttl_countdown_timer: None,
            filter_input,
            members_filter_input,
            focus_mode: KeyValueFocusMode::List,
            keys: Vec::new(),
            loaded_key_names: HashSet::new(),
            selected_index: None,
            selected_value: None,
            last_error: None,
            key_features,
            list_layout: KeyListLayout::default(),
            key_delimiter,
            expanded_folders: HashSet::new(),
            key_rows: Vec::new(),
            list_cursor: None,
            key_list_scroll: UniformListScrollHandle::new(),
            type_filter: None,
            scan_cursor: None,
            scan_started: false,
            scanned_keys: 0,
            scan_mode: ScanMode::Page,
            scan_generation: 0,
            key_total: None,
            key_metadata: HashMap::new(),
            metadata_in_flight: HashSet::new(),
            value_metadata: None,
            kv_load_anyway: false,
            value_view_as: ViewAs::Auto,
            value_compression: Compression::None,
            value_detection: ValueDetection::default(),
            rendered_value: None,
            value_render_generation: 0,
            compression_dropdown,
            _compression_dropdown_subscription: compression_dropdown_subscription,
            zset_pane: None,
            stream_pane: None,
            pending_stream_pane: false,
            expiry_editor: None,
            bulk_delete: None,
            bulk_delete_generation: 0,
            pending_bulk_delete_input: false,
            bulk_actions_open: false,
            console,
            _console_subscription: console_subscription,
            rename_input: None,
            renaming_index: None,
            editing_member_index: None,
            member_edit_input: None,
            member_edit_score_input: None,
            selected_member_index: None,
            cached_members: Vec::new(),
            string_edit_input: None,
            pending_key_delete: None,
            pending_member_delete: None,
            new_key_modal,
            pending_open_new_key_modal: false,
            add_member_modal,
            pending_open_add_member_modal: None,
            value_view_mode: KvValueViewMode::default(),
            document_tree_state: None,
            document_tree: None,
            _document_tree_subscription: None,
            context_menu: None,
            pending_menu_action: None,
            context_menu_focus: cx.focus_handle(),
            panel_origin: Point::default(),
            _subscriptions: subscriptions,
        };

        doc.reload_keys(cx);
        doc
    }

    // -- Public API --

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        self.title.clone()
    }

    pub fn state(&self) -> DocumentState {
        if self.runner.is_primary_active() {
            DocumentState::Loading
        } else {
            DocumentState::Clean
        }
    }

    pub fn refresh_policy(&self) -> RefreshPolicy {
        self.refresh_policy
    }

    pub fn set_active_tab(&mut self, active: bool) {
        self.is_active_tab = active;
    }

    pub fn set_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        if self.refresh_policy == policy {
            return;
        }

        self.refresh_policy = policy;
        self.update_refresh_timer(cx);
        cx.notify();
    }

    fn update_refresh_timer(&mut self, cx: &mut Context<Self>) {
        self._refresh_timer = None;

        let Some(duration) = self.refresh_policy.duration() else {
            return;
        };

        self._refresh_timer = Some(cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor().timer(duration).await;

                cx.update(|cx| {
                    let Some(entity) = this.upgrade() else {
                        return;
                    };

                    entity.update(cx, |doc, cx| {
                        if !doc.refresh_policy.is_auto() || doc.runner.is_primary_active() {
                            return;
                        }

                        let settings = doc.app_state.read(cx).general_settings();

                        if settings.auto_refresh_pause_on_error && doc.last_error.is_some() {
                            return;
                        }

                        if settings.auto_refresh_only_if_visible && !doc.is_active_tab {
                            return;
                        }

                        doc.reload_keys(cx);
                    });
                });
            }
        }));
    }

    // -- Live TTL countdown --

    pub(super) fn apply_ttl_from_entry(&mut self, entry: &KeyEntry, cx: &mut Context<Self>) {
        self._ttl_countdown_timer = None;

        match entry.ttl_seconds {
            None | Some(-1) => {
                self.ttl_state = TtlState::NoLimit;
                self.ttl_display = dbflux_i18n::t!("document.key_value.render.ttl.no_limit");
            }
            Some(-2) => {
                self.ttl_state = TtlState::Missing;
                self.ttl_display = dbflux_i18n::t!("document.key_value.render.ttl.missing");
            }
            Some(0) => {
                self.ttl_state = TtlState::Expired;
                self.ttl_display = dbflux_i18n::t!("document.key_value.render.ttl.expired");
            }
            Some(secs) if secs > 0 => {
                let deadline = Instant::now() + Duration::from_secs(secs as u64);
                self.ttl_state = TtlState::Remaining { deadline };
                self.ttl_display = format!("{}s", secs);
                self.start_ttl_timer(cx);
            }
            _ => {
                self.ttl_state = TtlState::NoLimit;
                self.ttl_display = dbflux_i18n::t!("document.key_value.render.ttl.no_limit");
            }
        }
    }

    pub(super) fn clear_ttl_state(&mut self) {
        self._ttl_countdown_timer = None;
        self.ttl_state = TtlState::NoLimit;
        self.ttl_display = String::new();
    }

    fn tick_ttl(&mut self) {
        let TtlState::Remaining { deadline } = self.ttl_state else {
            return;
        };

        let remaining = deadline.saturating_duration_since(Instant::now());

        if remaining.is_zero() {
            self.ttl_state = TtlState::Expired;
            self.ttl_display = dbflux_i18n::t!("document.key_value.render.ttl.expired");
            self._ttl_countdown_timer = None;
        } else {
            self.ttl_display = format!("{}s", remaining.as_secs());
        }
    }

    fn start_ttl_timer(&mut self, cx: &mut Context<Self>) {
        self._ttl_countdown_timer = Some(cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor().timer(Duration::from_secs(1)).await;

                let should_stop = cx.update(|cx| {
                    let Some(entity) = this.upgrade() else {
                        return true;
                    };

                    entity.update(cx, |doc, cx| {
                        doc.tick_ttl();
                        cx.notify();
                        doc.ttl_state == TtlState::Expired
                    })
                });

                if should_stop {
                    break;
                }
            }
        }));
    }

    pub fn can_close(&self) -> bool {
        true
    }

    pub fn connection_id(&self) -> Option<Uuid> {
        Some(self.profile_id)
    }

    pub fn database_name(&self) -> &str {
        &self.database
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_mode = KeyValueFocusMode::List;
        self.focus_handle.focus(window, cx);
        cx.notify();
    }

    pub fn active_context(&self, cx: &App) -> ContextId {
        if self.new_key_modal.read(cx).is_visible() {
            return self.new_key_modal.read(cx).active_context();
        }

        if self.add_member_modal.read(cx).is_visible() {
            return self.add_member_modal.read(cx).active_context();
        }

        if self.bulk_delete.is_some() {
            return ContextId::TextInput;
        }

        if self.pending_key_delete.is_some() || self.pending_member_delete.is_some() {
            return ContextId::ConfirmModal;
        }

        if self.context_menu.is_some() {
            return ContextId::ContextMenu;
        }

        if self.expiry_editor.is_some() {
            return ContextId::TextInput;
        }

        if self.is_document_view_active()
            && let Some(ts) = &self.document_tree_state
            && ts.read(cx).editing_node().is_some()
        {
            return ContextId::TextInput;
        }

        match self.focus_mode {
            KeyValueFocusMode::List | KeyValueFocusMode::ValuePanel => ContextId::Results,
            KeyValueFocusMode::TextInput => ContextId::TextInput,
        }
    }

    // -- Navigation helpers --

    pub(super) fn selected_key(&self) -> Option<String> {
        self.selected_index
            .and_then(|idx| self.keys.get(idx))
            .map(|entry| entry.key.clone())
    }

    pub(super) fn selected_key_type(&self) -> Option<KeyType> {
        self.selected_value
            .as_ref()
            .and_then(|v| v.entry.key_type)
            .or_else(|| {
                self.selected_index
                    .and_then(|index| self.keys.get(index))
                    .and_then(|entry| entry.key_type)
            })
    }

    pub(super) fn keyspace_index(&self) -> Option<u32> {
        parse_database_name(&self.database)
    }

    pub(super) fn move_member_selection(&mut self, delta: isize, cx: &mut Context<Self>) {
        if self.cached_members.is_empty() {
            self.selected_member_index = None;
            cx.notify();
            return;
        }

        let current = self.selected_member_index.unwrap_or(0) as isize;
        let next = (current + delta).clamp(0, (self.cached_members.len() - 1) as isize) as usize;
        self.selected_member_index = Some(next);
        cx.notify();
    }

    /// Moves the key list cursor by `delta` rows. Landing on a key opens it;
    /// landing on a folder only highlights it.
    pub(super) fn move_selection(&mut self, delta: isize, cx: &mut Context<Self>) {
        if self.key_rows.is_empty() {
            self.list_cursor = None;
            cx.notify();
            return;
        }

        let current = self.list_cursor.unwrap_or(0) as isize;
        let last = (self.key_rows.len() - 1) as isize;
        let next = if self.list_cursor.is_none() {
            0
        } else {
            (current + delta).clamp(0, last) as usize
        };

        self.set_list_cursor(next, cx);
    }

    pub(super) fn move_selection_to_edge(&mut self, last: bool, cx: &mut Context<Self>) {
        if self.key_rows.is_empty() {
            return;
        }

        let row = if last { self.key_rows.len() - 1 } else { 0 };
        self.set_list_cursor(row, cx);
    }

    pub(super) fn set_list_cursor(&mut self, row: usize, cx: &mut Context<Self>) {
        self.list_cursor = Some(row);
        self.key_list_scroll
            .scroll_to_item(row, ScrollStrategy::Center);

        if let Some(key_index) = self.key_rows.get(row).and_then(KeyListRow::key_index) {
            self.select_index(key_index, cx);
        }

        cx.notify();
    }

    pub(super) fn select_index(&mut self, index: usize, cx: &mut Context<Self>) {
        if self.selected_index == Some(index) {
            return;
        }

        self.selected_index = Some(index);
        self.list_cursor = self
            .key_rows
            .iter()
            .position(|row| row.key_index() == Some(index))
            .or(self.list_cursor);
        self.selected_value = None;
        self.selected_member_index = None;
        self.string_edit_input = None;
        self.expiry_editor = None;
        self.value_metadata = None;
        self.zset_pane = None;
        self.stream_pane = None;
        self.clear_ttl_state();
        self.rebuild_cached_members(cx);
        self.cancel_member_edit(cx);
        cx.notify();
        self.reload_selected_value(cx);
    }

    /// Expands or collapses the folder at `prefix` in the tree layout.
    pub(super) fn toggle_folder(&mut self, prefix: &str, cx: &mut Context<Self>) {
        if !self.expanded_folders.remove(prefix) {
            self.expanded_folders.insert(prefix.to_string());
        }

        self.rebuild_key_rows();
        cx.notify();
    }

    /// The folder under the list cursor, when there is one.
    pub(super) fn folder_at_cursor(&self) -> Option<(String, bool)> {
        match self.list_cursor.and_then(|row| self.key_rows.get(row)) {
            Some(KeyListRow::Folder {
                prefix, expanded, ..
            }) => Some((prefix.clone(), *expanded)),
            _ => None,
        }
    }

    fn set_list_layout(&mut self, layout: KeyListLayout, cx: &mut Context<Self>) {
        if self.list_layout == layout {
            return;
        }

        self.list_layout = layout;
        self.rebuild_key_rows();
        cx.notify();
    }

    /// Rebuilds the visible rows and keeps the cursor on the selected key.
    pub(super) fn rebuild_key_rows(&mut self) {
        self.key_rows = key_tree::build_key_rows(
            &self.keys,
            self.list_layout,
            &self.key_delimiter,
            &self.expanded_folders,
        );

        let selected_row = self.selected_index.and_then(|index| {
            self.key_rows
                .iter()
                .position(|row| row.key_index() == Some(index))
        });

        self.list_cursor = match (selected_row, self.list_cursor) {
            (Some(row), _) => Some(row),
            (None, Some(row)) if row < self.key_rows.len() => Some(row),
            (None, _) if self.key_rows.is_empty() => None,
            (None, _) => Some(0),
        };
    }

    pub(super) fn get_connection(&self, cx: &App) -> Option<Arc<dyn dbflux_core::Connection>> {
        self.app_state
            .read(cx)
            .connections()
            .get(&self.profile_id)
            .map(|conn| conn.connection.clone())
    }

    pub(super) fn get_connection_metadata_capabilities(
        &self,
        cx: &App,
    ) -> Option<DriverCapabilities> {
        self.get_connection(cx)
            .map(|connection| connection.metadata().capabilities)
    }

    /// Reports a user action that found the connection closed.
    pub(super) fn report_connection_inactive(&mut self, cx: &mut Context<Self>) {
        report_error(
            UserFacingError::new(
                ErrorKind::Network,
                dbflux_i18n::t!("document.key_value.mutation.error.connection_inactive"),
            ),
            cx,
        );
    }
}
