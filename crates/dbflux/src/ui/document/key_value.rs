use super::add_member_modal::{AddMemberEvent, AddMemberModal};
use super::new_key_modal::{NewKeyCreatedEvent, NewKeyModal, NewKeyType, NewKeyValue};
use super::task_runner::DocumentTaskRunner;
use super::types::{DocumentId, DocumentState};
use crate::app::AppState;
use crate::keymap::{Command, ContextId};
use crate::ui::components::document_tree::{
    DocumentTree, DocumentTreeEvent, DocumentTreeState, NodeId, TreeDirection,
};
use crate::ui::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{
    CancelToken, DbError, HashDeleteRequest, HashSetRequest, KeyDeleteRequest, KeyEntry,
    KeyGetRequest, KeyGetResult, KeyRenameRequest, KeyScanRequest, KeySetRequest, KeyType, ListEnd,
    ListPushRequest, ListRemoveRequest, ListSetRequest, MutationRequest, RefreshPolicy,
    SetAddRequest, SetCondition, SetRemoveRequest, StreamAddRequest, StreamDeleteRequest,
    StreamEntryId, TaskKind, Value, ValueRepr, ZSetAddRequest, ZSetRemoveRequest,
};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::scroll::ScrollableElement;
use gpui_component::{ActiveTheme, Sizable};
use std::collections::BTreeMap;
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
    app_state: Entity<AppState>,
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

    // Keys list (current page only)
    keys: Vec<KeyEntry>,
    selected_index: Option<usize>,
    selected_value: Option<KeyGetResult>,
    last_error: Option<String>,

    // Cursor-based pagination
    current_page: u64,
    current_cursor: Option<String>,
    next_cursor: Option<String>,
    previous_cursors: Vec<Option<String>>,

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
    context_menu_focus: FocusHandle,
    panel_origin: Point<Pixels>,

    _subscriptions: Vec<Subscription>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyValueFocusMode {
    /// Key list navigation (vim keys active).
    List,
    /// Right-side value/members panel (vim keys active).
    ValuePanel,
    /// Any text input is focused (filter, rename, member edit, add member).
    TextInput,
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
enum KvValueViewMode {
    #[default]
    Table,
    Document,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TtlState {
    NoLimit,
    Remaining { deadline: Instant },
    Expired,
    Missing,
}

/// Pending delete confirmation state for keys.
struct PendingKeyDelete {
    key: String,
    index: usize,
}

/// Pending delete confirmation state for members.
struct PendingMemberDelete {
    member_index: usize,
    member_display: String,
}

// ---------------------------------------------------------------------------
// Context menu
// ---------------------------------------------------------------------------

struct KvContextMenu {
    target: KvMenuTarget,
    position: Point<Pixels>,
    items: Vec<KvMenuItem>,
    selected_index: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum KvMenuTarget {
    Key,
    Value,
}

struct KvMenuItem {
    label: &'static str,
    action: KvMenuAction,
    icon: AppIcon,
    is_danger: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum KvMenuAction {
    CopyKey,
    RenameKey,
    NewKey,
    DeleteKey,
    CopyMember,
    EditMember,
    AddMember,
    DeleteMember,
    CopyValue,
    EditValue,
    CopyAsCommand,
}

#[derive(Clone, Debug)]
pub enum KeyValueDocumentEvent {
    RequestFocus,
}

impl KeyValueDocument {
    pub fn new(
        profile_id: Uuid,
        database: String,
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let filter_input = cx.new(|cx| InputState::new(window, cx).placeholder("Filter keys..."));
        let members_filter_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Filter members..."));
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

        let default_refresh = app_state.read(cx).general_settings().resolve_refresh_policy();

        let refresh_dropdown = cx.new(|_cx| {
            let items = RefreshPolicy::ALL
                .iter()
                .map(|policy| DropdownItem::new(policy.label()))
                .collect();

            Dropdown::new("kv-auto-refresh")
                .items(items)
                .selected_index(Some(default_refresh.index()))
                .compact_trigger(true)
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

        let mut doc = Self {
            id: DocumentId::new(),
            title: format!("Redis {}", database),
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
            selected_index: None,
            selected_value: None,
            last_error: None,
            current_page: 1,
            current_cursor: None,
            next_cursor: None,
            previous_cursors: Vec::new(),
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

                let _ = cx.update(|cx| {
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

    fn apply_ttl_from_entry(&mut self, entry: &KeyEntry, cx: &mut Context<Self>) {
        self._ttl_countdown_timer = None;

        match entry.ttl_seconds {
            None | Some(-1) => {
                self.ttl_state = TtlState::NoLimit;
                self.ttl_display = "No limit".into();
            }
            Some(-2) => {
                self.ttl_state = TtlState::Missing;
                self.ttl_display = "Missing".into();
            }
            Some(0) => {
                self.ttl_state = TtlState::Expired;
                self.ttl_display = "Expired".into();
            }
            Some(secs) if secs > 0 => {
                let deadline = Instant::now() + Duration::from_secs(secs as u64);
                self.ttl_state = TtlState::Remaining { deadline };
                self.ttl_display = format!("{}s", secs);
                self.start_ttl_timer(cx);
            }
            _ => {
                self.ttl_state = TtlState::NoLimit;
                self.ttl_display = "No limit".into();
            }
        }
    }

    fn clear_ttl_state(&mut self) {
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
            self.ttl_display = "Expired".into();
            self._ttl_countdown_timer = None;
        } else {
            self.ttl_display = format!("{}s", remaining.as_secs());
        }
    }

    fn start_ttl_timer(&mut self, cx: &mut Context<Self>) {
        self._ttl_countdown_timer = Some(cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor().timer(Duration::from_secs(1)).await;

                let should_stop = cx
                    .update(|cx| {
                        let Some(entity) = this.upgrade() else {
                            return true;
                        };

                        entity.update(cx, |doc, cx| {
                            doc.tick_ttl();
                            cx.notify();
                            doc.ttl_state == TtlState::Expired
                        })
                    })
                    .unwrap_or(true);

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
        self.focus_handle.focus(window);
        cx.notify();
    }

    pub fn active_context(&self, cx: &App) -> ContextId {
        if self.new_key_modal.read(cx).is_visible() {
            return self.new_key_modal.read(cx).active_context();
        }

        if self.add_member_modal.read(cx).is_visible() {
            return self.add_member_modal.read(cx).active_context();
        }

        if self.pending_key_delete.is_some() || self.pending_member_delete.is_some() {
            return ContextId::ConfirmModal;
        }

        if self.context_menu.is_some() {
            return ContextId::ContextMenu;
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

    // -- Command dispatch --

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.new_key_modal.read(cx).is_visible() {
            let handled = self
                .new_key_modal
                .update(cx, |modal, cx| modal.dispatch_command(cmd, window, cx));

            if !self.new_key_modal.read(cx).is_visible() {
                self.focus_mode = KeyValueFocusMode::List;
                self.focus_handle.focus(window);
                cx.notify();
            }

            return handled;
        }

        if self.add_member_modal.read(cx).is_visible() {
            let handled = self
                .add_member_modal
                .update(cx, |modal, cx| modal.dispatch_command(cmd, window, cx));

            if !self.add_member_modal.read(cx).is_visible() {
                self.focus_mode = KeyValueFocusMode::ValuePanel;
                self.focus_handle.focus(window);
                cx.notify();
            }

            return handled;
        }

        if self.context_menu.is_some() {
            return self.dispatch_menu_command(cmd, window, cx);
        }

        match cmd {
            // -- Context menu --
            Command::OpenContextMenu => {
                let target = match self.focus_mode {
                    KeyValueFocusMode::ValuePanel => KvMenuTarget::Value,
                    _ => KvMenuTarget::Key,
                };
                let position = self.keyboard_menu_position(target);
                self.open_context_menu(target, position, window, cx);
                true
            }

            // -- Panel switching (h/l) --
            Command::ColumnLeft => {
                if self.focus_mode == KeyValueFocusMode::ValuePanel {
                    if self.is_document_view_active()
                        && let Some(ts) = &self.document_tree_state
                    {
                        let cursor = ts.read(cx).cursor().cloned();
                        let is_expanded = cursor
                            .as_ref()
                            .map(|id| ts.read(cx).is_expanded(id))
                            .unwrap_or(false);
                        let is_root = cursor
                            .as_ref()
                            .map(|id| id.parent().is_none())
                            .unwrap_or(true);

                        if is_expanded || !is_root {
                            ts.update(cx, |s, cx| s.handle_left(cx));
                            return true;
                        }
                    }

                    self.focus_mode = KeyValueFocusMode::List;
                    cx.notify();
                }
                true
            }
            Command::ColumnRight => {
                if self.focus_mode == KeyValueFocusMode::List && self.selected_value.is_some() {
                    self.focus_mode = KeyValueFocusMode::ValuePanel;

                    if self.is_document_view_active()
                        && let Some(ts) = &self.document_tree_state
                        && ts.read(cx).cursor().is_none()
                    {
                        ts.update(cx, |s, cx| s.move_to_first(cx));
                    } else if self.selected_member_index.is_none()
                        && !self.cached_members.is_empty()
                    {
                        self.selected_member_index = Some(0);
                    }

                    cx.notify();
                } else if self.focus_mode == KeyValueFocusMode::ValuePanel
                    && self.is_document_view_active()
                    && let Some(ts) = &self.document_tree_state
                {
                    ts.update(cx, |s, cx| s.handle_right(cx));
                }
                true
            }

            // -- Vertical navigation (j/k) --
            Command::SelectNext => {
                match self.focus_mode {
                    KeyValueFocusMode::ValuePanel => {
                        if self.is_document_view_active() {
                            if let Some(ts) = &self.document_tree_state {
                                ts.update(cx, |s, cx| {
                                    s.move_cursor(TreeDirection::Down, cx);
                                });
                            }
                        } else {
                            self.move_member_selection(1, cx);
                        }
                    }
                    _ => {
                        self.focus_mode = KeyValueFocusMode::List;
                        self.move_selection(1, cx);
                    }
                }
                true
            }
            Command::SelectPrev => {
                match self.focus_mode {
                    KeyValueFocusMode::ValuePanel => {
                        if self.is_document_view_active() {
                            if let Some(ts) = &self.document_tree_state {
                                ts.update(cx, |s, cx| {
                                    s.move_cursor(TreeDirection::Up, cx);
                                });
                            }
                        } else {
                            self.move_member_selection(-1, cx);
                        }
                    }
                    _ => {
                        self.focus_mode = KeyValueFocusMode::List;
                        self.move_selection(-1, cx);
                    }
                }
                true
            }
            Command::SelectFirst => {
                match self.focus_mode {
                    KeyValueFocusMode::ValuePanel => {
                        if self.is_document_view_active() {
                            if let Some(ts) = &self.document_tree_state {
                                ts.update(cx, |s, cx| s.move_to_first(cx));
                            }
                        } else if !self.cached_members.is_empty() {
                            self.selected_member_index = Some(0);
                            cx.notify();
                        }
                    }
                    _ => {
                        if !self.keys.is_empty() {
                            self.focus_mode = KeyValueFocusMode::List;
                            self.select_index(0, cx);
                        }
                    }
                }
                true
            }
            Command::SelectLast => {
                match self.focus_mode {
                    KeyValueFocusMode::ValuePanel => {
                        if self.is_document_view_active() {
                            if let Some(ts) = &self.document_tree_state {
                                ts.update(cx, |s, cx| s.move_to_last(cx));
                            }
                        } else if !self.cached_members.is_empty() {
                            self.selected_member_index = Some(self.cached_members.len() - 1);
                            cx.notify();
                        }
                    }
                    _ => {
                        if !self.keys.is_empty() {
                            self.focus_mode = KeyValueFocusMode::List;
                            self.select_index(self.keys.len() - 1, cx);
                        }
                    }
                }
                true
            }
            Command::ExpandCollapse => {
                if self.focus_mode == KeyValueFocusMode::ValuePanel
                    && self.is_document_view_active()
                    && let Some(ts) = &self.document_tree_state
                {
                    let cursor = ts.read(cx).cursor().cloned();
                    if let Some(id) = cursor {
                        ts.update(cx, |s, cx| s.toggle_expand(&id, cx));
                    }
                }
                true
            }

            // -- Actions --
            Command::Delete => {
                if self.focus_mode == KeyValueFocusMode::ValuePanel {
                    if let Some(idx) = self.selected_member_index {
                        self.request_delete_member(idx, cx);
                    }
                } else {
                    self.request_delete_key(cx);
                }
                true
            }
            Command::Rename => {
                self.start_rename(window, cx);
                true
            }
            Command::FocusSearch | Command::FocusToolbar => {
                let target_input = if self.focus_mode == KeyValueFocusMode::ValuePanel {
                    &self.members_filter_input
                } else {
                    &self.filter_input
                };
                target_input.update(cx, |input, cx| input.focus(window, cx));
                self.focus_mode = KeyValueFocusMode::TextInput;
                cx.notify();
                true
            }
            Command::FocusUp => {
                self.focus_mode = KeyValueFocusMode::TextInput;
                self.filter_input
                    .update(cx, |input, cx| input.focus(window, cx));
                cx.notify();
                true
            }
            Command::FocusDown => {
                self.focus_mode = KeyValueFocusMode::List;
                self.focus_handle.focus(window);
                cx.notify();
                true
            }
            Command::Cancel => {
                if self.pending_key_delete.is_some() {
                    self.cancel_delete_key(cx);
                } else if self.pending_member_delete.is_some() {
                    self.cancel_delete_member(cx);
                } else if self.string_edit_input.is_some() {
                    self.cancel_string_edit(cx);
                    self.focus_handle.focus(window);
                } else if self.renaming_index.is_some() {
                    self.cancel_rename(cx);
                    self.focus_handle.focus(window);
                } else if self.editing_member_index.is_some() {
                    self.cancel_member_edit(cx);
                    self.focus_handle.focus(window);
                } else if self.runner.cancel_primary(cx) {
                    self.last_error = None;
                } else {
                    self.focus_mode = KeyValueFocusMode::List;
                    self.focus_handle.focus(window);
                }
                cx.notify();
                true
            }
            Command::Execute => {
                if self.pending_key_delete.is_some() {
                    self.confirm_delete_key(cx);
                    return true;
                }
                if self.pending_member_delete.is_some() {
                    self.confirm_delete_member(cx);
                    return true;
                }
                if self.focus_mode == KeyValueFocusMode::ValuePanel {
                    if self.is_document_view_active() {
                        if let Some(ts) = &self.document_tree_state {
                            ts.update(cx, |s, cx| s.start_edit_at_cursor(window, cx));
                        }
                        return true;
                    }

                    if self.is_stream_type() {
                        return true;
                    }
                    if self.is_structured_type() {
                        if let Some(idx) = self.selected_member_index {
                            self.start_member_edit(idx, window, cx);
                        }
                    } else {
                        self.start_string_edit(window, cx);
                    }
                    return true;
                }
                self.start_string_edit(window, cx);
                true
            }
            Command::ResultsNextPage | Command::PageDown => {
                if self.focus_mode == KeyValueFocusMode::ValuePanel
                    && self.is_document_view_active()
                {
                    if let Some(ts) = &self.document_tree_state {
                        ts.update(cx, |s, cx| s.page_down(20, cx));
                    }
                } else {
                    self.go_next_page(cx);
                }
                true
            }
            Command::ResultsPrevPage | Command::PageUp => {
                if self.focus_mode == KeyValueFocusMode::ValuePanel
                    && self.is_document_view_active()
                {
                    if let Some(ts) = &self.document_tree_state {
                        ts.update(cx, |s, cx| s.page_up(20, cx));
                    }
                } else {
                    self.go_prev_page(cx);
                }
                true
            }
            Command::ResultsAddRow => {
                if self.focus_mode == KeyValueFocusMode::ValuePanel && self.is_structured_type() {
                    if let Some(key_type) = self.selected_key_type() {
                        self.pending_open_add_member_modal = Some(key_type);
                    }
                } else {
                    self.pending_open_new_key_modal = true;
                }
                cx.notify();
                true
            }
            Command::ResultsCopyRow => {
                let text = match self.focus_mode {
                    KeyValueFocusMode::ValuePanel => self
                        .selected_member_index
                        .and_then(|idx| self.cached_members.get(idx))
                        .map(|m| m.display.clone()),
                    _ => self
                        .selected_index
                        .and_then(|idx| self.keys.get(idx))
                        .map(|entry| entry.key.clone()),
                };
                if let Some(text) = text {
                    cx.write_to_clipboard(ClipboardItem::new_string(text));
                }
                true
            }
            Command::RefreshSchema => {
                self.reload_keys(cx);
                true
            }
            _ => false,
        }
    }

    // -- Context menu --

    fn build_key_menu_items(&self) -> Vec<KvMenuItem> {
        let mut items = vec![
            KvMenuItem {
                label: "Copy Key",
                action: KvMenuAction::CopyKey,
                icon: AppIcon::Columns,
                is_danger: false,
            },
            KvMenuItem {
                label: "Copy as Command",
                action: KvMenuAction::CopyAsCommand,
                icon: AppIcon::Code,
                is_danger: false,
            },
            KvMenuItem {
                label: "Rename",
                action: KvMenuAction::RenameKey,
                icon: AppIcon::Pencil,
                is_danger: false,
            },
            KvMenuItem {
                label: "New Key",
                action: KvMenuAction::NewKey,
                icon: AppIcon::Plus,
                is_danger: false,
            },
            KvMenuItem {
                label: "Delete Key",
                action: KvMenuAction::DeleteKey,
                icon: AppIcon::Delete,
                is_danger: true,
            },
        ];

        if self.selected_value.is_none() {
            items.retain(|item| item.action != KvMenuAction::CopyAsCommand);
        }

        items
    }

    fn build_value_menu_items(&self) -> Vec<KvMenuItem> {
        if self.is_stream_type() {
            vec![
                KvMenuItem {
                    label: "Copy Entry",
                    action: KvMenuAction::CopyMember,
                    icon: AppIcon::Columns,
                    is_danger: false,
                },
                KvMenuItem {
                    label: "Copy as Command",
                    action: KvMenuAction::CopyAsCommand,
                    icon: AppIcon::Code,
                    is_danger: false,
                },
                KvMenuItem {
                    label: "Add Entry",
                    action: KvMenuAction::AddMember,
                    icon: AppIcon::Plus,
                    is_danger: false,
                },
                KvMenuItem {
                    label: "Delete Entry",
                    action: KvMenuAction::DeleteMember,
                    icon: AppIcon::Delete,
                    is_danger: true,
                },
            ]
        } else if self.is_structured_type() {
            vec![
                KvMenuItem {
                    label: "Copy Member",
                    action: KvMenuAction::CopyMember,
                    icon: AppIcon::Columns,
                    is_danger: false,
                },
                KvMenuItem {
                    label: "Copy as Command",
                    action: KvMenuAction::CopyAsCommand,
                    icon: AppIcon::Code,
                    is_danger: false,
                },
                KvMenuItem {
                    label: "Edit Member",
                    action: KvMenuAction::EditMember,
                    icon: AppIcon::Pencil,
                    is_danger: false,
                },
                KvMenuItem {
                    label: "Add Member",
                    action: KvMenuAction::AddMember,
                    icon: AppIcon::Plus,
                    is_danger: false,
                },
                KvMenuItem {
                    label: "Delete Member",
                    action: KvMenuAction::DeleteMember,
                    icon: AppIcon::Delete,
                    is_danger: true,
                },
            ]
        } else {
            vec![
                KvMenuItem {
                    label: "Copy Value",
                    action: KvMenuAction::CopyValue,
                    icon: AppIcon::Columns,
                    is_danger: false,
                },
                KvMenuItem {
                    label: "Copy as Command",
                    action: KvMenuAction::CopyAsCommand,
                    icon: AppIcon::Code,
                    is_danger: false,
                },
                KvMenuItem {
                    label: "Edit Value",
                    action: KvMenuAction::EditValue,
                    icon: AppIcon::Pencil,
                    is_danger: false,
                },
                KvMenuItem {
                    label: "Delete Key",
                    action: KvMenuAction::DeleteKey,
                    icon: AppIcon::Delete,
                    is_danger: true,
                },
            ]
        }
    }

    /// Computes a window-coordinate position for keyboard-triggered menus,
    /// aligned vertically with the selected row in the active panel.
    fn keyboard_menu_position(&self, target: KvMenuTarget) -> Point<Pixels> {
        // Left panel: toolbar(32) + status line(~24) = 56px before keys list
        // Right panel: toolbar(32) + metadata(~24) + optional filter(~30) + header(24) ≈ 110px
        let left_header = Heights::TOOLBAR + Heights::ROW_COMPACT;
        let right_header =
            Heights::TOOLBAR + Heights::ROW_COMPACT + px(30.0) + Heights::ROW_COMPACT;

        match target {
            KvMenuTarget::Key => {
                let row_index = self.selected_index.unwrap_or(0) as f32;
                Point {
                    x: self.panel_origin.x + px(12.0),
                    y: self.panel_origin.y + left_header + Heights::ROW * row_index,
                }
            }
            KvMenuTarget::Value => {
                let row_index = self.selected_member_index.unwrap_or(0) as f32;
                Point {
                    x: self.panel_origin.x + px(240.0) + px(12.0),
                    y: self.panel_origin.y + right_header + Heights::ROW * row_index,
                }
            }
        }
    }

    fn open_context_menu(
        &mut self,
        target: KvMenuTarget,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let items = match target {
            KvMenuTarget::Key => self.build_key_menu_items(),
            KvMenuTarget::Value => self.build_value_menu_items(),
        };

        self.context_menu = Some(KvContextMenu {
            target,
            position,
            items,
            selected_index: 0,
        });

        self.context_menu_focus.focus(window);
        cx.notify();
    }

    fn close_context_menu(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(menu) = self.context_menu.take() {
            match menu.target {
                KvMenuTarget::Key => self.focus_mode = KeyValueFocusMode::List,
                KvMenuTarget::Value => self.focus_mode = KeyValueFocusMode::ValuePanel,
            }
        }

        self.focus_handle.focus(window);
        cx.notify();
    }

    fn dispatch_menu_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let item_count = match &self.context_menu {
            Some(menu) => menu.items.len(),
            None => return false,
        };

        match cmd {
            Command::MenuDown => {
                if let Some(ref mut menu) = self.context_menu {
                    menu.selected_index = (menu.selected_index + 1) % item_count;
                    cx.notify();
                }
                true
            }
            Command::MenuUp => {
                if let Some(ref mut menu) = self.context_menu {
                    menu.selected_index = if menu.selected_index == 0 {
                        item_count - 1
                    } else {
                        menu.selected_index - 1
                    };
                    cx.notify();
                }
                true
            }
            Command::MenuSelect => {
                if let Some(menu) = self.context_menu.take() {
                    let action = menu.items[menu.selected_index].action;
                    let target = menu.target;
                    self.execute_menu_action(action, target, window, cx);
                }
                true
            }
            Command::MenuBack | Command::Cancel => {
                self.close_context_menu(window, cx);
                true
            }
            _ => false,
        }
    }

    fn execute_menu_action(
        &mut self,
        action: KvMenuAction,
        target: KvMenuTarget,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match target {
            KvMenuTarget::Key => self.focus_mode = KeyValueFocusMode::List,
            KvMenuTarget::Value => self.focus_mode = KeyValueFocusMode::ValuePanel,
        }
        self.focus_handle.focus(window);

        match action {
            KvMenuAction::CopyKey => {
                if let Some(key) = self.selected_key() {
                    cx.write_to_clipboard(ClipboardItem::new_string(key));
                }
            }
            KvMenuAction::RenameKey => {
                self.start_rename(window, cx);
            }
            KvMenuAction::NewKey => {
                self.pending_open_new_key_modal = true;
            }
            KvMenuAction::DeleteKey => {
                self.request_delete_key(cx);
            }
            KvMenuAction::CopyMember => {
                if let Some(idx) = self.selected_member_index
                    && let Some(member) = self.cached_members.get(idx)
                {
                    cx.write_to_clipboard(ClipboardItem::new_string(member.display.clone()));
                }
            }
            KvMenuAction::EditMember => {
                if let Some(idx) = self.selected_member_index {
                    self.start_member_edit(idx, window, cx);
                }
            }
            KvMenuAction::AddMember => {
                if let Some(key_type) = self.selected_key_type() {
                    self.pending_open_add_member_modal = Some(key_type);
                }
            }
            KvMenuAction::DeleteMember => {
                if let Some(idx) = self.selected_member_index {
                    self.request_delete_member(idx, cx);
                }
            }
            KvMenuAction::CopyValue => {
                if let Some(value) = &self.selected_value {
                    let text = String::from_utf8_lossy(&value.value).to_string();
                    cx.write_to_clipboard(ClipboardItem::new_string(text));
                }
            }
            KvMenuAction::EditValue => {
                self.start_string_edit(window, cx);
            }
            KvMenuAction::CopyAsCommand => {
                self.handle_copy_as_command(target, cx);
            }
        }

        cx.notify();
    }

    // -- Selection helpers --

    fn selected_key(&self) -> Option<String> {
        self.selected_index
            .and_then(|idx| self.keys.get(idx))
            .map(|entry| entry.key.clone())
    }

    fn selected_key_type(&self) -> Option<KeyType> {
        self.selected_value.as_ref().and_then(|v| v.entry.key_type)
    }

    fn keyspace_index(&self) -> Option<u32> {
        parse_database_name(&self.database)
    }

    fn move_member_selection(&mut self, delta: isize, cx: &mut Context<Self>) {
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

    fn move_selection(&mut self, delta: isize, cx: &mut Context<Self>) {
        if self.keys.is_empty() {
            self.selected_index = None;
            self.selected_value = None;
            self.clear_ttl_state();
            self.rebuild_cached_members(cx);
            cx.notify();
            return;
        }

        let current = self.selected_index.unwrap_or(0) as isize;
        let next = (current + delta).clamp(0, (self.keys.len() - 1) as isize) as usize;
        self.select_index(next, cx);
    }

    fn select_index(&mut self, index: usize, cx: &mut Context<Self>) {
        if self.selected_index == Some(index) {
            return;
        }

        self.selected_index = Some(index);
        self.selected_value = None;
        self.selected_member_index = None;
        self.string_edit_input = None;
        self.clear_ttl_state();
        self.rebuild_cached_members(cx);
        self.cancel_member_edit(cx);
        cx.notify();
        self.reload_selected_value(cx);
    }

    // -- Key CRUD --

    fn reload_keys(&mut self, cx: &mut Context<Self>) {
        self.current_page = 1;
        self.current_cursor = None;
        self.next_cursor = None;
        self.previous_cursors.clear();
        self.load_page(cx);
    }

    fn go_next_page(&mut self, cx: &mut Context<Self>) {
        let Some(next) = self.next_cursor.clone() else {
            return;
        };
        self.previous_cursors.push(self.current_cursor.clone());
        self.current_cursor = Some(next);
        self.current_page += 1;
        self.load_page(cx);
    }

    fn go_prev_page(&mut self, cx: &mut Context<Self>) {
        let Some(prev) = self.previous_cursors.pop() else {
            return;
        };
        self.current_cursor = prev;
        self.current_page = self.current_page.saturating_sub(1).max(1);
        self.load_page(cx);
    }

    fn can_go_next(&self) -> bool {
        !self.runner.is_primary_active() && self.next_cursor.is_some()
    }

    fn can_go_prev(&self) -> bool {
        !self.runner.is_primary_active() && !self.previous_cursors.is_empty()
    }

    fn load_page(&mut self, cx: &mut Context<Self>) {
        self.keys.clear();
        self.selected_index = None;
        self.selected_value = None;
        self.last_error = None;
        self.string_edit_input = None;
        self.clear_ttl_state();
        self.rebuild_cached_members(cx);
        self.cancel_rename(cx);
        self.cancel_member_edit(cx);

        let Some(connection) = self.get_connection(cx) else {
            self.last_error = Some("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.last_error = Some("Too many background tasks running, please wait".to_string());
            cx.notify();
            return;
        }

        let filter = self.filter_input.read(cx).value().trim().to_string();
        let is_first_page = self.current_page == 1;
        let is_unfiltered = filter.is_empty();
        let database = self.database.clone();
        let entity = cx.entity().clone();

        let description = if filter.is_empty() {
            format!("SCAN {}", database)
        } else {
            format!("SCAN {} *{}*", database, filter)
        };

        let (task_id, cancel_token) = self
            .runner
            .start_primary(TaskKind::KeyScan, description, cx);
        cx.notify();

        let request = KeyScanRequest {
            cursor: self.current_cursor.clone(),
            filter: if filter.is_empty() {
                None
            } else {
                Some(format!("*{}*", filter))
            },
            limit: 200,
            keyspace: parse_database_name(&database),
        };

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;
                    api.scan_keys(&request)
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    match result {
                        Ok(page) => {
                            this.runner.complete_primary(task_id, cx);

                            this.keys = page.entries;
                            this.next_cursor = page.next_cursor;
                            this.last_error = None;

                            if is_first_page && is_unfiltered {
                                let key_names: Vec<String> =
                                    this.keys.iter().map(|e| e.key.clone()).collect();

                                this.app_state.update(cx, |state, _cx| {
                                    state.set_redis_cached_keys(
                                        this.profile_id,
                                        this.database.clone(),
                                        key_names,
                                    );
                                });
                            }

                            if !this.keys.is_empty() {
                                this.selected_index = Some(0);
                                this.reload_selected_value(cx);
                            }
                        }
                        Err(error) => {
                            this.runner.fail_primary(task_id, error.to_string(), cx);
                            this.last_error = Some(error.to_string());
                        }
                    }

                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    fn reload_selected_value(&mut self, cx: &mut Context<Self>) {
        let Some(key) = self.selected_key() else {
            self.selected_value = None;
            self.rebuild_cached_members(cx);
            cx.notify();
            return;
        };

        let Some(connection) = self.get_connection(cx) else {
            self.last_error = Some("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        let description = format!("GET {}", dbflux_core::truncate_string_safe(&key, 60));
        let (task_id, cancel_token) = self.runner.start_primary(TaskKind::KeyGet, description, cx);
        cx.notify();

        let keyspace = self.keyspace_index();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;
                    api.get_key(&KeyGetRequest {
                        key,
                        keyspace,
                        include_type: true,
                        include_ttl: true,
                        include_size: true,
                    })
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    match result {
                        Ok(value) => {
                            this.runner.complete_primary(task_id, cx);

                            let key_type = value.entry.key_type;
                            let is_hash_or_stream =
                                matches!(key_type, Some(KeyType::Hash | KeyType::Stream));
                            this.apply_ttl_from_entry(&value.entry, cx);
                            this.selected_value = Some(value);
                            this.last_error = None;
                            this.value_view_mode = if is_hash_or_stream {
                                KvValueViewMode::Document
                            } else {
                                KvValueViewMode::Table
                            };
                            this.rebuild_cached_members(cx);
                        }
                        Err(error) => {
                            this.runner.fail_primary(task_id, error.to_string(), cx);
                            this.clear_ttl_state();
                            this.selected_value = None;
                            this.last_error = Some(error.to_string());
                            this.rebuild_cached_members(cx);
                        }
                    }

                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    // -- Delete confirmation --

    fn request_delete_key(&mut self, cx: &mut Context<Self>) {
        let Some(index) = self.selected_index else {
            return;
        };
        let Some(entry) = self.keys.get(index) else {
            return;
        };

        self.pending_key_delete = Some(PendingKeyDelete {
            key: entry.key.clone(),
            index,
        });
        cx.notify();
    }

    fn confirm_delete_key(&mut self, cx: &mut Context<Self>) {
        let Some(pending) = self.pending_key_delete.take() else {
            return;
        };

        if pending.index < self.keys.len() {
            self.keys.remove(pending.index);
            self.clear_ttl_state();

            if self.keys.is_empty() {
                self.selected_index = None;
                self.selected_value = None;
                self.rebuild_cached_members(cx);
            } else {
                let new_idx = pending.index.min(self.keys.len() - 1);
                self.selected_index = Some(new_idx);
                self.selected_value = None;
                self.rebuild_cached_members(cx);
                self.reload_selected_value(cx);
            }
        }
        cx.notify();

        self.delete_key_async(pending.key, cx);
    }

    fn cancel_delete_key(&mut self, cx: &mut Context<Self>) {
        self.pending_key_delete = None;
        cx.notify();
    }

    fn request_delete_member(&mut self, member_index: usize, cx: &mut Context<Self>) {
        let Some(member) = self.cached_members.get(member_index) else {
            return;
        };

        self.pending_member_delete = Some(PendingMemberDelete {
            member_index,
            member_display: member.display.clone(),
        });
        cx.notify();
    }

    fn confirm_delete_member(&mut self, cx: &mut Context<Self>) {
        let Some(pending) = self.pending_member_delete.take() else {
            return;
        };

        if self.selected_key().is_none()
            || self.selected_key_type().is_none()
            || self.get_connection(cx).is_none()
        {
            self.last_error = Some("Cannot delete member: connection or key unavailable".into());
            cx.notify();
            return;
        }

        let member = if pending.member_index < self.cached_members.len() {
            Some(self.cached_members.remove(pending.member_index))
        } else {
            None
        };

        if self.cached_members.is_empty() {
            self.selected_member_index = None;
        } else if let Some(sel) = self.selected_member_index {
            let new_sel = sel.min(self.cached_members.len() - 1);
            self.selected_member_index = Some(new_sel);
        }

        cx.notify();

        if let Some(member) = member {
            self.delete_member_async(member, cx);
        }
    }

    fn cancel_delete_member(&mut self, cx: &mut Context<Self>) {
        self.pending_member_delete = None;
        cx.notify();
    }

    fn delete_key_async(&mut self, key: String, cx: &mut Context<Self>) {
        let Some(connection) = self.get_connection(cx) else {
            self.last_error = Some("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        let description = format!("DEL {}", dbflux_core::truncate_string_safe(&key, 60));
        let (task_id, _cancel_token) =
            self.runner
                .start_mutation(TaskKind::KeyMutation, description, cx);

        let keyspace = self.keyspace_index();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;
                    api.delete_key(&KeyDeleteRequest { key, keyspace })
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    match result {
                        Ok(true) => {
                            this.runner.complete_mutation(task_id, cx);
                            this.last_error = None;
                        }
                        Ok(false) => {
                            this.runner
                                .fail_mutation(task_id, "Key was not deleted", cx);
                            this.last_error = Some("Key was not deleted".to_string());
                            this.reload_keys(cx);
                            return;
                        }
                        Err(error) => {
                            this.runner.fail_mutation(task_id, error.to_string(), cx);
                            this.last_error = Some(error.to_string());
                            this.reload_keys(cx);
                            return;
                        }
                    }
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    fn delete_member_async(&mut self, member: MemberEntry, cx: &mut Context<Self>) {
        let Some(key) = self.selected_key() else {
            return;
        };
        let Some(key_type) = self.selected_key_type() else {
            return;
        };
        let Some(connection) = self.get_connection(cx) else {
            return;
        };

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::KeyMutation,
            format!(
                "Delete member from {}",
                dbflux_core::truncate_string_safe(&key, 40)
            ),
            cx,
        );

        let keyspace = self.keyspace_index();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;

                    match key_type {
                        KeyType::Hash => {
                            if let Some(field) = member.field {
                                api.hash_delete(&HashDeleteRequest {
                                    key,
                                    fields: vec![field],
                                    keyspace,
                                })?;
                            }
                        }
                        KeyType::List => {
                            api.list_remove(&ListRemoveRequest {
                                key,
                                value: member.display,
                                count: 1,
                                keyspace,
                            })?;
                        }
                        KeyType::Set => {
                            api.set_remove(&SetRemoveRequest {
                                key,
                                members: vec![member.display],
                                keyspace,
                            })?;
                        }
                        KeyType::SortedSet => {
                            api.zset_remove(&ZSetRemoveRequest {
                                key,
                                members: vec![member.display],
                                keyspace,
                            })?;
                        }
                        KeyType::Stream => {
                            if let Some(id) = member.entry_id {
                                api.stream_delete(&StreamDeleteRequest {
                                    key,
                                    ids: vec![id],
                                    keyspace,
                                })?;
                            }
                        }
                        _ => {}
                    }

                    Ok::<(), DbError>(())
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| match result {
                    Ok(()) => {
                        this.runner.complete_mutation(task_id, cx);
                        this.last_error = None;
                        this.reload_selected_value(cx);
                    }
                    Err(error) => {
                        this.runner.fail_mutation(task_id, error.to_string(), cx);
                        this.last_error = Some(error.to_string());
                        this.reload_selected_value(cx);
                    }
                });
            })
            .ok();
        })
        .detach();
    }

    // -- Inline rename --

    fn start_rename(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(index) = self.selected_index else {
            return;
        };
        let Some(key) = self.keys.get(index) else {
            return;
        };

        let input = cx.new(|cx| InputState::new(window, cx));
        input.update(cx, |state, cx| {
            state.set_value(&key.key, window, cx);
            state.focus(window, cx);
        });

        self._subscriptions.push(cx.subscribe_in(
            &input,
            window,
            |this, _, event: &InputEvent, window, cx| match event {
                InputEvent::PressEnter { .. } => this.commit_rename(window, cx),
                InputEvent::Blur => {
                    this.cancel_rename(cx);
                    this.focus_handle.focus(window);
                }
                _ => {}
            },
        ));

        self.rename_input = Some(input);
        self.renaming_index = Some(index);
        self.focus_mode = KeyValueFocusMode::TextInput;
        cx.notify();
    }

    fn commit_rename(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(index) = self.renaming_index.take() else {
            return;
        };
        let Some(input) = self.rename_input.take() else {
            return;
        };

        self.focus_mode = KeyValueFocusMode::List;
        self.focus_handle.focus(window);

        let new_name = input.read(cx).value().trim().to_string();
        let Some(old_name) = self.keys.get(index).map(|k| k.key.clone()) else {
            cx.notify();
            return;
        };

        if new_name.is_empty() || new_name == old_name {
            cx.notify();
            return;
        }

        // Optimistic: update local key name immediately
        if let Some(entry) = self.keys.get_mut(index) {
            entry.key = new_name.clone();
        }
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            self.last_error = Some("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::KeyMutation,
            format!(
                "RENAME {} -> {}",
                dbflux_core::truncate_string_safe(&old_name, 30),
                dbflux_core::truncate_string_safe(&new_name, 30)
            ),
            cx,
        );

        let keyspace = self.keyspace_index();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;
                    api.rename_key(&KeyRenameRequest {
                        from_key: old_name,
                        to_key: new_name,
                        keyspace,
                    })
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| match result {
                    Ok(()) => {
                        this.runner.complete_mutation(task_id, cx);
                        this.last_error = None;
                        this.reload_keys(cx);
                    }
                    Err(error) => {
                        this.runner.fail_mutation(task_id, error.to_string(), cx);
                        this.last_error = Some(error.to_string());
                        cx.notify();
                    }
                });
            })
            .ok();
        })
        .detach();

        cx.notify();
    }

    fn cancel_rename(&mut self, cx: &mut Context<Self>) {
        self.rename_input = None;
        self.renaming_index = None;
        self.focus_mode = KeyValueFocusMode::List;
        cx.notify();
    }

    // -- Inline string/JSON value editing --

    fn start_string_edit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(value) = &self.selected_value else {
            return;
        };
        let key_type = value.entry.key_type.unwrap_or(KeyType::Unknown);
        if !matches!(key_type, KeyType::String | KeyType::Json) {
            return;
        }

        let text = String::from_utf8_lossy(&value.value).to_string();
        let input = cx.new(|cx| {
            let mut state = InputState::new(window, cx);
            state.set_value(text, window, cx);
            state
        });
        input.update(cx, |state, cx| {
            state.focus(window, cx);
        });

        self._subscriptions.push(cx.subscribe_in(
            &input,
            window,
            |this, _, event: &InputEvent, window, cx| match event {
                InputEvent::PressEnter { .. } => this.commit_string_edit(window, cx),
                InputEvent::Blur => {
                    this.cancel_string_edit(cx);
                    this.focus_handle.focus(window);
                }
                _ => {}
            },
        ));

        self.string_edit_input = Some(input);
        self.focus_mode = KeyValueFocusMode::TextInput;
        cx.notify();
    }

    fn commit_string_edit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(input) = self.string_edit_input.take() else {
            return;
        };
        let new_text = input.read(cx).value().to_string();

        self.focus_mode = KeyValueFocusMode::List;
        self.focus_handle.focus(window);

        let Some(value) = &self.selected_value else {
            cx.notify();
            return;
        };

        let key = value.entry.key.clone();
        let key_type = value.entry.key_type.unwrap_or(KeyType::String);

        let repr = if key_type == KeyType::Json {
            ValueRepr::Json
        } else {
            ValueRepr::Text
        };

        // Optimistic: update the cached value immediately
        if let Some(val) = &mut self.selected_value {
            val.value = new_text.clone().into_bytes();
        }
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            return;
        };

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::KeyMutation,
            format!("SET {}", dbflux_core::truncate_string_safe(&key, 60)),
            cx,
        );

        let keyspace = self.keyspace_index();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;
                    api.set_key(&KeySetRequest {
                        key,
                        value: new_text.into_bytes(),
                        repr,
                        keyspace,
                        ttl_seconds: None,
                        condition: SetCondition::Always,
                    })
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| match result {
                    Ok(()) => {
                        this.runner.complete_mutation(task_id, cx);
                        this.last_error = None;
                        this.reload_selected_value(cx);
                    }
                    Err(error) => {
                        this.runner.fail_mutation(task_id, error.to_string(), cx);
                        this.last_error = Some(error.to_string());
                        cx.notify();
                    }
                });
            })
            .ok();
        })
        .detach();
    }

    fn cancel_string_edit(&mut self, cx: &mut Context<Self>) {
        self.string_edit_input = None;
        self.focus_mode = KeyValueFocusMode::List;
        cx.notify();
    }

    // -- Member editing --

    fn start_member_edit(&mut self, index: usize, window: &mut Window, cx: &mut Context<Self>) {
        let Some(member) = self.cached_members.get(index).cloned() else {
            return;
        };

        let input = cx.new(|cx| InputState::new(window, cx));
        input.update(cx, |state, cx| {
            state.set_value(&member.display, window, cx);
            state.focus(window, cx);
        });

        self._subscriptions.push(cx.subscribe_in(
            &input,
            window,
            |this, _, event: &InputEvent, window, cx| match event {
                InputEvent::PressEnter { .. } => this.commit_member_edit(window, cx),
                InputEvent::Blur => {
                    this.cancel_member_edit(cx);
                    this.focus_handle.focus(window);
                }
                _ => {}
            },
        ));

        let score_input = if member.score.is_some() {
            let score_in = cx.new(|cx| InputState::new(window, cx));
            score_in.update(cx, |state, cx| {
                state.set_value(
                    member.score.map(|s| s.to_string()).unwrap_or_default(),
                    window,
                    cx,
                );
            });
            Some(score_in)
        } else {
            None
        };

        self.editing_member_index = Some(index);
        self.member_edit_input = Some(input);
        self.member_edit_score_input = score_input;
        self.focus_mode = KeyValueFocusMode::TextInput;
        cx.notify();
    }

    fn commit_member_edit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(member_index) = self.editing_member_index.take() else {
            return;
        };
        let Some(input) = self.member_edit_input.take() else {
            return;
        };
        let score_input = self.member_edit_score_input.take();
        self.focus_mode = KeyValueFocusMode::ValuePanel;
        self.selected_member_index = Some(member_index);
        self.focus_handle.focus(window);

        let new_value = input.read(cx).value().to_string();
        let new_score = score_input.map(|si| si.read(cx).value().parse::<f64>().unwrap_or(0.0));

        let Some(old_member) = self.cached_members.get(member_index).cloned() else {
            cx.notify();
            return;
        };

        // Optimistic: update the cached member immediately
        if let Some(cached) = self.cached_members.get_mut(member_index) {
            cached.display = new_value.clone();
            if let Some(score) = new_score {
                cached.score = Some(score);
            }
        }
        cx.notify();

        let Some(key) = self.selected_key() else {
            cx.notify();
            return;
        };
        let Some(key_type) = self.selected_key_type() else {
            cx.notify();
            return;
        };
        let Some(connection) = self.get_connection(cx) else {
            cx.notify();
            return;
        };

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::KeyMutation,
            format!(
                "Edit member in {}",
                dbflux_core::truncate_string_safe(&key, 40)
            ),
            cx,
        );

        let keyspace = self.keyspace_index();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;

                    match key_type {
                        KeyType::Hash => {
                            if let Some(field_name) = &old_member.field
                                && new_value != old_member.display
                            {
                                api.hash_delete(&HashDeleteRequest {
                                    key: key.clone(),
                                    fields: vec![field_name.clone()],
                                    keyspace,
                                })?;
                                api.hash_set(&HashSetRequest {
                                    key,
                                    fields: vec![(field_name.clone(), new_value)],
                                    keyspace,
                                })?;
                            }
                        }
                        KeyType::List => {
                            api.list_set(&ListSetRequest {
                                key,
                                index: member_index as i64,
                                value: new_value,
                                keyspace,
                            })?;
                        }
                        KeyType::Set => {
                            if new_value != old_member.display {
                                api.set_remove(&SetRemoveRequest {
                                    key: key.clone(),
                                    members: vec![old_member.display],
                                    keyspace,
                                })?;
                                api.set_add(&SetAddRequest {
                                    key,
                                    members: vec![new_value],
                                    keyspace,
                                })?;
                            }
                        }
                        KeyType::SortedSet => {
                            let score = new_score.unwrap_or(old_member.score.unwrap_or(0.0));

                            if new_value != old_member.display {
                                api.zset_remove(&ZSetRemoveRequest {
                                    key: key.clone(),
                                    members: vec![old_member.display],
                                    keyspace,
                                })?;
                            }
                            api.zset_add(&ZSetAddRequest {
                                key,
                                members: vec![(new_value, score)],
                                keyspace,
                            })?;
                        }
                        _ => {}
                    }

                    Ok::<(), DbError>(())
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| match result {
                    Ok(()) => {
                        this.runner.complete_mutation(task_id, cx);
                        this.last_error = None;
                        this.reload_selected_value(cx);
                    }
                    Err(error) => {
                        this.runner.fail_mutation(task_id, error.to_string(), cx);
                        this.last_error = Some(error.to_string());
                        cx.notify();
                    }
                });
            })
            .ok();
        })
        .detach();

        cx.notify();
    }

    fn cancel_member_edit(&mut self, cx: &mut Context<Self>) {
        if self.editing_member_index.is_none() {
            return;
        }

        self.editing_member_index = None;
        self.member_edit_input = None;
        self.member_edit_score_input = None;
        self.focus_mode = KeyValueFocusMode::ValuePanel;
        cx.notify();
    }

    // -- Add Member modal --

    fn handle_add_member_event(&mut self, event: AddMemberEvent, cx: &mut Context<Self>) {
        let Some(key) = self.selected_key() else {
            return;
        };
        let Some(key_type) = self.selected_key_type() else {
            return;
        };
        let Some(connection) = self.get_connection(cx) else {
            self.last_error = Some("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::KeyMutation,
            format!(
                "Add member to {}",
                dbflux_core::truncate_string_safe(&key, 40)
            ),
            cx,
        );

        let keyspace = self.keyspace_index();
        let entity = cx.entity().clone();
        let fields = event.fields;

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;

                    match key_type {
                        KeyType::Hash => {
                            api.hash_set(&HashSetRequest {
                                key,
                                fields,
                                keyspace,
                            })?;
                        }
                        KeyType::List => {
                            let values = fields.into_iter().map(|(member, _)| member).collect();
                            api.list_push(&ListPushRequest {
                                key,
                                values,
                                end: ListEnd::Tail,
                                keyspace,
                            })?;
                        }
                        KeyType::Set => {
                            let members = fields.into_iter().map(|(member, _)| member).collect();
                            api.set_add(&SetAddRequest {
                                key,
                                members,
                                keyspace,
                            })?;
                        }
                        KeyType::SortedSet => {
                            let members = fields
                                .into_iter()
                                .map(|(member, score_str)| {
                                    let score = score_str.parse::<f64>().unwrap_or(0.0);
                                    (member, score)
                                })
                                .collect();
                            api.zset_add(&ZSetAddRequest {
                                key,
                                members,
                                keyspace,
                            })?;
                        }
                        KeyType::Stream => {
                            api.stream_add(&StreamAddRequest {
                                key,
                                id: StreamEntryId::Auto,
                                fields,
                                maxlen: None,
                                keyspace,
                            })?;
                        }
                        _ => {}
                    }

                    Ok::<(), DbError>(())
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| match result {
                    Ok(()) => {
                        this.runner.complete_mutation(task_id, cx);
                        this.last_error = None;
                        this.reload_selected_value(cx);
                    }
                    Err(error) => {
                        this.runner.fail_mutation(task_id, error.to_string(), cx);
                        this.last_error = Some(error.to_string());
                        cx.notify();
                    }
                });
            })
            .ok();
        })
        .detach();
    }

    // -- New Key creation --

    fn handle_new_key_created(&mut self, event: NewKeyCreatedEvent, cx: &mut Context<Self>) {
        let Some(connection) = self.get_connection(cx) else {
            self.last_error = Some("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::KeyMutation,
            format!(
                "Create key {}",
                dbflux_core::truncate_string_safe(&event.key_name, 40)
            ),
            cx,
        );

        let keyspace = self.keyspace_index();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;

                    match event.value {
                        NewKeyValue::Simple(text) => {
                            let repr = if event.key_type == NewKeyType::Json {
                                ValueRepr::Json
                            } else {
                                ValueRepr::Text
                            };
                            api.set_key(&KeySetRequest {
                                key: event.key_name.clone(),
                                value: text.into_bytes(),
                                repr,
                                keyspace,
                                ttl_seconds: event.ttl,
                                condition: SetCondition::Always,
                            })?;
                        }
                        NewKeyValue::HashFields(fields) => {
                            api.hash_set(&HashSetRequest {
                                key: event.key_name.clone(),
                                fields,
                                keyspace,
                            })?;
                            if let Some(ttl) = event.ttl {
                                api.expire_key(&dbflux_core::KeyExpireRequest {
                                    key: event.key_name.clone(),
                                    ttl_seconds: ttl,
                                    keyspace,
                                })?;
                            }
                        }
                        NewKeyValue::ListMembers(members) => {
                            api.list_push(&ListPushRequest {
                                key: event.key_name.clone(),
                                values: members,
                                end: ListEnd::Tail,
                                keyspace,
                            })?;
                            if let Some(ttl) = event.ttl {
                                api.expire_key(&dbflux_core::KeyExpireRequest {
                                    key: event.key_name.clone(),
                                    ttl_seconds: ttl,
                                    keyspace,
                                })?;
                            }
                        }
                        NewKeyValue::SetMembers(members) => {
                            api.set_add(&SetAddRequest {
                                key: event.key_name.clone(),
                                members,
                                keyspace,
                            })?;
                            if let Some(ttl) = event.ttl {
                                api.expire_key(&dbflux_core::KeyExpireRequest {
                                    key: event.key_name.clone(),
                                    ttl_seconds: ttl,
                                    keyspace,
                                })?;
                            }
                        }
                        NewKeyValue::ZSetMembers(members) => {
                            api.zset_add(&ZSetAddRequest {
                                key: event.key_name.clone(),
                                members,
                                keyspace,
                            })?;
                            if let Some(ttl) = event.ttl {
                                api.expire_key(&dbflux_core::KeyExpireRequest {
                                    key: event.key_name.clone(),
                                    ttl_seconds: ttl,
                                    keyspace,
                                })?;
                            }
                        }
                        NewKeyValue::StreamFields(fields) => {
                            api.stream_add(&StreamAddRequest {
                                key: event.key_name.clone(),
                                id: StreamEntryId::Auto,
                                fields,
                                maxlen: None,
                                keyspace,
                            })?;

                            if let Some(ttl) = event.ttl {
                                api.expire_key(&dbflux_core::KeyExpireRequest {
                                    key: event.key_name.clone(),
                                    ttl_seconds: ttl,
                                    keyspace,
                                })?;
                            }
                        }
                    }

                    Ok::<(), DbError>(())
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| match result {
                    Ok(()) => {
                        this.runner.complete_mutation(task_id, cx);
                        this.last_error = None;
                        this.reload_keys(cx);
                    }
                    Err(error) => {
                        this.runner.fail_mutation(task_id, error.to_string(), cx);
                        this.last_error = Some(error.to_string());
                        cx.notify();
                    }
                });
            })
            .ok();
        })
        .detach();
    }

    // -- Helpers --

    fn get_connection(&self, cx: &Context<Self>) -> Option<Arc<dyn dbflux_core::Connection>> {
        self.app_state
            .read(cx)
            .connections()
            .get(&self.profile_id)
            .map(|conn| conn.connection.clone())
    }

    fn handle_copy_as_command(&self, target: KvMenuTarget, cx: &mut Context<Self>) {
        let Some(conn) = self.get_connection(cx) else {
            return;
        };
        let Some(generator) = conn.query_generator() else {
            return;
        };

        let keyspace = self.keyspace_index();
        let mutation = match target {
            KvMenuTarget::Key => self.build_key_mutation(keyspace),
            KvMenuTarget::Value => self.build_member_mutation(keyspace),
        };

        if let Some(mutation) = mutation
            && let Some(generated) = generator.generate_mutation(&mutation)
        {
            cx.write_to_clipboard(ClipboardItem::new_string(generated.text));
        }
    }

    fn build_key_mutation(&self, keyspace: Option<u32>) -> Option<MutationRequest> {
        let key = self.selected_key()?;
        let value = self.selected_value.as_ref()?;
        let key_type = value.entry.key_type?;

        match key_type {
            KeyType::String => {
                let mut request = KeySetRequest::new(key, value.value.clone());
                if let Some(ks) = keyspace {
                    request = request.with_keyspace(ks);
                }
                Some(MutationRequest::KeyValueSet(request))
            }

            KeyType::Hash => {
                let json = serde_json::from_slice::<serde_json::Value>(&value.value).ok()?;
                let serde_json::Value::Object(map) = json else {
                    return None;
                };

                let fields: Vec<(String, String)> = map
                    .into_iter()
                    .map(|(field, val)| {
                        let val_str = match val {
                            serde_json::Value::String(s) => s,
                            other => other.to_string(),
                        };
                        (field, val_str)
                    })
                    .collect();

                if fields.is_empty() {
                    return None;
                }

                Some(MutationRequest::KeyValueHashSet(HashSetRequest {
                    key,
                    fields,
                    keyspace,
                }))
            }

            KeyType::Set => {
                let json = serde_json::from_slice::<serde_json::Value>(&value.value).ok()?;
                let serde_json::Value::Array(items) = json else {
                    return None;
                };

                let members: Vec<String> = items
                    .into_iter()
                    .map(|item| match item {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    })
                    .collect();

                if members.is_empty() {
                    return None;
                }

                Some(MutationRequest::KeyValueSetAdd(SetAddRequest {
                    key,
                    members,
                    keyspace,
                }))
            }

            KeyType::List => {
                let json = serde_json::from_slice::<serde_json::Value>(&value.value).ok()?;
                let serde_json::Value::Array(items) = json else {
                    return None;
                };

                let values: Vec<String> = items
                    .into_iter()
                    .map(|item| match item {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    })
                    .collect();

                if values.is_empty() {
                    return None;
                }

                Some(MutationRequest::KeyValueListPush(ListPushRequest {
                    key,
                    values,
                    end: ListEnd::Tail,
                    keyspace,
                }))
            }

            KeyType::SortedSet => {
                let json = serde_json::from_slice::<serde_json::Value>(&value.value).ok()?;
                let serde_json::Value::Array(items) = json else {
                    return None;
                };

                let members: Vec<(String, f64)> = items
                    .into_iter()
                    .filter_map(|item| {
                        let obj = item.as_object()?;
                        let member = obj
                            .get("member")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())?;
                        let score = obj
                            .get("score")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        Some((member, score))
                    })
                    .collect();

                if members.is_empty() {
                    return None;
                }

                Some(MutationRequest::KeyValueZSetAdd(ZSetAddRequest {
                    key,
                    members,
                    keyspace,
                }))
            }

            KeyType::Stream | KeyType::Bytes | KeyType::Json | KeyType::Unknown => None,
        }
    }

    fn build_member_mutation(&self, keyspace: Option<u32>) -> Option<MutationRequest> {
        let key = self.selected_key()?;
        let key_type = self.selected_key_type()?;
        let member_idx = self.selected_member_index?;
        let member = self.cached_members.get(member_idx)?;

        match key_type {
            KeyType::Hash => {
                let field = member.field.clone()?;
                Some(MutationRequest::KeyValueHashSet(HashSetRequest {
                    key,
                    fields: vec![(field, member.display.clone())],
                    keyspace,
                }))
            }
            KeyType::Set => Some(MutationRequest::KeyValueSetAdd(SetAddRequest {
                key,
                members: vec![member.display.clone()],
                keyspace,
            })),
            KeyType::SortedSet => {
                let score = member.score.unwrap_or(0.0);
                Some(MutationRequest::KeyValueZSetAdd(ZSetAddRequest {
                    key,
                    members: vec![(member.display.clone(), score)],
                    keyspace,
                }))
            }
            KeyType::List => Some(MutationRequest::KeyValueListPush(ListPushRequest {
                key,
                values: vec![member.display.clone()],
                end: ListEnd::Tail,
                keyspace,
            })),
            KeyType::Stream => {
                let entry_id = member.entry_id.as_ref()?;
                let fields: Vec<(String, String)> = if let Some(field) = &member.field {
                    vec![(field.clone(), member.display.clone())]
                } else {
                    vec![("value".to_string(), member.display.clone())]
                };
                Some(MutationRequest::KeyValueStreamAdd(StreamAddRequest {
                    key,
                    id: StreamEntryId::Explicit(entry_id.clone()),
                    fields,
                    maxlen: None,
                    keyspace,
                }))
            }
            KeyType::String => {
                let value_bytes = self
                    .selected_value
                    .as_ref()
                    .map(|v| v.value.clone())
                    .unwrap_or_default();
                let mut request = KeySetRequest::new(key, value_bytes);
                if let Some(ks) = keyspace {
                    request = request.with_keyspace(ks);
                }
                Some(MutationRequest::KeyValueSet(request))
            }
            KeyType::Bytes | KeyType::Json | KeyType::Unknown => None,
        }
    }

    fn rebuild_cached_members(&mut self, cx: &mut Context<Self>) {
        self.cached_members = match &self.selected_value {
            Some(value) => parse_members(value),
            None => Vec::new(),
        };

        if self.value_view_mode == KvValueViewMode::Document && self.supports_document_view() {
            self.rebuild_document_tree(cx);
        }
    }

    fn is_structured_type(&self) -> bool {
        matches!(
            self.selected_key_type(),
            Some(
                KeyType::Hash | KeyType::List | KeyType::Set | KeyType::SortedSet | KeyType::Stream
            )
        )
    }

    fn is_stream_type(&self) -> bool {
        matches!(self.selected_key_type(), Some(KeyType::Stream))
    }

    fn needs_value_column(&self) -> bool {
        matches!(
            self.selected_key_type(),
            Some(KeyType::Hash | KeyType::SortedSet | KeyType::Stream)
        )
    }

    fn supports_document_view(&self) -> bool {
        matches!(
            self.selected_key_type(),
            Some(KeyType::Hash | KeyType::Stream)
        )
    }

    fn is_document_view_active(&self) -> bool {
        self.value_view_mode == KvValueViewMode::Document
            && self.supports_document_view()
            && self.document_tree_state.is_some()
    }

    fn toggle_value_view_mode(&mut self, cx: &mut Context<Self>) {
        if !self.supports_document_view() {
            return;
        }

        self.value_view_mode = match self.value_view_mode {
            KvValueViewMode::Table => KvValueViewMode::Document,
            KvValueViewMode::Document => KvValueViewMode::Table,
        };

        if self.value_view_mode == KvValueViewMode::Document {
            self.rebuild_document_tree(cx);
        } else {
            self.document_tree = None;
            self.document_tree_state = None;
            self._document_tree_subscription = None;
        }

        cx.notify();
    }

    fn rebuild_document_tree(&mut self, cx: &mut Context<Self>) {
        let entries = self.members_to_tree_values();
        if entries.is_empty() {
            self.document_tree = None;
            self.document_tree_state = None;
            self._document_tree_subscription = None;
            return;
        }

        let tree_state = cx.new(|cx| {
            let mut state = DocumentTreeState::new(cx);
            state.load_from_values(entries, cx);
            state
        });

        let tree = cx.new(|cx| DocumentTree::new("kv-document-tree", tree_state.clone(), cx));

        let subscription = cx.subscribe(
            &tree_state,
            |this, _state, event: &DocumentTreeEvent, cx| match event {
                DocumentTreeEvent::Focused => {
                    cx.emit(KeyValueDocumentEvent::RequestFocus);
                }
                DocumentTreeEvent::InlineEditCommitted { node_id, new_value } => {
                    this.handle_tree_inline_edit(node_id, new_value, cx);
                }
                _ => {}
            },
        );

        self.document_tree_state = Some(tree_state);
        self.document_tree = Some(tree);
        self._document_tree_subscription = Some(subscription);
    }

    fn handle_tree_inline_edit(
        &mut self,
        node_id: &NodeId,
        new_value: &str,
        cx: &mut Context<Self>,
    ) {
        let Some(doc_index) = node_id.doc_index() else {
            return;
        };

        if !matches!(self.selected_key_type(), Some(KeyType::Hash)) {
            return;
        }

        let Some(member) = self.cached_members.get(doc_index).cloned() else {
            return;
        };
        let Some(field_name) = &member.field else {
            return;
        };

        if new_value == member.display {
            return;
        }

        let field_name = field_name.clone();
        let new_value = new_value.to_string();

        if let Some(cached) = self.cached_members.get_mut(doc_index) {
            cached.display = new_value.clone();
        }
        self.rebuild_document_tree(cx);

        let Some(key) = self.selected_key() else {
            return;
        };
        let Some(connection) = self.get_connection(cx) else {
            return;
        };

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::KeyMutation,
            format!(
                "HSET {} {}",
                dbflux_core::truncate_string_safe(&key, 30),
                dbflux_core::truncate_string_safe(&field_name, 30)
            ),
            cx,
        );

        let keyspace = self.keyspace_index();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;

                    api.hash_set(&HashSetRequest {
                        key,
                        fields: vec![(field_name, new_value)],
                        keyspace,
                    })?;

                    Ok::<(), DbError>(())
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| match result {
                    Ok(()) => {
                        this.runner.complete_mutation(task_id, cx);
                        this.last_error = None;
                        this.reload_selected_value(cx);
                    }
                    Err(error) => {
                        this.runner.fail_mutation(task_id, error.to_string(), cx);
                        this.last_error = Some(error.to_string());
                        cx.notify();
                    }
                });
            })
            .ok();
        })
        .detach();
    }

    /// Convert cached members into `(label, Value)` pairs for the document tree.
    fn members_to_tree_values(&self) -> Vec<(String, Value)> {
        let key_type = self.selected_key_type();

        match key_type {
            Some(KeyType::Hash) => self
                .cached_members
                .iter()
                .map(|m| {
                    let label = m.field.clone().unwrap_or_else(|| m.display.clone());
                    let val = Value::Text(m.display.clone());
                    (label, val)
                })
                .collect(),

            Some(KeyType::Stream) => self
                .cached_members
                .iter()
                .map(|m| {
                    let fields_val = m
                        .field
                        .as_deref()
                        .map(parse_json_to_value)
                        .unwrap_or(Value::Null);

                    (m.display.clone(), fields_val)
                })
                .collect(),

            _ => Vec::new(),
        }
    }
}

impl EventEmitter<KeyValueDocumentEvent> for KeyValueDocument {}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

impl Render for KeyValueDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Handle deferred modal opens before borrowing theme
        if self.pending_open_new_key_modal {
            self.pending_open_new_key_modal = false;
            self.new_key_modal
                .update(cx, |modal, cx| modal.open(window, cx));
        }

        if let Some(key_type) = self.pending_open_add_member_modal.take() {
            self.add_member_modal
                .update(cx, |modal, cx| modal.open(key_type, window, cx));
        }

        let theme = cx.theme();

        let error_message = self.last_error.clone();
        let is_structured = self.is_structured_type();
        let needs_value_col = self.needs_value_column();

        // Delete confirmation state (capture before building UI)
        let has_pending_delete =
            self.pending_key_delete.is_some() || self.pending_member_delete.is_some();
        let (delete_title, delete_message) = if let Some(pending) = &self.pending_key_delete {
            (
                "Delete key?".to_string(),
                format!("Delete \"{}\"? This action cannot be undone.", pending.key),
            )
        } else if let Some(pending) = &self.pending_member_delete {
            (
                "Delete member?".to_string(),
                format!(
                    "Delete \"{}\"? This action cannot be undone.",
                    pending.member_display
                ),
            )
        } else {
            (String::new(), String::new())
        };

        let filter_text = self
            .members_filter_input
            .read(cx)
            .value()
            .trim()
            .to_ascii_lowercase();
        let filtered_members: Vec<(usize, &MemberEntry)> = self
            .cached_members
            .iter()
            .enumerate()
            .filter(|(_, m)| {
                filter_text.is_empty() || m.display.to_ascii_lowercase().contains(&filter_text)
            })
            .collect();

        // -- Right panel --
        let right_panel = if let Some(value) = &self.selected_value {
            let key_name = value.entry.key.clone();
            let type_label = value
                .entry
                .key_type
                .map(|t| key_type_label(t).to_string())
                .unwrap_or_else(|| "Unknown".to_string());
            let ttl_color = match self.ttl_state {
                TtlState::Expired => theme.danger,
                TtlState::Missing => theme.warning,
                _ => theme.muted_foreground,
            };

            let size_label = value
                .entry
                .size_bytes
                .map(|s| format!("{} B", s))
                .unwrap_or_default();

            let mut panel = div().flex_1().flex().flex_col().overflow_hidden();

            // Header bar
            panel = panel.child(
                div()
                    .h(Heights::TOOLBAR)
                    .flex()
                    .items_center()
                    .justify_between()
                    .px(Spacing::MD)
                    .border_b_1()
                    .border_l_1()
                    .border_color(theme.border)
                    .bg(theme.tab_bar)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(
                                svg()
                                    .path(AppIcon::KeyRound.path())
                                    .size(Heights::ICON_SM)
                                    .text_color(theme.muted_foreground),
                            )
                            .child(div().text_size(FontSizes::BASE).child(key_name)),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .when(is_structured, |d| {
                                d.child(
                                    icon_button_base("kv-add-member", AppIcon::Plus, theme)
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            cx.listener(|this, _, _, cx| {
                                                if let Some(key_type) = this.selected_key_type() {
                                                    this.pending_open_add_member_modal =
                                                        Some(key_type);
                                                    cx.notify();
                                                }
                                            }),
                                        ),
                                )
                            })
                            .when(self.supports_document_view(), |d| {
                                let toggle_icon = match self.value_view_mode {
                                    KvValueViewMode::Table => AppIcon::Braces,
                                    KvValueViewMode::Document => AppIcon::Table,
                                };
                                d.child(
                                    icon_button_base("kv-toggle-view", toggle_icon, theme)
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            cx.listener(|this, _, _, cx| {
                                                this.toggle_value_view_mode(cx);
                                            }),
                                        ),
                                )
                            })
                            .child(
                                icon_button_base("kv-refresh-val", AppIcon::RefreshCcw, theme)
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            this.reload_selected_value(cx);
                                        }),
                                    ),
                            )
                            .child(
                                icon_button_base("kv-delete-key", AppIcon::Delete, theme)
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            this.request_delete_key(cx);
                                        }),
                                    ),
                            ),
                    ),
            );

            // Metadata row
            panel = panel.child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::LG)
                    .px(Spacing::MD)
                    .py(Spacing::XS)
                    .border_b_1()
                    .border_l_1()
                    .border_color(theme.border)
                    .text_size(FontSizes::XS)
                    .text_color(theme.muted_foreground)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .child(type_label),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .child(
                                svg()
                                    .path(AppIcon::Clock.path())
                                    .size(Heights::ICON_SM)
                                    .text_color(ttl_color),
                            )
                            .text_color(ttl_color)
                            .child(self.ttl_display.clone()),
                    )
                    .child(size_label),
            );

            if is_structured
                && self.value_view_mode == KvValueViewMode::Document
                && self.supports_document_view()
            {
                // -- Document tree view for Hash / Stream --
                if let Some(tree) = &self.document_tree {
                    panel = panel.child(
                        div()
                            .flex_1()
                            .overflow_hidden()
                            .border_l_1()
                            .border_color(theme.border)
                            .child(tree.clone()),
                    );
                } else {
                    panel = panel.child(
                        div()
                            .flex_1()
                            .flex()
                            .items_center()
                            .justify_center()
                            .border_l_1()
                            .border_color(theme.border)
                            .text_color(theme.muted_foreground)
                            .text_size(FontSizes::SM)
                            .child("No data"),
                    );
                }
            } else if is_structured {
                // -- Table view for structured types --

                // Members filter
                panel = panel.child(
                    div()
                        .w_full()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .px(Spacing::MD)
                        .py(Spacing::XS)
                        .border_b_1()
                        .border_l_1()
                        .border_color(theme.border)
                        .child(
                            svg()
                                .path(AppIcon::Search.path())
                                .size(Heights::ICON_SM)
                                .text_color(theme.muted_foreground),
                        )
                        .child(
                            div()
                                .flex_1()
                                .on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(|this, _, _, cx| {
                                        this.focus_mode = KeyValueFocusMode::TextInput;
                                        cx.stop_propagation();
                                        cx.notify();
                                    }),
                                )
                                .child(
                                    Input::new(&self.members_filter_input)
                                        .small()
                                        .cleanable(true)
                                        .w_full(),
                                ),
                        ),
                );

                // Members list header
                let mut header = div()
                    .flex()
                    .items_center()
                    .px(Spacing::MD)
                    .h(Heights::ROW_COMPACT)
                    .border_b_1()
                    .border_l_1()
                    .border_color(theme.border)
                    .bg(theme.secondary)
                    .text_size(FontSizes::XS)
                    .text_color(theme.muted_foreground);

                let is_stream = self.is_stream_type();
                header = header.child(div().w(px(30.0)).child("#"));
                header = header.child(div().flex_1().child(if is_stream { "ID" } else { "Value" }));
                if needs_value_col {
                    header = header.child(div().w(px(200.0)).child(if is_stream {
                        "Fields"
                    } else {
                        "Field/Score"
                    }));
                }
                header = header.child(div().w(Heights::ICON_MD));

                panel = panel.child(header);

                // Members list
                let mut members_list = div()
                    .flex_1()
                    .overflow_y_scrollbar()
                    .border_l_1()
                    .border_color(theme.border);

                for (original_index, member) in &filtered_members {
                    let idx = *original_index;
                    let is_editing = self.editing_member_index == Some(idx);
                    let is_selected = self.focus_mode == KeyValueFocusMode::ValuePanel
                        && self.selected_member_index == Some(idx);

                    let mut row = div()
                        .flex()
                        .items_center()
                        .px(Spacing::MD)
                        .h(Heights::ROW)
                        .border_b_1()
                        .border_color(theme.border)
                        .text_size(FontSizes::SM)
                        .when(is_selected, |d| d.bg(theme.list_active))
                        .when(!is_selected, |d| d.hover(|d| d.bg(theme.list_active)))
                        .on_mouse_down(
                            MouseButton::Right,
                            cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                                cx.stop_propagation();
                                this.focus_mode = KeyValueFocusMode::ValuePanel;
                                this.selected_member_index = Some(idx);
                                cx.emit(KeyValueDocumentEvent::RequestFocus);
                                this.open_context_menu(
                                    KvMenuTarget::Value,
                                    event.position,
                                    window,
                                    cx,
                                );
                            }),
                        );

                    row = row.child(
                        div()
                            .w(px(30.0))
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child(format!("{}", idx)),
                    );

                    if is_editing {
                        if let Some(input) = &self.member_edit_input {
                            row =
                                row.child(div().flex_1().child(Input::new(input).small().w_full()));
                            if let Some(score_input) = &self.member_edit_score_input {
                                row = row.child(
                                    div()
                                        .w(px(200.0))
                                        .child(Input::new(score_input).small().w_full()),
                                );
                            }
                        }
                    } else {
                        let value_cell = div().flex_1().child(member.display.clone());

                        row = row.child(if is_stream {
                            value_cell
                        } else {
                            value_cell.cursor_pointer().on_mouse_down(
                                MouseButton::Left,
                                cx.listener(move |this, _, window, cx| {
                                    cx.stop_propagation();
                                    this.start_member_edit(idx, window, cx);
                                }),
                            )
                        });

                        if needs_value_col {
                            row = row.child(
                                div().w(px(200.0)).text_color(theme.muted_foreground).child(
                                    member
                                        .field
                                        .clone()
                                        .or(member.score.map(|s| s.to_string()))
                                        .unwrap_or_default(),
                                ),
                            );
                        }

                        row = row.child(
                            icon_button_base(
                                ElementId::Name(format!("del-member-{}", idx).into()),
                                AppIcon::Delete,
                                theme,
                            )
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(move |this, _, _, cx| {
                                    cx.stop_propagation();
                                    this.request_delete_member(idx, cx);
                                }),
                            ),
                        );
                    }

                    members_list = members_list.child(row);
                }

                panel = panel.child(members_list);
            } else if let Some(input) = &self.string_edit_input {
                // Inline editing for String/JSON values
                panel = panel.child(
                    div()
                        .flex_1()
                        .overflow_y_scrollbar()
                        .p(Spacing::MD)
                        .border_l_1()
                        .border_color(theme.border)
                        .child(Input::new(input).small().w_full()),
                );
            } else {
                // Read-only value preview for String/JSON/Binary
                let is_editable = matches!(
                    value.entry.key_type,
                    Some(KeyType::String) | Some(KeyType::Json)
                );
                let value_preview = render_value_preview(value);

                panel = panel.child(
                    div()
                        .flex_1()
                        .overflow_y_scrollbar()
                        .p(Spacing::MD)
                        .border_l_1()
                        .border_color(theme.border)
                        .text_size(FontSizes::SM)
                        .when(is_editable, |d| {
                            d.cursor_pointer().on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, window, cx| {
                                    this.start_string_edit(window, cx);
                                }),
                            )
                        })
                        .on_mouse_down(
                            MouseButton::Right,
                            cx.listener(|this, event: &MouseDownEvent, window, cx| {
                                cx.stop_propagation();
                                this.focus_mode = KeyValueFocusMode::ValuePanel;
                                cx.emit(KeyValueDocumentEvent::RequestFocus);
                                this.open_context_menu(
                                    KvMenuTarget::Value,
                                    event.position,
                                    window,
                                    cx,
                                );
                            }),
                        )
                        .child(value_preview)
                        .when(is_editable, |d| {
                            d.child(
                                div()
                                    .pt(Spacing::SM)
                                    .text_size(FontSizes::XS)
                                    .text_color(theme.muted_foreground)
                                    .child("Click or press Enter to edit"),
                            )
                        }),
                );
            }

            panel.into_any_element()
        } else {
            div()
                .flex_1()
                .flex()
                .items_center()
                .justify_center()
                .border_l_1()
                .border_color(theme.border)
                .text_color(theme.muted_foreground)
                .text_size(FontSizes::SM)
                .child(if self.runner.is_primary_active() {
                    "Loading..."
                } else {
                    "Select a key to inspect"
                })
                .into_any_element()
        };

        let refresh_label = if self.refresh_policy.is_auto() {
            self.refresh_policy.label()
        } else {
            "Refresh"
        };

        // -- Left panel --
        let left_panel = div()
            .w_1_3()
            .min_w(px(240.0))
            .flex()
            .flex_col()
            .overflow_hidden()
            // Toolbar
            .child(
                div()
                    .h(Heights::TOOLBAR)
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .px(Spacing::SM)
                    .border_b_1()
                    .border_color(theme.border)
                    .bg(theme.tab_bar)
                    .child(
                        svg()
                            .path(AppIcon::Search.path())
                            .size(Heights::ICON_SM)
                            .text_color(theme.muted_foreground),
                    )
                    .child(
                        div()
                            .flex_1()
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, _, cx| {
                                    this.focus_mode = KeyValueFocusMode::TextInput;
                                    cx.stop_propagation();
                                    cx.notify();
                                }),
                            )
                            .child(
                                Input::new(&self.filter_input)
                                    .small()
                                    .cleanable(true)
                                    .w_full(),
                            ),
                    )
                    .child(
                        icon_button_base("kv-add", AppIcon::Plus, theme).on_mouse_down(
                            MouseButton::Left,
                            cx.listener(|this, _, _, cx| {
                                this.pending_open_new_key_modal = true;
                                cx.notify();
                            }),
                        ),
                    )
                    .child(
                        div()
                            .id("kv-refresh-control")
                            .h(Heights::BUTTON)
                            .flex()
                            .items_center()
                            .gap_0()
                            .rounded(Radii::SM)
                            .bg(theme.background)
                            .border_1()
                            .border_color(theme.input)
                            .child(
                                div()
                                    .id("kv-refresh-action")
                                    .h_full()
                                    .px(Spacing::SM)
                                    .flex()
                                    .items_center()
                                    .gap_1()
                                    .text_size(FontSizes::SM)
                                    .text_color(theme.foreground)
                                    .cursor_pointer()
                                    .hover(|d| d.bg(theme.accent.opacity(0.08)))
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            if this.runner.is_primary_active() {
                                                this.runner.cancel_primary(cx);
                                                this.last_error = None;
                                                cx.notify();
                                            } else {
                                                this.reload_keys(cx);
                                            }
                                        }),
                                    )
                                    .child(
                                        svg()
                                            .path(if self.runner.is_primary_active() {
                                                AppIcon::Loader.path()
                                            } else if self.refresh_policy.is_auto() {
                                                AppIcon::Clock.path()
                                            } else {
                                                AppIcon::RefreshCcw.path()
                                            })
                                            .size(Heights::ICON_SM)
                                            .text_color(theme.foreground),
                                    )
                                    .child(refresh_label),
                            )
                            .child(div().w(px(1.0)).h_full().bg(theme.input))
                            .child(
                                div()
                                    .w(px(28.0))
                                    .h_full()
                                    .child(self.refresh_dropdown.clone()),
                            ),
                    ),
            )
            // Pagination bar
            .child({
                let can_prev = self.can_go_prev();
                let can_next = self.can_go_next();
                let current_page = self.current_page;
                let key_count = self.keys.len();

                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .h(Heights::ROW_COMPACT)
                    .px(Spacing::SM)
                    .border_b_1()
                    .border_color(theme.border)
                    .bg(theme.tab_bar)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child(
                                svg()
                                    .path(AppIcon::Rows3.path())
                                    .size_3()
                                    .text_color(theme.muted_foreground),
                            )
                            .child(if self.runner.is_primary_active() {
                                "Loading...".to_string()
                            } else {
                                format!("{} keys", key_count)
                            }),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(
                                div()
                                    .id("kv-prev-page")
                                    .flex()
                                    .items_center()
                                    .gap_1()
                                    .px(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .text_size(FontSizes::XS)
                                    .when(can_prev, |d| {
                                        d.cursor_pointer()
                                            .text_color(theme.foreground)
                                            .hover(|d| d.bg(theme.secondary))
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.go_prev_page(cx);
                                            }))
                                    })
                                    .when(!can_prev, |d| {
                                        d.text_color(theme.muted_foreground).opacity(0.5)
                                    })
                                    .child(
                                        svg()
                                            .path(AppIcon::ChevronLeft.path())
                                            .size_3()
                                            .text_color(if can_prev {
                                                theme.foreground
                                            } else {
                                                theme.muted_foreground
                                            }),
                                    )
                                    .child("Prev"),
                            )
                            .child(
                                div()
                                    .text_size(FontSizes::XS)
                                    .text_color(theme.muted_foreground)
                                    .child(format!("Page {}", current_page)),
                            )
                            .child(
                                div()
                                    .id("kv-next-page")
                                    .flex()
                                    .items_center()
                                    .gap_1()
                                    .px(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .text_size(FontSizes::XS)
                                    .when(can_next, |d| {
                                        d.cursor_pointer()
                                            .text_color(theme.foreground)
                                            .hover(|d| d.bg(theme.secondary))
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.go_next_page(cx);
                                            }))
                                    })
                                    .when(!can_next, |d| {
                                        d.text_color(theme.muted_foreground).opacity(0.5)
                                    })
                                    .child("Next")
                                    .child(
                                        svg()
                                            .path(AppIcon::ChevronRight.path())
                                            .size_3()
                                            .text_color(if can_next {
                                                theme.foreground
                                            } else {
                                                theme.muted_foreground
                                            }),
                                    ),
                            ),
                    )
            })
            .when_some(error_message, |this, message| {
                this.child(
                    div()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .text_size(FontSizes::XS)
                        .text_color(theme.muted_foreground)
                        .child(format!("Error: {}", message)),
                )
            })
            // Keys list
            .child(div().flex_1().overflow_y_scrollbar().children(
                self.keys.iter().enumerate().map(|(index, key)| {
                    let selected = self.selected_index == Some(index);
                    let is_renaming = self.renaming_index == Some(index);
                    let row_bg = if selected {
                        theme.list_active
                    } else {
                        theme.transparent
                    };

                    let (icon, icon_color) = key_type_icon(key.key_type);

                    let mut row = div()
                        .h(Heights::ROW)
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .px(Spacing::SM)
                        .bg(row_bg)
                        .border_b_1()
                        .border_color(theme.border)
                        .cursor_pointer()
                        .hover(|d| d.bg(theme.list_active))
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |this, _, _, cx| {
                                this.focus_mode = KeyValueFocusMode::List;
                                this.select_index(index, cx);
                            }),
                        )
                        .on_mouse_down(
                            MouseButton::Right,
                            cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                                cx.stop_propagation();
                                this.focus_mode = KeyValueFocusMode::List;
                                this.select_index(index, cx);
                                cx.emit(KeyValueDocumentEvent::RequestFocus);
                                this.open_context_menu(
                                    KvMenuTarget::Key,
                                    event.position,
                                    window,
                                    cx,
                                );
                            }),
                        );

                    row = row.child(
                        svg()
                            .path(icon.path())
                            .size(Heights::ICON_SM)
                            .text_color(icon_color),
                    );

                    if is_renaming {
                        if let Some(input) = &self.rename_input {
                            row =
                                row.child(div().flex_1().child(Input::new(input).small().w_full()));
                        }
                    } else {
                        row = row.child(
                            div()
                                .flex_1()
                                .text_size(FontSizes::SM)
                                .overflow_hidden()
                                .text_ellipsis()
                                .whitespace_nowrap()
                                .child(key.key.clone()),
                        );

                        row = row.child(
                            div()
                                .text_size(FontSizes::XS)
                                .text_color(theme.muted_foreground)
                                .child(
                                    key.key_type
                                        .map(|t| key_type_label(t).to_string())
                                        .unwrap_or_else(|| "?".to_string()),
                                ),
                        );
                    }

                    row
                }),
            ));

        // -- Compose --
        let this_entity = cx.entity().clone();

        div()
            .size_full()
            .track_focus(&self.focus_handle)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.focus_mode = KeyValueFocusMode::List;
                    cx.emit(KeyValueDocumentEvent::RequestFocus);
                    cx.notify();
                }),
            )
            .flex()
            .child(
                canvas(
                    move |bounds, _, cx| {
                        this_entity.update(cx, |this, _| {
                            this.panel_origin = bounds.origin;
                        });
                    },
                    |_, _, _, _| {},
                )
                .absolute()
                .size_full(),
            )
            .child(left_panel)
            .child(right_panel)
            .when(self.new_key_modal.read(cx).is_visible(), |d| {
                d.child(self.new_key_modal.clone())
            })
            .when(self.add_member_modal.read(cx).is_visible(), |d| {
                d.child(self.add_member_modal.clone())
            })
            .when(has_pending_delete, |d| {
                d.child(render_delete_confirm_modal(
                    &delete_title,
                    &delete_message,
                    cx,
                ))
            })
            .when_some(self.context_menu.as_ref(), |d, menu| {
                d.child(render_kv_context_menu(
                    menu,
                    &self.context_menu_focus,
                    self.panel_origin,
                    cx,
                ))
            })
    }
}

// ---------------------------------------------------------------------------
// Rendering helpers
// ---------------------------------------------------------------------------

fn render_delete_confirm_modal(
    title: &str,
    message: &str,
    cx: &mut Context<KeyValueDocument>,
) -> impl IntoElement {
    let theme = cx.theme();
    let btn_hover = theme.muted;

    div()
        .id("kv-delete-modal-overlay")
        .absolute()
        .inset_0()
        .bg(gpui::hsla(0.0, 0.0, 0.0, 0.5))
        .flex()
        .items_center()
        .justify_center()
        .on_mouse_down(MouseButton::Left, |_, _, cx| {
            cx.stop_propagation();
        })
        .child(
            div()
                .bg(theme.background)
                .border_1()
                .border_color(theme.border)
                .rounded(Radii::MD)
                .p(Spacing::MD)
                .min_w(px(300.0))
                .flex()
                .flex_col()
                .gap(Spacing::MD)
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .child(
                            svg()
                                .path(AppIcon::TriangleAlert.path())
                                .size_5()
                                .text_color(theme.warning),
                        )
                        .child(
                            div()
                                .text_size(FontSizes::SM)
                                .font_weight(FontWeight::SEMIBOLD)
                                .text_color(theme.foreground)
                                .child(title.to_string()),
                        ),
                )
                .child(
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(theme.muted_foreground)
                        .child(message.to_string()),
                )
                .child(
                    div()
                        .flex()
                        .justify_end()
                        .gap(Spacing::SM)
                        .child(
                            div()
                                .id("kv-delete-cancel-btn")
                                .flex()
                                .items_center()
                                .gap_1()
                                .px(Spacing::SM)
                                .py(Spacing::XS)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(theme.muted_foreground)
                                .bg(theme.secondary)
                                .hover(move |d| d.bg(btn_hover))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.pending_key_delete = None;
                                    this.pending_member_delete = None;
                                    cx.notify();
                                }))
                                .child(
                                    svg()
                                        .path(AppIcon::X.path())
                                        .size_4()
                                        .text_color(theme.muted_foreground),
                                )
                                .child("Cancel"),
                        )
                        .child(
                            div()
                                .id("kv-delete-confirm-btn")
                                .flex()
                                .items_center()
                                .gap_1()
                                .px(Spacing::SM)
                                .py(Spacing::XS)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(theme.background)
                                .bg(theme.danger)
                                .hover(|d| d.opacity(0.9))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    if this.pending_key_delete.is_some() {
                                        this.confirm_delete_key(cx);
                                    } else if this.pending_member_delete.is_some() {
                                        this.confirm_delete_member(cx);
                                    }
                                }))
                                .child(
                                    svg()
                                        .path(AppIcon::Delete.path())
                                        .size_4()
                                        .text_color(theme.background),
                                )
                                .child("Delete"),
                        ),
                ),
        )
}

fn render_kv_context_menu(
    menu: &KvContextMenu,
    menu_focus: &FocusHandle,
    panel_origin: Point<Pixels>,
    cx: &mut Context<KeyValueDocument>,
) -> impl IntoElement {
    let theme = cx.theme();
    let menu_width = px(180.0);
    let menu_x = menu.position.x - panel_origin.x;
    let menu_y = menu.position.y - panel_origin.y;
    let selected_index = menu.selected_index;

    let menu_items: Vec<AnyElement> = menu
        .items
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            let is_selected = idx == selected_index;
            let is_danger = item.is_danger;
            let label = item.label;
            let icon = item.icon;
            let action = item.action;

            div()
                .id(SharedString::from(label))
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .h(Heights::ROW_COMPACT)
                .px(Spacing::SM)
                .mx(Spacing::XS)
                .rounded(Radii::SM)
                .cursor_pointer()
                .text_size(FontSizes::SM)
                .text_color(if is_danger {
                    theme.danger
                } else {
                    theme.foreground
                })
                .when(is_selected, |d| {
                    d.bg(if is_danger {
                        theme.danger.opacity(0.1)
                    } else {
                        theme.accent
                    })
                    .text_color(if is_danger {
                        theme.danger
                    } else {
                        theme.accent_foreground
                    })
                })
                .when(!is_selected, |d| {
                    d.hover(|d| {
                        d.bg(if is_danger {
                            theme.danger.opacity(0.1)
                        } else {
                            theme.secondary
                        })
                    })
                })
                .on_mouse_move(cx.listener(move |this, _, _, cx| {
                    if let Some(ref mut m) = this.context_menu
                        && m.selected_index != idx
                    {
                        m.selected_index = idx;
                        cx.notify();
                    }
                }))
                .on_click(cx.listener(move |this, _, window, cx| {
                    if let Some(m) = this.context_menu.take() {
                        let target = m.target;
                        this.execute_menu_action(action, target, window, cx);
                    }
                }))
                .child(svg().path(icon.path()).size_4().text_color(if is_danger {
                    theme.danger
                } else if is_selected {
                    theme.accent_foreground
                } else {
                    theme.muted_foreground
                }))
                .child(label)
                .into_any_element()
        })
        .collect();

    deferred(
        div()
            .id("kv-context-menu-overlay")
            .absolute()
            .top_0()
            .left_0()
            .size_full()
            .track_focus(menu_focus)
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                use crate::keymap::{KeyChord, default_keymap};

                let chord = KeyChord::from_gpui(&event.keystroke);
                let keymap = default_keymap();

                if let Some(cmd) = keymap.resolve(ContextId::ContextMenu, &chord)
                    && this.dispatch_menu_command(cmd, window, cx)
                {
                    cx.stop_propagation();
                }
            }))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, window, cx| {
                    this.close_context_menu(window, cx);
                }),
            )
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(|this, _, window, cx| {
                    this.close_context_menu(window, cx);
                }),
            )
            .child(
                div()
                    .id("kv-context-menu")
                    .absolute()
                    .left(menu_x)
                    .top(menu_y)
                    .w(menu_width)
                    .bg(theme.popover)
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::MD)
                    .shadow_lg()
                    .py(Spacing::XS)
                    .occlude()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    .children(menu_items),
            ),
    )
    .with_priority(1)
}

fn icon_button_base(
    id: impl Into<ElementId>,
    icon: AppIcon,
    theme: &gpui_component::Theme,
) -> Stateful<Div> {
    let foreground = theme.muted_foreground;
    let hover_bg = theme.secondary;

    div()
        .id(id.into())
        .w(Heights::ICON_MD)
        .h(Heights::ICON_MD)
        .flex()
        .items_center()
        .justify_center()
        .rounded(Radii::SM)
        .cursor_pointer()
        .hover(move |d| d.bg(hover_bg))
        .child(
            svg()
                .path(icon.path())
                .size(Heights::ICON_SM)
                .text_color(foreground),
        )
}

fn key_type_icon(key_type: Option<KeyType>) -> (AppIcon, Hsla) {
    match key_type {
        Some(KeyType::String) | Some(KeyType::Bytes) => {
            (AppIcon::CaseSensitive, hsla(0.5, 0.6, 0.6, 1.0))
        }
        Some(KeyType::Hash) => (AppIcon::Hash, hsla(0.75, 0.6, 0.6, 1.0)),
        Some(KeyType::List) => (AppIcon::Rows3, hsla(0.6, 0.6, 0.6, 1.0)),
        Some(KeyType::Set) => (AppIcon::Box, hsla(0.08, 0.7, 0.6, 1.0)),
        Some(KeyType::SortedSet) => (AppIcon::ArrowUp, hsla(0.08, 0.7, 0.6, 1.0)),
        Some(KeyType::Json) => (AppIcon::Braces, hsla(0.35, 0.6, 0.6, 1.0)),
        Some(KeyType::Stream) => (AppIcon::Zap, hsla(0.15, 0.7, 0.6, 1.0)),
        _ => (AppIcon::KeyRound, hsla(0.0, 0.0, 0.5, 1.0)),
    }
}

fn key_type_label(key_type: KeyType) -> &'static str {
    match key_type {
        KeyType::String => "String",
        KeyType::Bytes => "Bytes",
        KeyType::Hash => "Hash",
        KeyType::List => "List",
        KeyType::Set => "Set",
        KeyType::SortedSet => "ZSet",
        KeyType::Json => "JSON",
        KeyType::Stream => "Stream",
        KeyType::Unknown => "?",
    }
}

fn render_value_preview(value: &KeyGetResult) -> String {
    match value.repr {
        ValueRepr::Text | ValueRepr::Json | ValueRepr::Structured | ValueRepr::Stream => {
            let text = String::from_utf8_lossy(&value.value);
            let max_chars = 4000;

            if text.chars().count() > max_chars {
                let truncated: String = text.chars().take(max_chars).collect();
                format!("{}\n... (truncated)", truncated)
            } else {
                text.to_string()
            }
        }
        ValueRepr::Binary => format!("{} bytes (binary)", value.value.len()),
    }
}

fn parse_database_name(name: &str) -> Option<u32> {
    let trimmed = name.trim();
    let digits = trimmed.strip_prefix("db").unwrap_or(trimmed);
    digits.parse::<u32>().ok()
}

// ---------------------------------------------------------------------------
// Member parsing
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct MemberEntry {
    display: String,
    field: Option<String>,
    score: Option<f64>,
    /// Stream entry ID, used for XDEL targeting.
    entry_id: Option<String>,
}

fn parse_members(value: &KeyGetResult) -> Vec<MemberEntry> {
    if value.repr == ValueRepr::Stream {
        return parse_stream_entries(&value.value);
    }

    if value.repr != ValueRepr::Structured {
        return vec![MemberEntry {
            display: String::from_utf8_lossy(&value.value).to_string(),
            field: None,
            score: None,
            entry_id: None,
        }];
    }

    let Ok(json) = serde_json::from_slice::<serde_json::Value>(&value.value) else {
        return vec![MemberEntry {
            display: String::from_utf8_lossy(&value.value).to_string(),
            field: None,
            score: None,
            entry_id: None,
        }];
    };

    match json {
        serde_json::Value::Object(map) => map
            .into_iter()
            .map(|(k, v)| {
                let display = match &v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                MemberEntry {
                    display,
                    field: Some(k),
                    score: None,
                    entry_id: None,
                }
            })
            .collect(),
        serde_json::Value::Array(items) => items
            .into_iter()
            .map(|item| match item {
                serde_json::Value::Object(map) => {
                    let member = map
                        .get("member")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let score = map.get("score").and_then(|v| v.as_f64());

                    if score.is_some() {
                        MemberEntry {
                            display: member,
                            field: None,
                            score,
                            entry_id: None,
                        }
                    } else {
                        MemberEntry {
                            display: match map.values().next() {
                                Some(serde_json::Value::String(s)) => s.clone(),
                                Some(v) => v.to_string(),
                                None => String::new(),
                            },
                            field: None,
                            score: None,
                            entry_id: None,
                        }
                    }
                }
                serde_json::Value::String(s) => MemberEntry {
                    display: s,
                    field: None,
                    score: None,
                    entry_id: None,
                },
                other => MemberEntry {
                    display: other.to_string(),
                    field: None,
                    score: None,
                    entry_id: None,
                },
            })
            .collect(),
        _ => vec![MemberEntry {
            display: String::from_utf8_lossy(&value.value).to_string(),
            field: None,
            score: None,
            entry_id: None,
        }],
    }
}

/// Parses stream entries from `[{"id":"...","fields":{...}}]` JSON into member rows.
/// Each entry becomes a `MemberEntry` with `display = id` and `field = compact JSON of fields`.
fn parse_stream_entries(raw: &[u8]) -> Vec<MemberEntry> {
    let Ok(entries) = serde_json::from_slice::<Vec<serde_json::Value>>(raw) else {
        return vec![MemberEntry {
            display: String::from_utf8_lossy(raw).to_string(),
            field: None,
            score: None,
            entry_id: None,
        }];
    };

    entries
        .into_iter()
        .filter_map(|entry| {
            let id = entry.get("id")?.as_str()?.to_string();
            let fields = entry.get("fields")?;
            let fields_str = serde_json::to_string(fields).ok()?;

            Some(MemberEntry {
                display: id.clone(),
                field: Some(fields_str),
                score: None,
                entry_id: Some(id),
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// JSON → Value conversion
// ---------------------------------------------------------------------------

/// Parse a JSON string into a `Value` tree. Falls back to `Value::Text` on error.
fn parse_json_to_value(json_str: &str) -> Value {
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(json_str) else {
        return Value::Text(json_str.to_string());
    };
    serde_json_to_value(&parsed)
}

fn serde_json_to_value(jv: &serde_json::Value) -> Value {
    match jv {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::Text(s.clone()),
        serde_json::Value::Array(arr) => {
            Value::Array(arr.iter().map(serde_json_to_value).collect())
        }
        serde_json::Value::Object(map) => {
            let fields: BTreeMap<String, Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), serde_json_to_value(v)))
                .collect();
            Value::Document(fields)
        }
    }
}
