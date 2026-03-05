mod commands;
mod context_menu;
mod copy_command;
mod document_view;
mod mutations;
mod pagination;
pub(super) mod parsing;
mod render;

// Re-export sibling `document/` modules so submodules can use `super::*_modal`.
use super::add_member_modal;
use super::new_key_modal;

use super::add_member_modal::{AddMemberEvent, AddMemberModal};
use super::new_key_modal::{NewKeyCreatedEvent, NewKeyModal};
use super::task_runner::DocumentTaskRunner;
use super::types::{DocumentId, DocumentState};
use crate::app::AppState;
use crate::keymap::{Command, ContextId};
use crate::ui::components::document_tree::{DocumentTree, DocumentTreeState};
use crate::ui::components::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use context_menu::KvContextMenu;
use dbflux_core::{CancelToken, KeyEntry, KeyGetResult, KeyType, RefreshPolicy};
use gpui::*;
use gpui_component::input::{InputEvent, InputState};
use parsing::{MemberEntry, parse_database_name};
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

#[derive(Clone, Debug)]
pub enum KeyValueDocumentEvent {
    RequestFocus,
}

impl EventEmitter<KeyValueDocumentEvent> for KeyValueDocument {}

// ---------------------------------------------------------------------------
// Lifecycle, public API, and navigation helpers
// ---------------------------------------------------------------------------

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

        let default_refresh = app_state
            .read(cx)
            .effective_settings_for_connection(Some(profile_id))
            .resolve_refresh_policy();

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

    pub(super) fn apply_ttl_from_entry(&mut self, entry: &KeyEntry, cx: &mut Context<Self>) {
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

    // -- Navigation helpers --

    pub(super) fn selected_key(&self) -> Option<String> {
        self.selected_index
            .and_then(|idx| self.keys.get(idx))
            .map(|entry| entry.key.clone())
    }

    pub(super) fn selected_key_type(&self) -> Option<KeyType> {
        self.selected_value.as_ref().and_then(|v| v.entry.key_type)
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

    pub(super) fn move_selection(&mut self, delta: isize, cx: &mut Context<Self>) {
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

    pub(super) fn select_index(&mut self, index: usize, cx: &mut Context<Self>) {
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

    pub(super) fn get_connection(
        &self,
        cx: &Context<Self>,
    ) -> Option<Arc<dyn dbflux_core::Connection>> {
        self.app_state
            .read(cx)
            .connections()
            .get(&self.profile_id)
            .map(|conn| conn.connection.clone())
    }
}
