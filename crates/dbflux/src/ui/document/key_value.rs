use super::new_key_modal::{NewKeyCreatedEvent, NewKeyModal, NewKeyType, NewKeyValue};
use super::types::{DocumentId, DocumentState};
use crate::app::AppState;
use crate::keymap::{Command, ContextId};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{
    DbError, HashDeleteRequest, HashSetRequest, KeyDeleteRequest, KeyEntry, KeyGetRequest,
    KeyGetResult, KeyRenameRequest, KeyScanRequest, KeySetRequest, KeyType, ListEnd,
    ListPushRequest, ListRemoveRequest, ListSetRequest, SetAddRequest, SetCondition,
    SetRemoveRequest, ValueRepr, ZSetAddRequest, ZSetRemoveRequest,
};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::scroll::ScrollableElement;
use gpui_component::{ActiveTheme, Sizable};
use std::sync::Arc;
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

    // Keys list
    keys: Vec<KeyEntry>,
    next_cursor: Option<String>,
    selected_index: Option<usize>,
    selected_value: Option<KeyGetResult>,
    loading_keys: bool,
    loading_value: bool,
    value_load_generation: u64,
    last_error: Option<String>,

    // Inline rename
    rename_input: Option<Entity<InputState>>,
    renaming_index: Option<usize>,

    // Inline member editing
    editing_member_index: Option<usize>,
    member_edit_input: Option<Entity<InputState>>,
    member_edit_score_input: Option<Entity<InputState>>,

    // Add member inputs (always visible at bottom of member list)
    add_member_input: Entity<InputState>,
    add_member_value_input: Entity<InputState>,

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
        let add_member_input = cx.new(|cx| InputState::new(window, cx).placeholder("New member"));
        let add_member_value_input = cx.new(|cx| InputState::new(window, cx).placeholder("Value"));

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

        subscriptions.push(cx.subscribe_in(
            &add_member_input,
            window,
            |this, _, event: &InputEvent, window, cx| {
                if matches!(event, InputEvent::PressEnter { .. }) {
                    if this.needs_value_column() {
                        this.add_member_value_input
                            .update(cx, |input, cx| input.focus(window, cx));
                    } else {
                        this.add_member_from_inputs(window, cx);
                    }
                }
            },
        ));

        subscriptions.push(cx.subscribe_in(
            &add_member_value_input,
            window,
            |this, _, event: &InputEvent, window, cx| {
                if matches!(event, InputEvent::PressEnter { .. }) {
                    this.add_member_from_inputs(window, cx);
                }
            },
        ));

        let mut doc = Self {
            id: DocumentId::new(),
            title: format!("Redis {}", database),
            profile_id,
            database,
            app_state,
            focus_handle: cx.focus_handle(),
            filter_input,
            members_filter_input,
            focus_mode: KeyValueFocusMode::List,
            keys: Vec::new(),
            next_cursor: None,
            selected_index: None,
            selected_value: None,
            loading_keys: false,
            loading_value: false,
            value_load_generation: 0,
            last_error: None,
            rename_input: None,
            renaming_index: None,
            editing_member_index: None,
            member_edit_input: None,
            member_edit_score_input: None,
            add_member_input,
            add_member_value_input,
            selected_member_index: None,
            cached_members: Vec::new(),
            string_edit_input: None,
            pending_key_delete: None,
            pending_member_delete: None,
            new_key_modal,
            pending_open_new_key_modal: false,
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
        if self.loading_keys || self.loading_value {
            DocumentState::Loading
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

        if self.pending_key_delete.is_some() || self.pending_member_delete.is_some() {
            return ContextId::ConfirmModal;
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

        match cmd {
            // -- Panel switching (h/l) --
            Command::ColumnLeft => {
                if self.focus_mode == KeyValueFocusMode::ValuePanel {
                    self.focus_mode = KeyValueFocusMode::List;
                    cx.notify();
                }
                true
            }
            Command::ColumnRight => {
                if self.focus_mode == KeyValueFocusMode::List && self.selected_value.is_some() {
                    self.focus_mode = KeyValueFocusMode::ValuePanel;
                    if self.selected_member_index.is_none() && !self.cached_members.is_empty() {
                        self.selected_member_index = Some(0);
                    }
                    cx.notify();
                }
                true
            }

            // -- Vertical navigation (j/k) --
            Command::SelectNext => {
                match self.focus_mode {
                    KeyValueFocusMode::ValuePanel => self.move_member_selection(1, cx),
                    _ => {
                        self.focus_mode = KeyValueFocusMode::List;
                        self.move_selection(1, cx);
                    }
                }
                true
            }
            Command::SelectPrev => {
                match self.focus_mode {
                    KeyValueFocusMode::ValuePanel => self.move_member_selection(-1, cx),
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
                        if !self.cached_members.is_empty() {
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
                        if !self.cached_members.is_empty() {
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
            Command::ResultsNextPage => {
                self.load_next_page(cx);
                true
            }
            Command::ResultsAddRow => {
                if self.focus_mode == KeyValueFocusMode::ValuePanel && self.is_structured_type() {
                    self.add_member_input
                        .update(cx, |input, cx| input.focus(window, cx));
                    self.focus_mode = KeyValueFocusMode::TextInput;
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
        let next =
            (current + delta).clamp(0, (self.cached_members.len() - 1) as isize) as usize;
        self.selected_member_index = Some(next);
        cx.notify();
    }

    fn move_selection(&mut self, delta: isize, cx: &mut Context<Self>) {
        if self.keys.is_empty() {
            self.selected_index = None;
            self.selected_value = None;
            self.rebuild_cached_members();
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
        self.rebuild_cached_members();
        self.cancel_member_edit(cx);
        cx.notify();
        self.reload_selected_value(cx);
    }

    // -- Key CRUD --

    fn reload_keys(&mut self, cx: &mut Context<Self>) {
        if self.loading_keys {
            return;
        }

        self.loading_keys = true;
        self.next_cursor = None;
        self.keys.clear();
        self.selected_index = None;
        self.selected_value = None;
        self.last_error = None;
        self.string_edit_input = None;
        self.rebuild_cached_members();
        self.cancel_rename(cx);
        self.cancel_member_edit(cx);
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            self.loading_keys = false;
            self.last_error = Some("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        let filter = self.filter_input.read(cx).value().trim().to_string();
        let database = self.database.clone();
        let entity = cx.entity().clone();
        let request = KeyScanRequest {
            cursor: None,
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
                    this.loading_keys = false;

                    match result {
                        Ok(page) => {
                            this.keys = page.entries;
                            this.next_cursor = page.next_cursor;
                            this.last_error = None;

                            if !this.keys.is_empty() {
                                this.selected_index = Some(0);
                                this.reload_selected_value(cx);
                            }
                        }
                        Err(error) => {
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

    fn load_next_page(&mut self, cx: &mut Context<Self>) {
        if self.loading_keys {
            return;
        }

        let Some(cursor) = self.next_cursor.clone() else {
            return;
        };

        let Some(connection) = self.get_connection(cx) else {
            self.last_error = Some("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        self.loading_keys = true;
        cx.notify();

        let filter = self.filter_input.read(cx).value().trim().to_string();
        let database = self.database.clone();
        let entity = cx.entity().clone();

        let request = KeyScanRequest {
            cursor: Some(cursor),
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
                    this.loading_keys = false;

                    match result {
                        Ok(page) => {
                            this.keys.extend(page.entries);
                            this.next_cursor = page.next_cursor;
                            this.last_error = None;
                        }
                        Err(error) => {
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
            self.rebuild_cached_members();
            cx.notify();
            return;
        };

        let Some(connection) = self.get_connection(cx) else {
            self.last_error = Some("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        self.value_load_generation += 1;
        let generation = self.value_load_generation;

        self.loading_value = true;
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
                    // Discard stale responses from previous selections
                    if this.value_load_generation != generation {
                        return;
                    }

                    this.loading_value = false;

                    match result {
                        Ok(value) => {
                            this.selected_value = Some(value);
                            this.last_error = None;
                            this.rebuild_cached_members();
                        }
                        Err(error) => {
                            this.selected_value = None;
                            this.last_error = Some(error.to_string());
                            this.rebuild_cached_members();
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

        // Optimistic: remove from local list immediately
        if pending.index < self.keys.len() {
            self.keys.remove(pending.index);

            if self.keys.is_empty() {
                self.selected_index = None;
                self.selected_value = None;
                self.rebuild_cached_members();
            } else {
                let new_idx = pending.index.min(self.keys.len() - 1);
                self.selected_index = Some(new_idx);
                self.selected_value = None;
                self.rebuild_cached_members();
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
                            this.last_error = None;
                        }
                        Ok(false) => {
                            this.last_error = Some("Key was not deleted".to_string());
                            this.reload_keys(cx);
                            return;
                        }
                        Err(error) => {
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
                                    field,
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
                                member: member.display,
                                keyspace,
                            })?;
                        }
                        KeyType::SortedSet => {
                            api.zset_remove(&ZSetRemoveRequest {
                                key,
                                member: member.display,
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
                        this.last_error = None;
                        this.reload_selected_value(cx);
                    }
                    Err(error) => {
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
                        this.last_error = None;
                        this.reload_keys(cx);
                    }
                    Err(error) => {
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
                        this.last_error = None;
                        this.reload_selected_value(cx);
                    }
                    Err(error) => {
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
                                    field: field_name.clone(),
                                    keyspace,
                                })?;
                                api.hash_set(&HashSetRequest {
                                    key,
                                    field: field_name.clone(),
                                    value: new_value,
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
                                    member: old_member.display,
                                    keyspace,
                                })?;
                                api.set_add(&SetAddRequest {
                                    key,
                                    member: new_value,
                                    keyspace,
                                })?;
                            }
                        }
                        KeyType::SortedSet => {
                            let score = new_score.unwrap_or(old_member.score.unwrap_or(0.0));

                            if new_value != old_member.display {
                                api.zset_remove(&ZSetRemoveRequest {
                                    key: key.clone(),
                                    member: old_member.display,
                                    keyspace,
                                })?;
                            }
                            api.zset_add(&ZSetAddRequest {
                                key,
                                member: new_value,
                                score,
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
                        this.last_error = None;
                        this.reload_selected_value(cx);
                    }
                    Err(error) => {
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

    fn add_member_from_inputs(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let member_text = self.add_member_input.read(cx).value().trim().to_string();
        if member_text.is_empty() {
            return;
        }

        let value_text = self
            .add_member_value_input
            .read(cx)
            .value()
            .trim()
            .to_string();

        let Some(key) = self.selected_key() else {
            return;
        };
        let Some(key_type) = self.selected_key_type() else {
            return;
        };
        let Some(connection) = self.get_connection(cx) else {
            return;
        };

        // Optimistic: add to cached members and clear inputs
        let new_member = match key_type {
            KeyType::Hash => MemberEntry {
                display: value_text.clone(),
                field: Some(member_text.clone()),
                score: None,
            },
            KeyType::SortedSet => MemberEntry {
                display: member_text.clone(),
                field: None,
                score: Some(value_text.parse::<f64>().unwrap_or(0.0)),
            },
            _ => MemberEntry {
                display: member_text.clone(),
                field: None,
                score: None,
            },
        };
        self.cached_members.push(new_member);

        self.add_member_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
            state.focus(window, cx);
        });
        self.add_member_value_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
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

                    match key_type {
                        KeyType::Hash => {
                            api.hash_set(&HashSetRequest {
                                key,
                                field: member_text,
                                value: value_text,
                                keyspace,
                            })?;
                        }
                        KeyType::List => {
                            api.list_push(&ListPushRequest {
                                key,
                                value: member_text,
                                end: ListEnd::Tail,
                                keyspace,
                            })?;
                        }
                        KeyType::Set => {
                            api.set_add(&SetAddRequest {
                                key,
                                member: member_text,
                                keyspace,
                            })?;
                        }
                        KeyType::SortedSet => {
                            let score = value_text.parse::<f64>().unwrap_or(0.0);
                            api.zset_add(&ZSetAddRequest {
                                key,
                                member: member_text,
                                score,
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
                        this.last_error = None;
                        this.reload_selected_value(cx);
                    }
                    Err(error) => {
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
                            for (field, value) in &fields {
                                api.hash_set(&HashSetRequest {
                                    key: event.key_name.clone(),
                                    field: field.clone(),
                                    value: value.clone(),
                                    keyspace,
                                })?;
                            }
                            if let Some(ttl) = event.ttl {
                                api.expire_key(&dbflux_core::KeyExpireRequest {
                                    key: event.key_name.clone(),
                                    ttl_seconds: ttl,
                                    keyspace,
                                })?;
                            }
                        }
                        NewKeyValue::ListMembers(members) => {
                            for member in &members {
                                api.list_push(&ListPushRequest {
                                    key: event.key_name.clone(),
                                    value: member.clone(),
                                    end: ListEnd::Tail,
                                    keyspace,
                                })?;
                            }
                            if let Some(ttl) = event.ttl {
                                api.expire_key(&dbflux_core::KeyExpireRequest {
                                    key: event.key_name.clone(),
                                    ttl_seconds: ttl,
                                    keyspace,
                                })?;
                            }
                        }
                        NewKeyValue::SetMembers(members) => {
                            for member in &members {
                                api.set_add(&SetAddRequest {
                                    key: event.key_name.clone(),
                                    member: member.clone(),
                                    keyspace,
                                })?;
                            }
                            if let Some(ttl) = event.ttl {
                                api.expire_key(&dbflux_core::KeyExpireRequest {
                                    key: event.key_name.clone(),
                                    ttl_seconds: ttl,
                                    keyspace,
                                })?;
                            }
                        }
                        NewKeyValue::ZSetMembers(members) => {
                            for (member, score) in &members {
                                api.zset_add(&ZSetAddRequest {
                                    key: event.key_name.clone(),
                                    member: member.clone(),
                                    score: *score,
                                    keyspace,
                                })?;
                            }
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
                        this.last_error = None;
                        this.reload_keys(cx);
                    }
                    Err(error) => {
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

    fn rebuild_cached_members(&mut self) {
        self.cached_members = match &self.selected_value {
            Some(value) => parse_members(value),
            None => Vec::new(),
        };
    }

    fn is_structured_type(&self) -> bool {
        matches!(
            self.selected_key_type(),
            Some(KeyType::Hash | KeyType::List | KeyType::Set | KeyType::SortedSet)
        )
    }

    fn needs_value_column(&self) -> bool {
        matches!(
            self.selected_key_type(),
            Some(KeyType::Hash | KeyType::SortedSet)
        )
    }
}

impl EventEmitter<KeyValueDocumentEvent> for KeyValueDocument {}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

impl Render for KeyValueDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Handle deferred modal open before borrowing theme
        if self.pending_open_new_key_modal {
            self.pending_open_new_key_modal = false;
            self.new_key_modal
                .update(cx, |modal, cx| modal.open(window, cx));
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
            let ttl_label = value
                .entry
                .ttl_seconds
                .map(|ttl| format!("{}s", ttl))
                .unwrap_or_else(|| "No limit".to_string());
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
                                    .text_color(theme.muted_foreground),
                            )
                            .child(ttl_label),
                    )
                    .child(size_label),
            );

            if is_structured {
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

                header = header.child(div().flex_1().child("#"));
                header = header.child(div().w(px(300.0)).child("Value"));
                if needs_value_col {
                    header = header.child(div().w(px(200.0)).child("Field/Score"));
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
                        .when(!is_selected, |d| d.hover(|d| d.bg(theme.list_active)));

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
                        row = row.child(
                            div()
                                .flex_1()
                                .cursor_pointer()
                                .on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(move |this, _, window, cx| {
                                        cx.stop_propagation();
                                        this.start_member_edit(idx, window, cx);
                                    }),
                                )
                                .child(member.display.clone()),
                        );

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

                // Add member row
                // Add member row placeholders are set at construction time

                let mut add_row = div()
                    .flex()
                    .items_center()
                    .px(Spacing::MD)
                    .h(Heights::ROW)
                    .border_l_1()
                    .border_color(theme.border)
                    .gap(Spacing::SM)
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, _, _, cx| {
                            this.focus_mode = KeyValueFocusMode::TextInput;
                            cx.stop_propagation();
                            cx.notify();
                        }),
                    );

                add_row = add_row.child(
                    div()
                        .flex_1()
                        .child(Input::new(&self.add_member_input).small().w_full()),
                );

                if needs_value_col {
                    add_row = add_row.child(
                        div()
                            .w(px(200.0))
                            .child(Input::new(&self.add_member_value_input).small().w_full()),
                    );
                }

                add_row = add_row.child(
                    icon_button_base("add-member-btn", AppIcon::Plus, theme).on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, _, window, cx| {
                            this.add_member_from_inputs(window, cx);
                        }),
                    ),
                );

                panel = panel.child(members_list).child(add_row);
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
                .child(if self.loading_value {
                    "Loading..."
                } else {
                    "Select a key to inspect"
                })
                .into_any_element()
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
                        icon_button_base("kv-reload", AppIcon::RefreshCcw, theme).on_mouse_down(
                            MouseButton::Left,
                            cx.listener(|this, _, _, cx| {
                                this.reload_keys(cx);
                            }),
                        ),
                    )
                    .child(
                        icon_button_base("kv-next", AppIcon::ChevronDown, theme).on_mouse_down(
                            MouseButton::Left,
                            cx.listener(|this, _, _, cx| {
                                this.load_next_page(cx);
                            }),
                        ),
                    ),
            )
            // Status line
            .child(
                div()
                    .flex()
                    .items_center()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .text_size(FontSizes::XS)
                    .text_color(theme.muted_foreground)
                    .border_b_1()
                    .border_color(theme.border)
                    .child(format!(
                        "{} keys{}",
                        self.keys.len(),
                        if self.loading_keys {
                            " (loading...)"
                        } else if self.next_cursor.is_some() {
                            " (more available)"
                        } else {
                            ""
                        }
                    )),
            )
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
            .child(left_panel)
            .child(right_panel)
            .when(self.new_key_modal.read(cx).is_visible(), |d| {
                d.child(self.new_key_modal.clone())
            })
            .when(has_pending_delete, |d| {
                d.child(render_delete_confirm_modal(
                    &delete_title,
                    &delete_message,
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
        ValueRepr::Text | ValueRepr::Json | ValueRepr::Structured => {
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
}

fn parse_members(value: &KeyGetResult) -> Vec<MemberEntry> {
    if value.repr != ValueRepr::Structured {
        return vec![MemberEntry {
            display: String::from_utf8_lossy(&value.value).to_string(),
            field: None,
            score: None,
        }];
    }

    let Ok(json) = serde_json::from_slice::<serde_json::Value>(&value.value) else {
        return vec![MemberEntry {
            display: String::from_utf8_lossy(&value.value).to_string(),
            field: None,
            score: None,
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
                        }
                    }
                }
                serde_json::Value::String(s) => MemberEntry {
                    display: s,
                    field: None,
                    score: None,
                },
                other => MemberEntry {
                    display: other.to_string(),
                    field: None,
                    score: None,
                },
            })
            .collect(),
        _ => vec![MemberEntry {
            display: String::from_utf8_lossy(&value.value).to_string(),
            field: None,
            score: None,
        }],
    }
}
