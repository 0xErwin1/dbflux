use super::add_member_modal::AddMemberEvent;
use super::new_key_modal::{NewKeyCreatedEvent, NewKeyType, NewKeyValue};
use super::parsing::{MemberEntry, parse_database_name};
use super::{KeyValueFocusMode, PendingKeyDelete, PendingMemberDelete};
use dbflux_core::{
    DbError, HashDeleteRequest, HashSetRequest, KeyDeleteRequest, KeyRenameRequest, KeySetRequest,
    KeyType, ListEnd, ListPushRequest, ListRemoveRequest, ListSetRequest, SetAddRequest,
    SetCondition, SetRemoveRequest, StreamAddRequest, StreamDeleteRequest, StreamEntryId, TaskKind,
    ValueRepr, ZSetAddRequest, ZSetRemoveRequest,
};
use gpui::*;
use gpui_component::input::{InputEvent, InputState};

impl super::KeyValueDocument {
    // -- Delete confirmation --

    pub(super) fn request_delete_key(&mut self, cx: &mut Context<Self>) {
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

    pub(super) fn confirm_delete_key(&mut self, cx: &mut Context<Self>) {
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

    pub(super) fn cancel_delete_key(&mut self, cx: &mut Context<Self>) {
        self.pending_key_delete = None;
        cx.notify();
    }

    pub(super) fn request_delete_member(&mut self, member_index: usize, cx: &mut Context<Self>) {
        let Some(member) = self.cached_members.get(member_index) else {
            return;
        };

        self.pending_member_delete = Some(PendingMemberDelete {
            member_index,
            member_display: member.display.clone(),
        });
        cx.notify();
    }

    pub(super) fn confirm_delete_member(&mut self, cx: &mut Context<Self>) {
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

    pub(super) fn cancel_delete_member(&mut self, cx: &mut Context<Self>) {
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

    pub(super) fn start_rename(&mut self, window: &mut Window, cx: &mut Context<Self>) {
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

    pub(super) fn cancel_rename(&mut self, cx: &mut Context<Self>) {
        self.rename_input = None;
        self.renaming_index = None;
        self.focus_mode = KeyValueFocusMode::List;
        cx.notify();
    }

    // -- Inline string/JSON value editing --

    pub(super) fn start_string_edit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
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

    pub(super) fn cancel_string_edit(&mut self, cx: &mut Context<Self>) {
        self.string_edit_input = None;
        self.focus_mode = KeyValueFocusMode::List;
        cx.notify();
    }

    // -- Member editing --

    pub(super) fn start_member_edit(
        &mut self,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
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
                        KeyType::Set if new_value != old_member.display => {
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

    pub(super) fn cancel_member_edit(&mut self, cx: &mut Context<Self>) {
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

    pub(super) fn handle_add_member_event(
        &mut self,
        event: AddMemberEvent,
        cx: &mut Context<Self>,
    ) {
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

    pub(super) fn handle_new_key_created(
        &mut self,
        event: NewKeyCreatedEvent,
        cx: &mut Context<Self>,
    ) {
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
}
