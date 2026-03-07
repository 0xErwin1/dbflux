use super::parsing::{MemberEntry, parse_json_to_value, parse_members};
use super::{KeyValueDocumentEvent, KvValueViewMode};
use crate::ui::AsyncUpdateResultExt;
use crate::ui::components::document_tree::{
    DocumentTree, DocumentTreeEvent, DocumentTreeState, NodeId,
};
use dbflux_core::{DbError, HashSetRequest, KeyType, TaskKind, Value};
use gpui::*;

impl super::KeyValueDocument {
    pub(super) fn rebuild_cached_members(&mut self, cx: &mut Context<Self>) {
        self.cached_members = match &self.selected_value {
            Some(value) => parse_members(value),
            None => Vec::new(),
        };

        if self.value_view_mode == KvValueViewMode::Document && self.supports_document_view() {
            self.rebuild_document_tree(cx);
        }
    }

    pub(super) fn is_structured_type(&self) -> bool {
        matches!(
            self.selected_key_type(),
            Some(
                KeyType::Hash | KeyType::List | KeyType::Set | KeyType::SortedSet | KeyType::Stream
            )
        )
    }

    pub(super) fn is_stream_type(&self) -> bool {
        matches!(self.selected_key_type(), Some(KeyType::Stream))
    }

    pub(super) fn needs_value_column(&self) -> bool {
        matches!(
            self.selected_key_type(),
            Some(KeyType::Hash | KeyType::SortedSet | KeyType::Stream)
        )
    }

    pub(super) fn supports_document_view(&self) -> bool {
        matches!(
            self.selected_key_type(),
            Some(KeyType::Hash | KeyType::Stream)
        )
    }

    pub(super) fn is_document_view_active(&self) -> bool {
        self.value_view_mode == KvValueViewMode::Document
            && self.supports_document_view()
            && self.document_tree_state.is_some()
    }

    pub(super) fn toggle_value_view_mode(&mut self, cx: &mut Context<Self>) {
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

    pub(super) fn rebuild_document_tree(&mut self, cx: &mut Context<Self>) {
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

    pub(super) fn handle_tree_inline_edit(
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
            .log_if_dropped();
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
