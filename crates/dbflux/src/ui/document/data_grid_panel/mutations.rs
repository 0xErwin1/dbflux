use super::utils::value_to_json;
use super::{DataGridPanel, DataSource, PendingDeleteConfirm, PendingToast};
use crate::ui::components::document_tree::NodeId;
use crate::ui::toast::ToastExt;
use dbflux_core::{
    CollectionRef, DocumentFilter, DocumentUpdate, Pagination, RowDelete, RowIdentity, RowInsert,
    RowPatch, RowState, TableRef, Value,
};
use gpui::*;
use std::collections::BTreeMap;
use uuid::Uuid;

fn parse_inline_document_value(input: &str) -> serde_json::Value {
    let trimmed = input.trim();

    if trimmed.eq_ignore_ascii_case("null") {
        return serde_json::Value::Null;
    }

    if trimmed.eq_ignore_ascii_case("true") {
        return serde_json::Value::Bool(true);
    }

    if trimmed.eq_ignore_ascii_case("false") {
        return serde_json::Value::Bool(false);
    }

    if (trimmed.starts_with('{') || trimmed.starts_with('[') || trimmed.starts_with('"'))
        && let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed)
    {
        return value;
    }

    if let Ok(value) = trimmed.parse::<i64>() {
        return serde_json::json!(value);
    }

    if let Ok(value) = trimmed.parse::<f64>()
        && let Some(number) = serde_json::Number::from_f64(value)
    {
        return serde_json::Value::Number(number);
    }

    serde_json::Value::String(input.to_string())
}

fn json_to_inline_value(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(boolean) => Value::Bool(*boolean),
        serde_json::Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                Value::Int(integer)
            } else if let Some(float) = number.as_f64() {
                Value::Float(float)
            } else {
                Value::Text(number.to_string())
            }
        }
        serde_json::Value::String(text) => Value::Text(text.clone()),
        serde_json::Value::Array(items) => {
            Value::Array(items.iter().map(json_to_inline_value).collect())
        }
        serde_json::Value::Object(object) => {
            if object.len() == 1
                && let Some(serde_json::Value::String(object_id)) = object.get("$oid")
            {
                return Value::ObjectId(object_id.clone());
            }

            let fields: BTreeMap<String, Value> = object
                .iter()
                .map(|(key, val)| (key.clone(), json_to_inline_value(val)))
                .collect();
            Value::Document(fields)
        }
    }
}

fn set_value_at_path(current: &mut Value, path: &[String], new_value: Value) -> bool {
    if path.is_empty() {
        *current = new_value;
        return true;
    }

    let mut cursor = current;

    for (idx, segment) in path.iter().enumerate() {
        let is_last = idx + 1 == path.len();

        if is_last {
            match cursor {
                Value::Document(fields) => {
                    fields.insert(segment.clone(), new_value);
                    return true;
                }
                Value::Array(items) => {
                    let Ok(item_idx) = segment.parse::<usize>() else {
                        return false;
                    };

                    if item_idx >= items.len() {
                        return false;
                    }

                    items[item_idx] = new_value;
                    return true;
                }
                _ => return false,
            }
        }

        cursor = match cursor {
            Value::Document(fields) => {
                let Some(next) = fields.get_mut(segment) else {
                    return false;
                };
                next
            }
            Value::Array(items) => {
                let Ok(item_idx) = segment.parse::<usize>() else {
                    return false;
                };

                let Some(next) = items.get_mut(item_idx) else {
                    return false;
                };

                next
            }
            _ => return false,
        };
    }

    false
}

impl DataGridPanel {
    // === Row Editing ===

    fn apply_inline_value_to_result(&mut self, node_id: &NodeId, new_value: &Value) {
        let Some(doc_index) = node_id.doc_index() else {
            return;
        };

        let Some(row) = self.result.rows.get_mut(doc_index) else {
            return;
        };

        let mut updated = false;

        if let Some(doc_col_idx) = self
            .result
            .columns
            .iter()
            .position(|c| c.name == "_document")
            && let Some(doc_cell) = row.get_mut(doc_col_idx)
        {
            updated |= set_value_at_path(doc_cell, &node_id.path[1..], new_value.clone());
        }

        if node_id.path.len() == 2 {
            let field_name = &node_id.path[1];
            if let Some(col_idx) = self
                .result
                .columns
                .iter()
                .position(|c| c.name == *field_name)
                && let Some(cell) = row.get_mut(col_idx)
            {
                *cell = new_value.clone();
                updated = true;
            }
        }

        if !updated {
            log::warn!(
                "[SAVE] Could not update cached row for path: {:?}",
                node_id.path
            );
        }
    }

    pub(super) fn handle_document_tree_inline_edit(
        &mut self,
        node_id: &NodeId,
        new_value: &str,
        cx: &mut Context<Self>,
    ) {
        let DataSource::Collection {
            profile_id,
            collection,
            ..
        } = &self.source
        else {
            return;
        };

        let Some(doc_index) = node_id.doc_index() else {
            return;
        };

        if node_id.path.len() < 2 {
            return;
        }

        let Some(row) = self.result.rows.get(doc_index) else {
            return;
        };

        let id_col_idx = self
            .result
            .columns
            .iter()
            .position(|c| c.name == "_id")
            .unwrap_or(0);

        let id_value = row.get(id_col_idx).cloned().unwrap_or(Value::Null);

        let filter = match &id_value {
            Value::ObjectId(oid) => DocumentFilter::new(serde_json::json!({"_id": {"$oid": oid}})),
            Value::Text(value) => DocumentFilter::new(serde_json::json!({"_id": value})),
            _ => {
                log::error!("[SAVE] Invalid _id value for document inline edit");
                return;
            }
        };

        let field_path = node_id.path[1..].join(".");
        let parsed_json = parse_inline_document_value(new_value);
        let inline_value = json_to_inline_value(&parsed_json);
        let node_id = node_id.clone();

        let mut set_fields = serde_json::Map::new();
        set_fields.insert(field_path, parsed_json);

        let update_doc = serde_json::json!({"$set": set_fields});
        let update = DocumentUpdate::new(collection.name.clone(), filter, update_doc)
            .with_database(collection.database.clone());

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();
        let profile_id = *profile_id;

        cx.spawn(async move |_this, cx| {
            let conn = cx
                .update(|cx| {
                    app_state
                        .read(cx)
                        .connections()
                        .get(&profile_id)
                        .map(|c| c.connection.clone())
                })
                .ok()
                .flatten();

            let Some(conn) = conn else {
                log::error!("[SAVE] No connection for profile {}", profile_id);
                return;
            };

            let result: Result<dbflux_core::CrudResult, dbflux_core::DbError> = cx
                .background_executor()
                .spawn(async move { conn.update_document(&update) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    match result {
                        Ok(_) => {
                            panel.apply_inline_value_to_result(&node_id, &inline_value);

                            if let Some(tree_state) = panel.document_tree_state.clone() {
                                tree_state.update(cx, |state, cx| {
                                    state.apply_inline_edit_value(
                                        &node_id,
                                        inline_value.clone(),
                                        cx,
                                    );
                                });
                            }

                            panel.pending_toast = Some(PendingToast {
                                message: "Document updated".to_string(),
                                is_error: false,
                            });
                        }
                        Err(error) => {
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Failed to update document: {}", error),
                                is_error: true,
                            });
                        }
                    }
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn handle_save_row(&mut self, row_idx: usize, cx: &mut Context<Self>) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let changes = {
            let state = table_state.read(cx);

            if !state.is_editable() {
                return;
            }

            let row_changes = state.edit_buffer().row_changes(row_idx);
            if row_changes.is_empty() {
                return;
            }

            row_changes
                .into_iter()
                .map(|(idx, cell)| (idx, cell.clone()))
                .collect::<Vec<_>>()
        };

        let changes_ref: Vec<(usize, &crate::ui::components::data_table::model::CellValue)> =
            changes.iter().map(|(idx, cell)| (*idx, cell)).collect();

        match &self.source {
            DataSource::Table {
                profile_id, table, ..
            } => {
                self.save_table_row(*profile_id, table.clone(), row_idx, &changes_ref, cx);
            }
            DataSource::Collection {
                profile_id,
                collection,
                ..
            } => {
                self.save_document(*profile_id, collection.clone(), row_idx, &changes_ref, cx);
            }
            DataSource::QueryResult { .. } => {}
        }
    }

    pub(super) fn save_table_row(
        &mut self,
        profile_id: Uuid,
        table_ref: TableRef,
        row_idx: usize,
        changes: &[(usize, &crate::ui::components::data_table::model::CellValue)],
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let state = table_state.read(cx);
        let pk_indices = state.pk_columns();
        let model = state.model();

        let mut pk_columns = Vec::with_capacity(pk_indices.len());
        let mut pk_values = Vec::with_capacity(pk_indices.len());

        for &col_idx in pk_indices {
            if let Some(col_spec) = model.columns.get(col_idx) {
                pk_columns.push(col_spec.title.to_string());
            }
            if let Some(cell) = model.cell(row_idx, col_idx) {
                pk_values.push(cell.to_value());
            }
        }

        if pk_columns.len() != pk_indices.len() || pk_values.len() != pk_indices.len() {
            log::error!("[SAVE] Failed to build row identity");
            return;
        }

        let identity = RowIdentity::new(pk_columns, pk_values);

        let change_values: Vec<(String, Value)> = changes
            .iter()
            .filter_map(|&(col_idx, cell_value)| {
                model
                    .columns
                    .get(col_idx)
                    .map(|col| (col.title.to_string(), cell_value.to_value()))
            })
            .collect();

        let patch = RowPatch::new(
            identity,
            table_ref.name.clone(),
            table_ref.schema.clone(),
            change_values,
        );

        let table_state_for_update = table_state.clone();
        table_state_for_update.update(cx, |state, cx| {
            state
                .edit_buffer_mut()
                .set_row_state(row_idx, RowState::Saving);
            cx.notify();
        });

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx
                .update(|cx| {
                    app_state
                        .read(cx)
                        .connections()
                        .get(&profile_id)
                        .map(|c| c.connection.clone())
                })
                .ok()
                .flatten();

            let Some(conn) = conn else {
                log::error!("[SAVE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        if let Some(table_state) = &panel.table_state {
                            table_state.update(cx, |state, cx| {
                                state.edit_buffer_mut().set_row_state(
                                    row_idx,
                                    RowState::Error("No connection".to_string()),
                                );
                                cx.notify();
                            });
                        }
                    });
                })
                .ok();
                return;
            };

            let result: Result<dbflux_core::CrudResult, dbflux_core::DbError> = cx
                .background_executor()
                .spawn(async move { conn.update_row(&patch) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    panel.handle_save_result(row_idx, result, cx);
                });
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn save_document(
        &mut self,
        profile_id: Uuid,
        collection: CollectionRef,
        row_idx: usize,
        changes: &[(usize, &crate::ui::components::data_table::model::CellValue)],
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let state = table_state.read(cx);
        let model = state.model();

        // Find _id column and get its value
        let id_col_idx = self
            .result
            .columns
            .iter()
            .position(|c| c.name == "_id")
            .unwrap_or(0);

        let id_value = model
            .cell(row_idx, id_col_idx)
            .map(|c| c.to_value())
            .unwrap_or(Value::Null);

        let filter = match &id_value {
            Value::ObjectId(oid) => DocumentFilter::new(serde_json::json!({"_id": {"$oid": oid}})),
            Value::Text(s) => DocumentFilter::new(serde_json::json!({"_id": s})),
            _ => {
                log::error!("[SAVE] Invalid _id value for document");
                return;
            }
        };

        // Build $set update from changes
        let mut set_fields = serde_json::Map::new();
        for &(col_idx, cell_value) in changes {
            if let Some(col) = model.columns.get(col_idx) {
                let value = cell_value.to_value();
                set_fields.insert(col.title.to_string(), value_to_json(&value));
            }
        }

        let update_doc = serde_json::json!({"$set": set_fields});

        let update = DocumentUpdate::new(collection.name.clone(), filter, update_doc)
            .with_database(collection.database.clone());

        let table_state_for_update = table_state.clone();
        table_state_for_update.update(cx, |state, cx| {
            state
                .edit_buffer_mut()
                .set_row_state(row_idx, RowState::Saving);
            cx.notify();
        });

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx
                .update(|cx| {
                    app_state
                        .read(cx)
                        .connections()
                        .get(&profile_id)
                        .map(|c| c.connection.clone())
                })
                .ok()
                .flatten();

            let Some(conn) = conn else {
                log::error!("[SAVE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        if let Some(table_state) = &panel.table_state {
                            table_state.update(cx, |state, cx| {
                                state.edit_buffer_mut().set_row_state(
                                    row_idx,
                                    RowState::Error("No connection".to_string()),
                                );
                                cx.notify();
                            });
                        }
                    });
                })
                .ok();
                return;
            };

            let result: Result<dbflux_core::CrudResult, dbflux_core::DbError> = cx
                .background_executor()
                .spawn(async move { conn.update_document(&update) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    panel.handle_save_result(row_idx, result, cx);
                });
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn handle_save_result(
        &mut self,
        row_idx: usize,
        result: Result<dbflux_core::CrudResult, dbflux_core::DbError>,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        match result {
            Ok(crud_result) => {
                table_state.update(cx, |state, cx| {
                    if let Some(returning_row) = crud_result.returning_row {
                        state.apply_returning_row(row_idx, &returning_row);
                    }
                    state.edit_buffer_mut().clear_row(row_idx);
                    cx.notify();
                });
                self.pending_toast = Some(PendingToast {
                    message: "Saved".to_string(),
                    is_error: false,
                });
            }
            Err(e) => {
                log::error!("[SAVE] Failed to save row {}: {}", row_idx, e);
                table_state.update(cx, |state, cx| {
                    state
                        .edit_buffer_mut()
                        .set_row_state(row_idx, RowState::Error(e.to_string()));
                    cx.notify();
                });
                self.pending_toast = Some(PendingToast {
                    message: format!("Save failed: {}", e),
                    is_error: true,
                });
            }
        }
        cx.notify();
    }

    pub(super) fn handle_commit_insert(&mut self, insert_idx: usize, cx: &mut Context<Self>) {
        match &self.source {
            DataSource::Collection {
                profile_id,
                collection,
                ..
            } => {
                self.commit_insert_collection(*profile_id, collection.clone(), insert_idx, cx);
            }
            DataSource::Table {
                profile_id, table, ..
            } => {
                self.commit_insert_table(*profile_id, table.clone(), insert_idx, cx);
            }
            DataSource::QueryResult { .. } => {}
        }
    }

    pub(super) fn commit_insert_collection(
        &mut self,
        profile_id: Uuid,
        collection: CollectionRef,
        insert_idx: usize,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let insert_data = {
            let state = table_state.read(cx);
            state
                .edit_buffer()
                .get_pending_insert_by_idx(insert_idx)
                .map(|cells| cells.to_vec())
        };

        let Some(cells) = insert_data else {
            return;
        };

        let mut doc = serde_json::Map::new();
        for (col_idx, cell) in cells.iter().enumerate() {
            if let Some(col) = self.result.columns.get(col_idx) {
                let value = cell.to_value();
                if !matches!(value, Value::Null) {
                    doc.insert(col.name.clone(), value_to_json(&value));
                }
            }
        }

        let insert = dbflux_core::DocumentInsert::one(collection.name.clone(), doc.into())
            .with_database(collection.database.clone());

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();
        let table_state_clone = table_state.clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx
                .update(|cx| {
                    app_state
                        .read(cx)
                        .connections()
                        .get(&profile_id)
                        .map(|c| c.connection.clone())
                })
                .ok()
                .flatten();

            let Some(conn) = conn else {
                log::error!("[INSERT] No connection for profile {}", profile_id);
                return;
            };

            let result = cx
                .background_executor()
                .spawn(async move { conn.insert_document(&insert) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    match result {
                        Ok(_) => {
                            table_state_clone.update(cx, |state, cx| {
                                state
                                    .edit_buffer_mut()
                                    .remove_pending_insert_by_idx(insert_idx);
                                cx.notify();
                            });
                            panel.pending_toast = Some(PendingToast {
                                message: "Document inserted".to_string(),
                                is_error: false,
                            });
                            panel.pending_refresh = true;
                        }
                        Err(e) => {
                            log::error!("[INSERT] Failed: {}", e);
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Insert failed: {}", e),
                                is_error: true,
                            });
                        }
                    }
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn commit_insert_table(
        &mut self,
        profile_id: Uuid,
        table_ref: TableRef,
        insert_idx: usize,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let insert_data = {
            let state = table_state.read(cx);
            state
                .edit_buffer()
                .get_pending_insert_by_idx(insert_idx)
                .map(|cells| cells.to_vec())
        };

        let Some(cells) = insert_data else {
            return;
        };

        let (columns, values) = {
            let state = table_state.read(cx);
            let model = state.model();

            let mut columns = Vec::new();
            let mut values = Vec::new();

            for (col_idx, cell) in cells.iter().enumerate() {
                let value = cell.to_value();

                if matches!(value, Value::Null) {
                    continue;
                }

                if let Some(col) = model.columns.get(col_idx) {
                    columns.push(col.title.to_string());
                    values.push(value);
                }
            }

            (columns, values)
        };

        if columns.is_empty() {
            self.pending_toast = Some(PendingToast {
                message: "Cannot insert: no values provided".to_string(),
                is_error: true,
            });
            cx.notify();
            return;
        }

        let insert = RowInsert::new(
            table_ref.name.clone(),
            table_ref.schema.clone(),
            columns,
            values,
        );

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();
        let table_state_clone = table_state.clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx
                .update(|cx| {
                    app_state
                        .read(cx)
                        .connections()
                        .get(&profile_id)
                        .map(|c| c.connection.clone())
                })
                .ok()
                .flatten();

            let Some(conn) = conn else {
                log::error!("[INSERT] No connection for profile {}", profile_id);
                return;
            };

            let result = cx
                .background_executor()
                .spawn(async move { conn.insert_row(&insert) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    match result {
                        Ok(_) => {
                            table_state_clone.update(cx, |state, cx| {
                                state
                                    .edit_buffer_mut()
                                    .remove_pending_insert_by_idx(insert_idx);
                                cx.notify();
                            });
                            panel.pending_toast = Some(PendingToast {
                                message: "Row inserted".to_string(),
                                is_error: false,
                            });
                            panel.pending_refresh = true;
                        }
                        Err(e) => {
                            log::error!("[INSERT] Failed: {}", e);
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Insert failed: {}", e),
                                is_error: true,
                            });
                        }
                    }
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn handle_commit_delete(&mut self, row_idx: usize, cx: &mut Context<Self>) {
        match &self.source {
            DataSource::Collection {
                profile_id,
                collection,
                ..
            } => {
                self.commit_delete_collection(*profile_id, collection.clone(), row_idx, cx);
            }
            DataSource::Table { .. } => {
                // Show confirmation before deleting from SQL tables
                self.pending_delete_confirm = Some(PendingDeleteConfirm {
                    row_idx,
                    is_table: true,
                });
                cx.notify();
            }
            DataSource::QueryResult { .. } => {}
        }
    }

    pub fn confirm_delete(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(confirm) = self.pending_delete_confirm.take() else {
            return;
        };

        if confirm.is_table
            && let DataSource::Table {
                profile_id, table, ..
            } = &self.source
        {
            self.commit_delete_table(*profile_id, table.clone(), confirm.row_idx, cx);
        }

        self.focus_active_view(window, cx);
        cx.notify();
    }

    pub fn cancel_delete(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.pending_delete_confirm.is_some() {
            self.pending_delete_confirm = None;
            self.focus_active_view(window, cx);
            cx.notify();
        }
    }

    #[allow(dead_code)]
    pub fn has_delete_confirm(&self) -> bool {
        self.pending_delete_confirm.is_some()
    }

    pub(super) fn commit_delete_collection(
        &mut self,
        profile_id: Uuid,
        collection: CollectionRef,
        row_idx: usize,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let id_col_idx = self
            .result
            .columns
            .iter()
            .position(|c| c.name == "_id")
            .unwrap_or(0);

        let id_value = {
            let state = table_state.read(cx);
            let model = state.model();
            model
                .cell(row_idx, id_col_idx)
                .map(|c| c.to_value())
                .unwrap_or(Value::Null)
        };

        let filter = match &id_value {
            Value::ObjectId(oid) => {
                dbflux_core::DocumentFilter::new(serde_json::json!({"_id": {"$oid": oid}}))
            }
            Value::Text(s) => dbflux_core::DocumentFilter::new(serde_json::json!({"_id": s})),
            _ => {
                log::error!("[DELETE] Invalid _id value for document");
                return;
            }
        };

        let delete = dbflux_core::DocumentDelete::new(collection.name.clone(), filter)
            .with_database(collection.database.clone());

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();
        let table_state_clone = table_state.clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx
                .update(|cx| {
                    app_state
                        .read(cx)
                        .connections()
                        .get(&profile_id)
                        .map(|c| c.connection.clone())
                })
                .ok()
                .flatten();

            let Some(conn) = conn else {
                log::error!("[DELETE] No connection for profile {}", profile_id);
                return;
            };

            let result = cx
                .background_executor()
                .spawn(async move { conn.delete_document(&delete) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    match result {
                        Ok(_) => {
                            table_state_clone.update(cx, |state, cx| {
                                state.edit_buffer_mut().unmark_delete(row_idx);
                                cx.notify();
                            });
                            panel.pending_toast = Some(PendingToast {
                                message: "Document deleted".to_string(),
                                is_error: false,
                            });
                            panel.pending_refresh = true;
                        }
                        Err(e) => {
                            log::error!("[DELETE] Failed: {}", e);
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Delete failed: {}", e),
                                is_error: true,
                            });
                        }
                    }
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn commit_delete_table(
        &mut self,
        profile_id: Uuid,
        table_ref: TableRef,
        row_idx: usize,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let (pk_columns, pk_values, pk_count) = {
            let state = table_state.read(cx);
            let pk_indices = state.pk_columns();
            let model = state.model();

            if pk_indices.is_empty() {
                (Vec::new(), Vec::new(), 0)
            } else {
                let mut pk_columns = Vec::with_capacity(pk_indices.len());
                let mut pk_values = Vec::with_capacity(pk_indices.len());
                let pk_count = pk_indices.len();

                for &col_idx in pk_indices {
                    if let Some(col_spec) = model.columns.get(col_idx) {
                        pk_columns.push(col_spec.title.to_string());
                    }
                    if let Some(cell) = model.cell(row_idx, col_idx) {
                        pk_values.push(cell.to_value());
                    }
                }

                (pk_columns, pk_values, pk_count)
            }
        };

        if pk_count == 0 {
            self.pending_toast = Some(PendingToast {
                message: "Cannot delete: no primary key defined for this table".to_string(),
                is_error: true,
            });
            cx.notify();
            return;
        }

        if pk_columns.len() != pk_count || pk_values.len() != pk_count {
            log::error!("[DELETE] Failed to build row identity");
            self.pending_toast = Some(PendingToast {
                message: "Cannot delete: failed to identify row".to_string(),
                is_error: true,
            });
            cx.notify();
            return;
        }

        let identity = RowIdentity::new(pk_columns, pk_values);
        let delete = RowDelete::new(identity, table_ref.name.clone(), table_ref.schema.clone());

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();
        let table_state_clone = table_state.clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx
                .update(|cx| {
                    app_state
                        .read(cx)
                        .connections()
                        .get(&profile_id)
                        .map(|c| c.connection.clone())
                })
                .ok()
                .flatten();

            let Some(conn) = conn else {
                log::error!("[DELETE] No connection for profile {}", profile_id);
                return;
            };

            let result = cx
                .background_executor()
                .spawn(async move { conn.delete_row(&delete) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    match result {
                        Ok(_) => {
                            table_state_clone.update(cx, |state, cx| {
                                state.edit_buffer_mut().unmark_delete(row_idx);
                                cx.notify();
                            });
                            panel.pending_toast = Some(PendingToast {
                                message: "Row deleted".to_string(),
                                is_error: false,
                            });
                            panel.pending_refresh = true;
                        }
                        Err(e) => {
                            log::error!("[DELETE] Failed: {}", e);
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Delete failed: {}", e),
                                is_error: true,
                            });
                        }
                    }
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }
}
