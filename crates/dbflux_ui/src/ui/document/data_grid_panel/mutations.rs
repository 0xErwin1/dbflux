use super::utils::{extract_pk_columns, value_to_json};
use super::{DataGridPanel, DataSource, PendingBatchRemaining, PendingDeleteConfirm, PendingToast};
use crate::ui::AsyncUpdateResultExt;
use crate::ui::components::document_tree::NodeId;
use crate::ui::components::toast::ToastExt;
use dbflux_core::{
    CollectionRef, DocumentFilter, DocumentUpdate, Pagination, QueryResult, RowDelete, RowIdentity,
    RowInsert, RowPatch, RowState, TableRef, TaskKind, Value,
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

        // Extract PK columns from schema metadata
        let pk_columns = extract_pk_columns(&self.result);

        let filter = if pk_columns.is_empty() {
            // MongoDB fallback: use _id column
            let id_col_idx = self
                .result
                .columns
                .iter()
                .position(|c| c.name == "_id")
                .unwrap_or(0);

            let id_value = row.get(id_col_idx).cloned().unwrap_or(Value::Null);

            match &id_value {
                Value::ObjectId(oid) => {
                    DocumentFilter::new(serde_json::json!({"_id": {"$oid": oid}}))
                }
                Value::Text(value) => DocumentFilter::new(serde_json::json!({"_id": value})),
                _ => {
                    log::error!("[SAVE] Invalid _id value for document inline edit");
                    return;
                }
            }
        } else {
            // Use PK columns from metadata
            let mut filter_obj = serde_json::Map::new();
            for (col_idx, col_name) in &pk_columns {
                if let Some(cell_value) = row.get(*col_idx) {
                    filter_obj.insert(col_name.clone(), value_to_json(cell_value));
                }
            }
            DocumentFilter::new(serde_json::Value::Object(filter_obj))
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

        let (task_id, _cancel_token) =
            self.runner
                .start_mutation(TaskKind::Query, "Update document field", cx);

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
                .unwrap_or_log_dropped();

            let Some(conn) = conn else {
                log::error!("[SAVE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                    });
                })
                .log_if_dropped();
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
                            panel.runner.complete_mutation(task_id, cx);
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
                            panel.runner.fail_mutation(task_id, error.to_string(), cx);
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Failed to update document: {}", error),
                                is_error: true,
                            });
                        }
                    }
                    cx.notify();
                });
            })
            .log_if_dropped();
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
                profile_id,
                database,
                table,
                ..
            } => {
                self.save_table_row(
                    *profile_id,
                    database.clone(),
                    table.clone(),
                    row_idx,
                    &changes_ref,
                    cx,
                );
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
        database: Option<String>,
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

        if pk_values
            .iter()
            .any(|value| matches!(value, Value::Unsupported(_)))
        {
            let message = "Cannot save row: primary key uses an unsupported value type".to_string();
            log::error!("[SAVE] {}", message);

            table_state.update(cx, |state, cx| {
                state
                    .edit_buffer_mut()
                    .set_row_state(row_idx, RowState::Error(message.clone()));
                cx.notify();
            });

            self.pending_toast = Some(PendingToast {
                message,
                is_error: true,
            });
            return;
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

        if change_values
            .iter()
            .any(|(_, value)| matches!(value, Value::Unsupported(_)))
        {
            let message = "Cannot save row: unsupported values are read-only".to_string();
            log::error!("[SAVE] {}", message);

            table_state.update(cx, |state, cx| {
                state
                    .edit_buffer_mut()
                    .set_row_state(row_idx, RowState::Error(message.clone()));
                cx.notify();
            });

            self.pending_toast = Some(PendingToast {
                message,
                is_error: true,
            });
            return;
        }

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

        let (task_id, _cancel_token) = self.runner.start_mutation(TaskKind::Query, "Save row", cx);

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx
                .update(|cx| {
                    app_state
                        .read(cx)
                        .connections()
                        .get(&profile_id)
                        .and_then(|connected| {
                            connected
                                .resolve_connection_for_execution(database.as_deref())
                                .ok()
                        })
                })
                .unwrap_or_log_dropped();

            let Some(conn) = conn else {
                log::error!("[SAVE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                    });
                })
                .log_if_dropped();
                return;
            };

            let result: Result<dbflux_core::CrudResult, dbflux_core::DbError> = cx
                .background_executor()
                .spawn(async move { conn.update_row(&patch) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    match &result {
                        Ok(_) => panel.runner.complete_mutation(task_id, cx),
                        Err(e) => panel.runner.fail_mutation(task_id, e.to_string(), cx),
                    }
                    panel.handle_save_result(row_idx, result, cx);
                });
            })
            .log_if_dropped();
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

        // Extract PK columns from schema metadata
        let pk_columns = extract_pk_columns(&self.result);

        let filter = if pk_columns.is_empty() {
            // MongoDB fallback: use _id column
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

            match &id_value {
                Value::ObjectId(oid) => {
                    DocumentFilter::new(serde_json::json!({"_id": {"$oid": oid}}))
                }
                Value::Text(s) => DocumentFilter::new(serde_json::json!({"_id": s})),
                _ => {
                    log::error!("[SAVE] Invalid _id value for document");
                    return;
                }
            }
        } else {
            // Use PK columns from metadata
            let mut filter_obj = serde_json::Map::new();
            for (col_idx, col_name) in &pk_columns {
                if let Some(cell) = model.cell(row_idx, *col_idx) {
                    filter_obj.insert(col_name.clone(), value_to_json(&cell.to_value()));
                }
            }
            DocumentFilter::new(serde_json::Value::Object(filter_obj))
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

        let (task_id, _cancel_token) =
            self.runner
                .start_mutation(TaskKind::Query, "Save document", cx);

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
                .unwrap_or_log_dropped();

            let Some(conn) = conn else {
                log::error!("[SAVE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);

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
                .log_if_dropped();
                return;
            };

            let result: Result<dbflux_core::CrudResult, dbflux_core::DbError> = cx
                .background_executor()
                .spawn(async move { conn.update_document(&update) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    match &result {
                        Ok(_) => panel.runner.complete_mutation(task_id, cx),
                        Err(e) => panel.runner.fail_mutation(task_id, e.to_string(), cx),
                    }
                    panel.handle_save_result(row_idx, result, cx);
                });
            })
            .log_if_dropped();
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

        // Chain to the next batch operation if in a pipeline
        if self.pending_batch_remaining.is_some() {
            self.process_next_batch_op(cx);
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
                profile_id,
                database,
                table,
                ..
            } => {
                self.commit_insert_table(
                    *profile_id,
                    database.clone(),
                    table.clone(),
                    insert_idx,
                    cx,
                );
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

        let (task_id, _cancel_token) =
            self.runner
                .start_mutation(TaskKind::Query, "Insert document", cx);

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
                .unwrap_or_log_dropped();

            let Some(conn) = conn else {
                log::error!("[INSERT] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                    });
                })
                .log_if_dropped();
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
                            panel.runner.complete_mutation(task_id, cx);

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

                            if panel.pending_batch_remaining.is_some() {
                                panel.process_next_batch_op(cx);
                            } else {
                                panel.pending_refresh = true;
                            }
                        }
                        Err(e) => {
                            log::error!("[INSERT] Failed: {}", e);
                            panel.runner.fail_mutation(task_id, e.to_string(), cx);
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Insert failed: {}", e),
                                is_error: true,
                            });
                        }
                    }
                    cx.notify();
                });
            })
            .log_if_dropped();
        })
        .detach();
    }

    pub(super) fn commit_insert_table(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
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

        let (task_id, _cancel_token) =
            self.runner
                .start_mutation(TaskKind::Query, "Insert row", cx);

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
                        .and_then(|connected| {
                            connected
                                .resolve_connection_for_execution(database.as_deref())
                                .ok()
                        })
                })
                .unwrap_or_log_dropped();

            let Some(conn) = conn else {
                log::error!("[INSERT] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                    });
                })
                .log_if_dropped();
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
                            panel.runner.complete_mutation(task_id, cx);

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

                            if panel.pending_batch_remaining.is_some() {
                                panel.process_next_batch_op(cx);
                            } else {
                                panel.pending_refresh = true;
                            }
                        }
                        Err(e) => {
                            log::error!("[INSERT] Failed: {}", e);
                            panel.runner.fail_mutation(task_id, e.to_string(), cx);
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Insert failed: {}", e),
                                is_error: true,
                            });
                        }
                    }
                    cx.notify();
                });
            })
            .log_if_dropped();
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
                    row_indices: vec![row_idx],
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

        if confirm.is_table {
            if let DataSource::Table {
                profile_id,
                database,
                table,
                ..
            } = &self.source
            {
                self.commit_bulk_deletes_table(
                    *profile_id,
                    database.clone(),
                    table.clone(),
                    confirm.row_indices,
                    cx,
                );
            }
        } else if let DataSource::Collection {
            profile_id,
            collection,
            ..
        } = &self.source
        {
            self.commit_bulk_deletes_collection(
                *profile_id,
                collection.clone(),
                confirm.row_indices,
                cx,
            );
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

        // Extract PK columns from schema metadata
        let pk_columns = extract_pk_columns(&self.result);

        let filter = if pk_columns.is_empty() {
            // MongoDB fallback: use _id column
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

            match &id_value {
                Value::ObjectId(oid) => {
                    dbflux_core::DocumentFilter::new(serde_json::json!({"_id": {"$oid": oid}}))
                }
                Value::Text(s) => dbflux_core::DocumentFilter::new(serde_json::json!({"_id": s})),
                _ => {
                    log::error!("[DELETE] Invalid _id value for document");
                    return;
                }
            }
        } else {
            // Use PK columns from metadata
            let state = table_state.read(cx);
            let model = state.model();
            let mut filter_obj = serde_json::Map::new();
            for (col_idx, col_name) in &pk_columns {
                if let Some(cell) = model.cell(row_idx, *col_idx) {
                    filter_obj.insert(col_name.clone(), value_to_json(&cell.to_value()));
                }
            }
            dbflux_core::DocumentFilter::new(serde_json::Value::Object(filter_obj))
        };

        let delete = dbflux_core::DocumentDelete::new(collection.name.clone(), filter)
            .with_database(collection.database.clone());

        let (task_id, _cancel_token) =
            self.runner
                .start_mutation(TaskKind::Query, "Delete document", cx);

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
                .unwrap_or_log_dropped();

            let Some(conn) = conn else {
                log::error!("[DELETE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                    });
                })
                .log_if_dropped();
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
                            panel.runner.complete_mutation(task_id, cx);

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
                            panel.runner.fail_mutation(task_id, e.to_string(), cx);
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Delete failed: {}", e),
                                is_error: true,
                            });
                        }
                    }
                    cx.notify();
                });
            })
            .log_if_dropped();
        })
        .detach();
    }

    #[allow(dead_code)]
    pub(super) fn commit_delete_table(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
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

        let (task_id, _cancel_token) =
            self.runner
                .start_mutation(TaskKind::Query, "Delete row", cx);

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
                        .and_then(|connected| {
                            connected
                                .resolve_connection_for_execution(database.as_deref())
                                .ok()
                        })
                })
                .unwrap_or_log_dropped();

            let Some(conn) = conn else {
                log::error!("[DELETE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                    });
                })
                .log_if_dropped();
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
                            panel.runner.complete_mutation(task_id, cx);

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
                            panel.runner.fail_mutation(task_id, e.to_string(), cx);
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Delete failed: {}", e),
                                is_error: true,
                            });
                        }
                    }
                    cx.notify();
                });
            })
            .log_if_dropped();
        })
        .detach();
    }

    /// Handle the SaveAllRequested event by dispatching all pending operations.
    ///
    /// Pipeline order: deletes -> inserts -> updates.
    /// Each stage completes before the next starts, and pending_refresh is only
    /// set after all operations finish.
    pub(super) fn handle_save_all(
        &mut self,
        pending_deletes: Vec<usize>,
        pending_inserts: Vec<usize>,
        dirty_rows: Vec<usize>,
        cx: &mut Context<Self>,
    ) {
        let has_remaining = !pending_inserts.is_empty() || !dirty_rows.is_empty();

        // Handle all pending deletes as a batch
        if !pending_deletes.is_empty() {
            // Store remaining ops for after deletes complete
            if has_remaining {
                self.pending_batch_remaining = Some(PendingBatchRemaining {
                    pending_inserts,
                    dirty_rows,
                });
            }

            match &self.source {
                DataSource::Collection {
                    profile_id,
                    collection,
                    ..
                } => {
                    self.commit_bulk_deletes_collection(
                        *profile_id,
                        collection.clone(),
                        pending_deletes,
                        cx,
                    );
                }
                DataSource::Table { .. } => {
                    self.pending_delete_confirm = Some(PendingDeleteConfirm {
                        row_indices: pending_deletes,
                        is_table: true,
                    });
                    cx.notify();
                }
                DataSource::QueryResult { .. } => {}
            }
            return;
        }

        // No deletes — start pipeline from inserts/dirty directly
        if has_remaining {
            self.pending_batch_remaining = Some(PendingBatchRemaining {
                pending_inserts,
                dirty_rows,
            });
            self.process_next_batch_op(cx);
        }
    }

    /// Advance the batch save pipeline by one step.
    ///
    /// Processes the next pending insert or dirty row. If none remain,
    /// sets pending_refresh to trigger a table reload.
    ///
    /// This is called from async completion callbacks (insert/delete/save)
    /// to chain the next operation in the pipeline without triggering an
    /// intermediate refresh that would destroy the edit buffer.
    pub(super) fn process_next_batch_op(&mut self, cx: &mut Context<Self>) {
        let Some(mut remaining) = self.pending_batch_remaining.take() else {
            self.pending_refresh = true;
            cx.notify();
            return;
        };

        if let Some(insert_idx) = remaining.pending_inserts.first().copied() {
            remaining.pending_inserts.remove(0);
            if !remaining.pending_inserts.is_empty() || !remaining.dirty_rows.is_empty() {
                self.pending_batch_remaining = Some(remaining);
            }
            self.handle_commit_insert(insert_idx, cx);
            return;
        }

        if let Some(row_idx) = remaining.dirty_rows.first().copied() {
            remaining.dirty_rows.remove(0);
            if !remaining.dirty_rows.is_empty() {
                self.pending_batch_remaining = Some(remaining);
            }
            self.handle_save_row(row_idx, cx);
            return;
        }

        // All batch operations complete
        self.pending_refresh = true;
        cx.notify();
    }

    /// Execute multiple row deletes for a SQL table in a single async block.
    /// Reads all PK identities upfront, executes deletes sequentially, then
    /// triggers a single refresh at the end.
    pub(super) fn commit_bulk_deletes_table(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        table_ref: TableRef,
        row_indices: Vec<usize>,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let count = row_indices.len();
        let pk_indices = {
            let state = table_state.read(cx);
            state.pk_columns()
        };

        if pk_indices.is_empty() {
            self.pending_toast = Some(PendingToast {
                message: "Cannot delete: no primary key defined for this table".to_string(),
                is_error: true,
            });
            cx.notify();
            return;
        }

        // Collect PK identity for every row upfront before any async work
        let mut identities = Vec::with_capacity(count);
        {
            let state = table_state.read(cx);
            let model = state.model();

            for &row_idx in &row_indices {
                let mut pk_columns = Vec::with_capacity(pk_indices.len());
                let mut pk_values = Vec::with_capacity(pk_indices.len());

                for &col_idx in pk_indices.iter() {
                    if let Some(col_spec) = model.columns.get(col_idx) {
                        pk_columns.push(col_spec.title.to_string());
                    }
                    if let Some(cell) = model.cell(row_idx, col_idx) {
                        pk_values.push(cell.to_value());
                    }
                }

                if pk_columns.len() != pk_indices.len() || pk_values.len() != pk_indices.len() {
                    log::error!(
                        "[BULK DELETE] Failed to build row identity for row {}",
                        row_idx
                    );
                    continue;
                }

                identities.push((row_idx, RowIdentity::new(pk_columns, pk_values)));
            }
        }

        if identities.is_empty() {
            self.pending_toast = Some(PendingToast {
                message: "Cannot delete: failed to identify any rows".to_string(),
                is_error: true,
            });
            cx.notify();
            return;
        }

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::Query,
            &format!("Delete {} row(s)", identities.len()),
            cx,
        );

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();
        let table_state_clone = table_state.clone();
        let table_name = table_ref.name.clone();
        let schema_name = table_ref.schema.clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx
                .update(|cx| {
                    app_state
                        .read(cx)
                        .connections()
                        .get(&profile_id)
                        .and_then(|connected| {
                            connected
                                .resolve_connection_for_execution(database.as_deref())
                                .ok()
                        })
                })
                .unwrap_or_log_dropped();

            let Some(conn) = conn else {
                log::error!("[BULK DELETE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                    });
                })
                .log_if_dropped();
                return;
            };

            let mut success_count = 0usize;
            let mut last_error: Option<dbflux_core::DbError> = None;

            for (row_idx, identity) in &identities {
                let delete =
                    RowDelete::new(identity.clone(), table_name.clone(), schema_name.clone());

                let conn = conn.clone();
                let result = cx
                    .background_executor()
                    .spawn(async move { conn.delete_row(&delete) })
                    .await;

                match result {
                    Ok(_) => {
                        success_count += 1;
                    }
                    Err(e) => {
                        log::error!("[BULK DELETE] Failed to delete row {}: {}", row_idx, e);
                        last_error = Some(e);
                        // Stop on first error — remaining rows may depend on consistency
                        break;
                    }
                }
            }

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    if let Some(e) = last_error {
                        panel.runner.fail_mutation(task_id, e.to_string(), cx);
                        panel.pending_toast = Some(PendingToast {
                            message: format!(
                                "Deleted {} of {} row(s), then failed: {}",
                                success_count,
                                identities.len(),
                                e
                            ),
                            is_error: true,
                        });
                    } else {
                        panel.runner.complete_mutation(task_id, cx);

                        // Unmark all successfully deleted rows
                        table_state_clone.update(cx, |state, cx| {
                            for i in 0..success_count {
                                let (row_idx, _) = &identities[i];
                                state.edit_buffer_mut().unmark_delete(*row_idx);
                            }
                            cx.notify();
                        });

                        panel.pending_toast = Some(PendingToast {
                            message: format!("{} row(s) deleted", success_count),
                            is_error: false,
                        });

                        if panel.pending_batch_remaining.is_some() {
                            panel.process_next_batch_op(cx);
                        } else {
                            panel.pending_refresh = true;
                        }
                    }
                    cx.notify();
                });
            })
            .log_if_dropped();
        })
        .detach();
    }

    /// Execute multiple document deletes for a collection in a single async block.
    pub(super) fn commit_bulk_deletes_collection(
        &mut self,
        profile_id: Uuid,
        collection: CollectionRef,
        row_indices: Vec<usize>,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let count = row_indices.len();
        let pk_columns = extract_pk_columns(&self.result);

        // Collect filters for every document upfront
        let mut filters = Vec::with_capacity(count);
        {
            let state = table_state.read(cx);
            let model = state.model();

            for &row_idx in &row_indices {
                let filter = if pk_columns.is_empty() {
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

                    match &id_value {
                        Value::ObjectId(oid) => dbflux_core::DocumentFilter::new(
                            serde_json::json!({"_id": {"$oid": oid}}),
                        ),
                        Value::Text(s) => {
                            dbflux_core::DocumentFilter::new(serde_json::json!({"_id": s}))
                        }
                        _ => {
                            log::error!(
                                "[BULK DELETE] Invalid _id value for document at row {}",
                                row_idx
                            );
                            continue;
                        }
                    }
                } else {
                    let mut filter_obj = serde_json::Map::new();
                    for (col_idx, col_name) in &pk_columns {
                        if let Some(cell) = model.cell(row_idx, *col_idx) {
                            filter_obj.insert(col_name.clone(), value_to_json(&cell.to_value()));
                        }
                    }
                    dbflux_core::DocumentFilter::new(serde_json::Value::Object(filter_obj))
                };

                filters.push((row_idx, filter));
            }
        }

        if filters.is_empty() {
            self.pending_toast = Some(PendingToast {
                message: "Cannot delete: failed to identify any documents".to_string(),
                is_error: true,
            });
            cx.notify();
            return;
        }

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::Query,
            &format!("Delete {} document(s)", filters.len()),
            cx,
        );

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();
        let table_state_clone = table_state.clone();
        let collection_name = collection.name.clone();
        let collection_db = collection.database.clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx
                .update(|cx| {
                    app_state
                        .read(cx)
                        .connections()
                        .get(&profile_id)
                        .map(|c| c.connection.clone())
                })
                .unwrap_or_log_dropped();

            let Some(conn) = conn else {
                log::error!("[BULK DELETE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                    });
                })
                .log_if_dropped();
                return;
            };

            let mut success_count = 0usize;
            let mut last_error: Option<dbflux_core::DbError> = None;

            for (row_idx, filter) in &filters {
                let delete =
                    dbflux_core::DocumentDelete::new(collection_name.clone(), filter.clone())
                        .with_database(collection_db.clone());

                let conn = conn.clone();
                let result = cx
                    .background_executor()
                    .spawn(async move { conn.delete_document(&delete) })
                    .await;

                match result {
                    Ok(_) => {
                        success_count += 1;
                    }
                    Err(e) => {
                        log::error!(
                            "[BULK DELETE] Failed to delete document at row {}: {}",
                            row_idx,
                            e
                        );
                        last_error = Some(e);
                        break;
                    }
                }
            }

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    if let Some(e) = last_error {
                        panel.runner.fail_mutation(task_id, e.to_string(), cx);
                        panel.pending_toast = Some(PendingToast {
                            message: format!(
                                "Deleted {} of {} document(s), then failed: {}",
                                success_count,
                                filters.len(),
                                e
                            ),
                            is_error: true,
                        });
                    } else {
                        panel.runner.complete_mutation(task_id, cx);

                        table_state_clone.update(cx, |state, cx| {
                            for i in 0..success_count {
                                let (row_idx, _) = &filters[i];
                                state.edit_buffer_mut().unmark_delete(*row_idx);
                            }
                            cx.notify();
                        });

                        panel.pending_toast = Some(PendingToast {
                            message: format!("{} document(s) deleted", success_count),
                            is_error: false,
                        });

                        if panel.pending_batch_remaining.is_some() {
                            panel.process_next_batch_op(cx);
                        } else {
                            panel.pending_refresh = true;
                        }
                    }
                    cx.notify();
                });
            })
            .log_if_dropped();
        })
        .detach();
    }
}
