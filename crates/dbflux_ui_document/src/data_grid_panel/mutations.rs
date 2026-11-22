use super::edit_guard::OnStop;
use super::expression_gate::HeldSave;
use super::utils::{extract_pk_columns, value_to_json};
use super::*;
use super::{
    DataGridEvent, DataGridPanel, DataSource, PendingBatchRemaining, PendingDeleteConfirm,
    PendingToast,
};
use dbflux_components::components::document_tree::NodeId;
use dbflux_core::{
    CollectionRef, ColumnAssignment, DocumentFilter, DocumentUpdate, Pagination, QueryResult,
    RowDelete, RowIdentity, RowInsert, RowPatch, RowState, TableRef, TaskKind, Value,
};
use dbflux_ui_base::AsyncUpdateResultExt;
use dbflux_ui_base::toast::{Toast, copy_action, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::*;
use std::collections::BTreeMap;
use uuid::Uuid;

/// The assignment a grid cell stages: its expression, written verbatim, or
/// its value, bound.
fn cell_assignment(
    name: &str,
    type_name: &str,
    cell: &dbflux_components::components::data_table::model::CellValue,
) -> ColumnAssignment {
    ColumnAssignment {
        name: name.to_string(),
        value: cell.to_value(),
        type_name: Some(type_name.to_string()),
        expression: cell.as_expression().map(str::to_owned),
    }
}

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
            // The Array arm indexes items[item_idx] after the
            // item_idx >= items.len() early return, so the bound is
            // enforced inside the arm.
            #[expect(
                clippy::indexing_slicing,
                reason = "the Array arm indexes items[item_idx] only after item_idx >= items.len() returned false"
            )]
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

    pub(super) fn queue_refresh_after_mutation_success(
        &mut self,
        batch_owned: bool,
        cx: &mut Context<Self>,
    ) {
        if batch_owned {
            self.batch_op_in_flight = false;

            if self.pending_batch_remaining.is_some() {
                self.process_next_batch_op(cx);
                return;
            }

            self.finish_close_after_apply(cx);
        }

        self.queue_reload_after_mutation(cx);
    }

    /// Queue the reload that shows a landed mutation. Edits still staged on
    /// other rows are carried over to the reloaded rows by primary key.
    /// Without a primary key they could only be dropped, so the reload is
    /// refused with the same warning as a user refresh instead.
    pub(super) fn queue_reload_after_mutation(&mut self, cx: &mut Context<Self>) {
        if self.pending_edits_lack_row_identity(cx) && self.reload_blocked_by_pending_edits(cx) {
            return;
        }

        self.pending.refresh = true;
        self.pending.refresh_keeps_edits = true;
    }

    fn apply_inline_value_to_result(&mut self, node_id: &NodeId, new_value: &Value) {
        let Some(doc_index) = node_id.doc_index() else {
            return;
        };

        let Some(row) = self.result.rows.get_mut(doc_index) else {
            return;
        };

        let mut updated = false;

        // doc_index() returned Some, so node_id.path has at least one element
        // and [1..] is in range; the [1] index below is covered by the
        // path.len() == 2 check.
        #[expect(
            clippy::indexing_slicing,
            reason = "doc_index() returned Some, so node_id.path has a leading element and [1..] is in range"
        )]
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
            #[expect(
                clippy::indexing_slicing,
                reason = "path.len() == 2 is checked by the enclosing if, so index 1 exists"
            )]
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
                    report_error(
                        UserFacingError::new(
                            ErrorKind::User,
                            dbflux_i18n::t!(
                                "document.data.mutation.error.update_document_unsupported_id"
                            ),
                        ),
                        cx,
                    );
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

        // doc_index() returned Some and path.len() < 2 returned early, so the
        // path keeps its leading document segment and [1..] is in range.
        #[expect(
            clippy::indexing_slicing,
            reason = "doc_index() returned Some and path.len() < 2 returned early, so [1..] is in range"
        )]
        let field_path = node_id.path[1..].join(".");
        let parsed_json = parse_inline_document_value(new_value);
        let inline_value = json_to_inline_value(&parsed_json);
        let node_id = node_id.clone();

        let mut set_fields = serde_json::Map::new();
        set_fields.insert(field_path, parsed_json);

        let update_doc = serde_json::json!({"$set": set_fields});
        let update = DocumentUpdate::new(collection.name.clone(), filter, update_doc)
            .with_database(collection.database.clone());

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::Query,
            crate::labels::mutation_update_document_field_task_label(),
            cx,
        );

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();
        let profile_id = *profile_id;

        cx.spawn(async move |_this, cx| {
            let conn = cx.update(|cx| {
                app_state
                    .read(cx)
                    .connections()
                    .get(&profile_id)
                    .map(|c| c.connection.clone())
            });

            let Some(conn) = conn else {
                log::error!("[SAVE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                        panel.abandon_close_apply_independent(cx);
                    });
                });
                return;
            };

            let result: Result<dbflux_core::CrudResult, dbflux_core::DbError> = cx
                .background_executor()
                .spawn(async move { conn.update_document(&update) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    match result {
                        Ok(mut crud_result) => {
                            crate::result_warnings::handoff_crud_returning_result(
                                &mut crud_result,
                                |warning| report_error(warning, cx),
                            );
                            panel.runner.complete_mutation(task_id, cx);
                            panel.apply_inline_value_to_result(&node_id, &inline_value);

                            if let Some(tree_state) =
                                panel.document_view.document_tree_state.clone()
                            {
                                tree_state.update(cx, |state, cx| {
                                    state.apply_inline_edit_value(
                                        &node_id,
                                        inline_value.clone(),
                                        cx,
                                    );
                                });
                            }

                            panel.pending.toast = Some(PendingToast {
                                message: dbflux_i18n::t!(
                                    "document.data.mutation.toast.document_updated"
                                ),
                                is_error: false,
                            });
                        }
                        Err(error) => {
                            panel.runner.fail_mutation(task_id, error.to_string(), cx);
                            report_error(
                                UserFacingError::new(
                                    ErrorKind::Driver,
                                    dbflux_i18n::t!(
                                        "document.data.mutation.error.update_document_failed",
                                        error = error
                                    ),
                                ),
                                cx,
                            );
                        }
                    }
                    cx.notify();
                });
            });
        })
        .detach();
    }

    /// The cells staged on `row_idx`, or `None` when the grid is not editable
    /// or the row has nothing staged.
    fn staged_row_changes(
        &self,
        row_idx: usize,
        cx: &App,
    ) -> Option<
        Vec<(
            usize,
            dbflux_components::components::data_table::model::CellValue,
        )>,
    > {
        let state = self.grid_table.table_state.as_ref()?.read(cx);

        if !state.is_editable() {
            return None;
        }

        let row_changes = state.edit_buffer().row_changes(row_idx);
        if row_changes.is_empty() {
            return None;
        }

        Some(
            row_changes
                .into_iter()
                .map(|(idx, cell)| (idx, cell.clone()))
                .collect(),
        )
    }

    pub(super) fn handle_save_row(
        &mut self,
        row_idx: usize,
        batch_owned: bool,
        cx: &mut Context<Self>,
    ) {
        if !self.queued_row_has_changes(row_idx, cx) {
            return;
        }

        if self.hold_save_for_expression_confirm(HeldSave::Row(row_idx), batch_owned, cx) {
            return;
        }

        // A batch was checked once as a whole before its first write.
        if !batch_owned
            && let DataSource::Table {
                profile_id,
                database,
                table,
                ..
            } = &self.source
        {
            let (profile_id, database, table) = (*profile_id, database.clone(), table.clone());
            // The key and the staged values are read now: a reload while the
            // check runs may move the row to another index.
            let Some(changes) = self.staged_row_changes(row_idx, cx) else {
                return;
            };
            let changes_ref: Vec<_> = changes.iter().map(|(idx, cell)| (*idx, cell)).collect();
            let Some(patch) = self.build_row_patch(&table, row_idx, &changes_ref, false, cx) else {
                return;
            };
            let sent: Vec<(usize, SentCell)> = changes;
            self.guard_edit(
                OnStop::Nothing,
                move |panel, cx| {
                    let Some(row_idx) = panel.row_index_of_key(patch.identity.values(), cx) else {
                        report_error(
                            UserFacingError::new(
                                ErrorKind::User,
                                dbflux_i18n::t!(
                                    "document.data.mutation.error.staged_row_left_grid"
                                ),
                            ),
                            cx,
                        );
                        return;
                    };
                    // The user reverted the row while the key was checked.
                    if !panel.queued_row_has_changes(row_idx, cx) {
                        return;
                    }
                    panel.send_row_patch(
                        profile_id,
                        database,
                        row_idx,
                        RowSave { patch, sent },
                        false,
                        cx,
                    );
                },
                cx,
            );
            return;
        }

        self.send_row_save(row_idx, batch_owned, cx);
    }

    fn send_row_save(&mut self, row_idx: usize, batch_owned: bool, cx: &mut Context<Self>) {
        let Some(changes) = self.staged_row_changes(row_idx, cx) else {
            return;
        };

        let changes_ref: Vec<(
            usize,
            &dbflux_components::components::data_table::model::CellValue,
        )> = changes.iter().map(|(idx, cell)| (*idx, cell)).collect();

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
                    batch_owned,
                    cx,
                );
            }
            DataSource::Collection {
                profile_id,
                collection,
                ..
            } => {
                self.save_document(
                    *profile_id,
                    collection.clone(),
                    row_idx,
                    &changes_ref,
                    batch_owned,
                    cx,
                );
            }
            DataSource::QueryResult { .. } => {}
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn save_table_row(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        table_ref: TableRef,
        row_idx: usize,
        changes: &[(
            usize,
            &dbflux_components::components::data_table::model::CellValue,
        )],
        batch_owned: bool,
        cx: &mut Context<Self>,
    ) {
        let Some(patch) = self.build_row_patch(&table_ref, row_idx, changes, batch_owned, cx)
        else {
            return;
        };
        let sent = changes
            .iter()
            .map(|&(col, cell)| (col, cell.clone()))
            .collect();
        self.send_row_patch(
            profile_id,
            database,
            row_idx,
            RowSave { patch, sent },
            batch_owned,
            cx,
        );
    }

    /// The patch for `row_idx`, with the row's key and staged values read now.
    /// `None` when the row cannot be identified or holds a value that cannot
    /// be written; the failure is already reported.
    pub(super) fn build_row_patch(
        &mut self,
        table_ref: &TableRef,
        row_idx: usize,
        changes: &[(
            usize,
            &dbflux_components::components::data_table::model::CellValue,
        )],
        batch_owned: bool,
        cx: &mut Context<Self>,
    ) -> Option<RowPatch> {
        let table_state = self.grid_table.table_state.as_ref()?;

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
            let message = dbflux_i18n::t!("document.data.mutation.error.save_row_unsupported_pk");

            table_state.update(cx, |state, cx| {
                state
                    .edit_buffer_mut()
                    .set_row_state(row_idx, RowState::Error(message.clone()));
                cx.notify();
            });

            if batch_owned {
                self.abandon_close_apply(cx);
            }
            report_error(UserFacingError::new(ErrorKind::Driver, message), cx);
            return None;
        }

        if pk_columns.len() != pk_indices.len() || pk_values.len() != pk_indices.len() {
            if batch_owned {
                self.abandon_close_apply(cx);
            }
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.data.mutation.error.save_row_identity_failed"),
                ),
                cx,
            );
            return None;
        }

        let identity = RowIdentity::new(pk_columns, pk_values);

        let change_values: Vec<ColumnAssignment> = changes
            .iter()
            .filter_map(|&(col_idx, cell_value)| {
                model
                    .columns
                    .get(col_idx)
                    .map(|col| cell_assignment(&col.title, &col.type_name, cell_value))
            })
            .collect();

        if change_values
            .iter()
            .any(|a| matches!(a.value, Value::Unsupported(_)))
        {
            let message =
                dbflux_i18n::t!("document.data.mutation.error.save_row_unsupported_values");

            table_state.update(cx, |state, cx| {
                state
                    .edit_buffer_mut()
                    .set_row_state(row_idx, RowState::Error(message.clone()));
                cx.notify();
            });

            if batch_owned {
                self.abandon_close_apply(cx);
            }
            report_error(UserFacingError::new(ErrorKind::Driver, message), cx);
            return None;
        }

        let patch = RowPatch::with_typed_changes(
            identity,
            table_ref.name.clone(),
            table_ref.schema.clone(),
            change_values,
        );

        Some(patch)
    }

    /// Sends a patch built by [`Self::build_row_patch`] for `row_idx`.
    pub(super) fn send_row_patch(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        row_idx: usize,
        save: RowSave,
        batch_owned: bool,
        cx: &mut Context<Self>,
    ) {
        let RowSave { patch, sent } = save;
        let Some(table_state) = self.grid_table.table_state.clone() else {
            return;
        };

        table_state.update(cx, |state, cx| {
            state
                .edit_buffer_mut()
                .set_row_state(row_idx, RowState::Saving);
            cx.notify();
        });

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::Query,
            crate::labels::mutation_save_row_task_label(),
            cx,
        );

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx.update(|cx| {
                app_state
                    .read(cx)
                    .connections()
                    .get(&profile_id)
                    .and_then(|connected| {
                        connected
                            .resolve_connection_for_execution(database.as_deref())
                            .ok()
                    })
            });

            let Some(conn) = conn else {
                log::error!("[SAVE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                        if batch_owned {
                            panel.abandon_close_apply(cx);
                        }
                    });
                });
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
                    panel.finish_save_result(row_idx, result, Some(sent), batch_owned, cx);
                });
            });
        })
        .detach();
    }

    pub(super) fn save_document(
        &mut self,
        profile_id: Uuid,
        collection: CollectionRef,
        row_idx: usize,
        changes: &[(
            usize,
            &dbflux_components::components::data_table::model::CellValue,
        )],
        batch_owned: bool,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.grid_table.table_state else {
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
                    if batch_owned {
                        self.abandon_close_apply(cx);
                    }
                    report_error(
                        UserFacingError::new(
                            ErrorKind::User,
                            dbflux_i18n::t!(
                                "document.data.mutation.error.save_document_unsupported_id"
                            ),
                        ),
                        cx,
                    );
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

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::Query,
            crate::labels::mutation_save_document_task_label(),
            cx,
        );

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx.update(|cx| {
                app_state
                    .read(cx)
                    .connections()
                    .get(&profile_id)
                    .map(|c| c.connection.clone())
            });

            let Some(conn) = conn else {
                log::error!("[SAVE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                        if batch_owned {
                            panel.abandon_close_apply(cx);
                        }

                        if let Some(table_state) = &panel.grid_table.table_state {
                            table_state.update(cx, |state, cx| {
                                state.edit_buffer_mut().set_row_state(
                                    row_idx,
                                    RowState::Error("No connection".to_string()),
                                );
                                cx.notify();
                            });
                        }
                    });
                });
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
                    panel.handle_save_result(row_idx, result, batch_owned, cx);
                });
            });
        })
        .detach();
    }

    pub(super) fn handle_save_result(
        &mut self,
        row_idx: usize,
        result: Result<dbflux_core::CrudResult, dbflux_core::DbError>,
        batch_owned: bool,
        cx: &mut Context<Self>,
    ) {
        self.finish_save_result(row_idx, result, None, batch_owned, cx);
    }

    /// `sent` is the staged cells the write carried. When given, only those are
    /// cleared after a success: a value staged after the patch was built has
    /// not been written and stays staged.
    fn finish_save_result(
        &mut self,
        row_idx: usize,
        result: Result<dbflux_core::CrudResult, dbflux_core::DbError>,
        sent: Option<Vec<(usize, SentCell)>>,
        batch_owned: bool,
        cx: &mut Context<Self>,
    ) {
        if batch_owned {
            self.batch_op_in_flight = false;
        }

        let Some(table_state) = &self.grid_table.table_state else {
            return;
        };

        match result {
            Ok(mut crud_result) => {
                crate::result_warnings::handoff_crud_returning_result(
                    &mut crud_result,
                    |warning| report_error(warning, cx),
                );

                let grid_columns: Vec<String> = self
                    .result
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect();
                let table_columns: Option<Vec<String>> =
                    self.table_views.profile.as_ref().map(|profile| {
                        profile
                            .columns
                            .iter()
                            .map(|column| column.name.to_string())
                            .collect()
                    });
                let returned_row = crud_result.returning_row.as_deref().and_then(|row| {
                    super::table_views::returned_row_for_grid(
                        row,
                        &grid_columns,
                        table_columns.as_deref(),
                    )
                });

                let mut reload_for_expression = false;
                table_state.update(cx, |state, cx| {
                    match returned_row {
                        Some(returned_row) => state.apply_returning_row(row_idx, &returned_row),
                        None if crud_result.returning_row.is_some() => {
                            // The database evaluated any expression, and the
                            // grid cannot see what it produced (a key may have
                            // changed), so the row is reloaded below.
                            reload_for_expression = state
                                .edit_buffer()
                                .row_changes(row_idx)
                                .iter()
                                .any(|(_, cell)| cell.as_expression().is_some());
                            state.keep_row_edits(row_idx)
                        }
                        None => {}
                    }
                    match &sent {
                        Some(sent) => clear_sent_cells(state.edit_buffer_mut(), row_idx, sent),
                        None => state.edit_buffer_mut().clear_row(row_idx),
                    }
                    cx.notify();
                });
                if reload_for_expression {
                    self.queue_reload_after_mutation(cx);
                }
                self.pending.toast = Some(PendingToast {
                    message: dbflux_i18n::t!("document.data.mutation.toast.saved"),
                    is_error: false,
                });
            }
            Err(e) => {
                table_state.update(cx, |state, cx| {
                    state
                        .edit_buffer_mut()
                        .set_row_state(row_idx, RowState::Error(e.to_string()));
                    cx.notify();
                });
                if batch_owned {
                    self.abandon_close_apply(cx);
                }
                report_error(
                    UserFacingError::new(
                        ErrorKind::Driver,
                        dbflux_i18n::t!("document.data.mutation.error.save_failed", error = e),
                    ),
                    cx,
                );
            }
        }

        // Chain the next batch operation. An independent save (the value
        // panel's Save, the toolbar's Save Row) must not advance or finish a
        // batch it is not part of.
        if batch_owned {
            if self.pending_batch_remaining.is_some() {
                self.process_next_batch_op(cx);
            } else {
                // The row that just landed was the batch's last one.
                self.finish_close_after_apply(cx);
            }
        }

        cx.notify();
    }

    pub(super) fn handle_commit_insert(
        &mut self,
        insert_idx: usize,
        batch_owned: bool,
        cx: &mut Context<Self>,
    ) {
        if self.hold_save_for_expression_confirm(HeldSave::Insert(insert_idx), batch_owned, cx) {
            return;
        }

        match &self.source {
            DataSource::Collection {
                profile_id,
                collection,
                ..
            } => {
                self.commit_insert_collection(
                    *profile_id,
                    collection.clone(),
                    insert_idx,
                    batch_owned,
                    cx,
                );
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
                    batch_owned,
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
        batch_owned: bool,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.grid_table.table_state else {
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

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::Query,
            crate::labels::mutation_insert_document_task_label(),
            cx,
        );

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();
        let table_state_clone = table_state.clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx.update(|cx| {
                app_state
                    .read(cx)
                    .connections()
                    .get(&profile_id)
                    .map(|c| c.connection.clone())
            });

            let Some(conn) = conn else {
                log::error!("[INSERT] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                        if batch_owned {
                            panel.abandon_close_apply(cx);
                        }
                    });
                });
                return;
            };

            let result = cx
                .background_executor()
                .spawn(async move { conn.insert_document(&insert) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    match result {
                        Ok(mut crud_result) => {
                            crate::result_warnings::handoff_crud_returning_result(
                                &mut crud_result,
                                |warning| report_error(warning, cx),
                            );
                            panel.runner.complete_mutation(task_id, cx);

                            table_state_clone.update(cx, |state, cx| {
                                state
                                    .edit_buffer_mut()
                                    .remove_pending_insert_by_idx(insert_idx);
                                cx.notify();
                            });
                            panel.renumber_queued_inserts_after_removal(insert_idx, cx);
                            panel.pending.toast = Some(PendingToast {
                                message: dbflux_i18n::t!(
                                    "document.data.mutation.toast.document_inserted"
                                ),
                                is_error: false,
                            });
                            panel.queue_refresh_after_mutation_success(batch_owned, cx);
                        }
                        Err(e) => {
                            panel.runner.fail_mutation(task_id, e.to_string(), cx);
                            if batch_owned {
                                panel.abandon_close_apply(cx);
                            }
                            report_error(
                                UserFacingError::new(
                                    ErrorKind::Driver,
                                    dbflux_i18n::t!(
                                        "document.data.mutation.error.insert_failed",
                                        error = e
                                    ),
                                ),
                                cx,
                            );
                        }
                    }
                    cx.notify();
                });
            });
        })
        .detach();
    }

    pub(super) fn commit_insert_table(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        table_ref: TableRef,
        insert_idx: usize,
        batch_owned: bool,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.grid_table.table_state else {
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

        let assignments: Vec<ColumnAssignment> = {
            let state = table_state.read(cx);
            let model = state.model();

            let mut assignments = Vec::new();

            for (col_idx, cell) in cells.iter().enumerate() {
                // An expression is a value of its own: it is written even
                // though its bound value is NULL.
                if matches!(cell.to_value(), Value::Null) && cell.as_expression().is_none() {
                    continue;
                }

                if let Some(col) = model.columns.get(col_idx) {
                    assignments.push(cell_assignment(&col.title, &col.type_name, cell));
                }
            }

            assignments
        };

        if assignments.is_empty() {
            if batch_owned {
                self.abandon_close_apply(cx);
            }
            report_error(
                UserFacingError::new(
                    ErrorKind::Driver,
                    dbflux_i18n::t!("document.data.mutation.error.insert_no_values"),
                ),
                cx,
            );
            return;
        }

        let insert = RowInsert::with_typed_assignments(
            table_ref.name.clone(),
            table_ref.schema.clone(),
            assignments,
        );

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::Query,
            crate::labels::mutation_insert_row_task_label(),
            cx,
        );

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();
        let table_state_clone = table_state.clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx.update(|cx| {
                app_state
                    .read(cx)
                    .connections()
                    .get(&profile_id)
                    .and_then(|connected| {
                        connected
                            .resolve_connection_for_execution(database.as_deref())
                            .ok()
                    })
            });

            let Some(conn) = conn else {
                log::error!("[INSERT] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                        if batch_owned {
                            panel.abandon_close_apply(cx);
                        }
                    });
                });
                return;
            };

            let result = cx
                .background_executor()
                .spawn(async move { conn.insert_row(&insert) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    match result {
                        Ok(mut crud_result) => {
                            crate::result_warnings::handoff_crud_returning_result(
                                &mut crud_result,
                                |warning| report_error(warning, cx),
                            );
                            panel.runner.complete_mutation(task_id, cx);

                            table_state_clone.update(cx, |state, cx| {
                                state
                                    .edit_buffer_mut()
                                    .remove_pending_insert_by_idx(insert_idx);
                                cx.notify();
                            });
                            panel.renumber_queued_inserts_after_removal(insert_idx, cx);
                            panel.pending.toast = Some(PendingToast {
                                message: dbflux_i18n::t!(
                                    "document.data.mutation.toast.row_inserted"
                                ),
                                is_error: false,
                            });
                            panel.queue_refresh_after_mutation_success(batch_owned, cx);
                        }
                        Err(e) => {
                            panel.runner.fail_mutation(task_id, e.to_string(), cx);
                            if batch_owned {
                                panel.abandon_close_apply(cx);
                            }
                            report_error(
                                UserFacingError::new(
                                    ErrorKind::Driver,
                                    dbflux_i18n::t!(
                                        "document.data.mutation.error.insert_failed",
                                        error = e
                                    ),
                                ),
                                cx,
                            );
                        }
                    }
                    cx.notify();
                });
            });
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
                let (profile_id, database, table) = (*profile_id, database.clone(), table.clone());
                // A write already running is not this delete's to abandon.
                let on_stop = if self.batch_op_in_flight {
                    OnStop::AbandonIndependent
                } else {
                    OnStop::AbandonBatch
                };
                if let Some((identities, unidentified)) =
                    self.capture_delete_identities(&confirm.row_indices, cx)
                {
                    self.batch_op_in_flight = true;
                    self.guard_edit(
                        on_stop,
                        move |panel, cx| {
                            panel.run_captured_deletes(
                                profile_id,
                                database,
                                table,
                                identities,
                                unidentified,
                                cx,
                            );
                        },
                        cx,
                    );
                }
            }
        } else if let DataSource::Collection {
            profile_id,
            collection,
            ..
        } = &self.source
        {
            self.batch_op_in_flight = true;
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
            // A close waiting on these deletes cannot be satisfied by a
            // confirmation the user dismissed: the tab stays open with its edits.
            self.abandon_close_apply(cx);
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
        let Some(table_state) = &self.grid_table.table_state else {
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
                    report_error(
                        UserFacingError::new(
                            ErrorKind::User,
                            dbflux_i18n::t!(
                                "document.data.mutation.error.delete_document_unsupported_id"
                            ),
                        ),
                        cx,
                    );
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

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::Query,
            crate::labels::mutation_delete_document_task_label(),
            cx,
        );

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();
        let table_state_clone = table_state.clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx.update(|cx| {
                app_state
                    .read(cx)
                    .connections()
                    .get(&profile_id)
                    .map(|c| c.connection.clone())
            });

            let Some(conn) = conn else {
                log::error!("[DELETE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                        panel.abandon_close_apply_independent(cx);
                    });
                });
                return;
            };

            let result = cx
                .background_executor()
                .spawn(async move { conn.delete_document(&delete) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    match result {
                        Ok(mut crud_result) => {
                            crate::result_warnings::handoff_crud_returning_result(
                                &mut crud_result,
                                |warning| report_error(warning, cx),
                            );
                            panel.runner.complete_mutation(task_id, cx);

                            table_state_clone.update(cx, |state, cx| {
                                state.edit_buffer_mut().unmark_delete(row_idx);
                                cx.notify();
                            });
                            panel.pending.toast = Some(PendingToast {
                                message: dbflux_i18n::t!(
                                    "document.data.mutation.toast.document_deleted"
                                ),
                                is_error: false,
                            });
                            panel.queue_reload_after_mutation(cx);
                        }
                        Err(e) => {
                            panel.runner.fail_mutation(task_id, e.to_string(), cx);
                            report_error(
                                UserFacingError::new(
                                    ErrorKind::Driver,
                                    dbflux_i18n::t!(
                                        "document.data.mutation.error.delete_failed",
                                        error = e
                                    ),
                                ),
                                cx,
                            );
                        }
                    }
                    cx.notify();
                });
            });
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
        // A second request while a batch is checked or running would capture
        // the same inserts and rows and send them again.
        if matches!(self.source, DataSource::Table { .. })
            && (self.batch_op_in_flight || self.pending_batch_remaining.is_some())
        {
            return;
        }

        let has_remaining = !pending_inserts.is_empty() || !dirty_rows.is_empty();

        // Handle all pending deletes as a batch
        if !pending_deletes.is_empty() {
            // Store remaining ops for after deletes complete
            if has_remaining {
                let dirty_keys = self.dirty_row_keys(&dirty_rows, cx);
                self.pending_batch_remaining = Some(PendingBatchRemaining {
                    pending_inserts,
                    dirty_rows,
                    dirty_keys,
                });
            }

            match &self.source {
                DataSource::Collection {
                    profile_id,
                    collection,
                    ..
                } => {
                    self.batch_op_in_flight = true;
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
        if !has_remaining {
            return;
        }

        let needs_key_check =
            !dirty_rows.is_empty() && matches!(self.source, DataSource::Table { .. });
        let dirty_keys = self.dirty_row_keys(&dirty_rows, cx);
        let remaining = PendingBatchRemaining {
            pending_inserts,
            dirty_rows,
            dirty_keys,
        };

        if needs_key_check {
            // Held so a close does not start the same rows a second time.
            self.batch_op_in_flight = true;
            // Parked on the guard, not in the closure, so an insert removed
            // while the key is checked still renumbers the batch.
            self.edit_guard.parked_batch = Some(remaining);
            self.guard_edit(
                OnStop::AbandonBatch,
                move |panel, cx| {
                    panel.batch_op_in_flight = false;
                    panel.pending_batch_remaining = panel.edit_guard.parked_batch.take();
                    panel.process_next_batch_op(cx);
                },
                cx,
            );
            return;
        }

        self.pending_batch_remaining = Some(remaining);
        self.process_next_batch_op(cx);
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
            self.queue_reload_after_mutation(cx);
            cx.notify();
            return;
        };

        loop {
            if let Some(insert_idx) = remaining.pending_inserts.first().copied() {
                remaining.pending_inserts.remove(0);
                if !self.queued_insert_is_pending(insert_idx, cx) {
                    // The insert was removed before the pump reached it. It has
                    // nothing to write, so it must not park the batch.
                    continue;
                }
                if !remaining.pending_inserts.is_empty() || !remaining.dirty_rows.is_empty() {
                    self.pending_batch_remaining = Some(remaining);
                }
                self.batch_op_in_flight = true;
                self.handle_commit_insert(insert_idx, true, cx);
                return;
            }

            if let Some(queued_idx) = remaining.dirty_rows.first().copied() {
                remaining.dirty_rows.remove(0);
                let key = if remaining.dirty_keys.is_empty() {
                    None
                } else {
                    remaining.dirty_keys.remove(0)
                };

                // A reload may have moved the row since the batch was
                // requested: the row is found again by its key.
                let row_idx = match key {
                    Some(key) => match self.row_index_of_key(&key, cx) {
                        Some(row_idx) => row_idx,
                        None => {
                            self.abandon_close_apply(cx);
                            report_error(
                                UserFacingError::new(
                                    ErrorKind::User,
                                    dbflux_i18n::t!(
                                        "document.data.mutation.error.staged_row_left_grid"
                                    ),
                                ),
                                cx,
                            );
                            return;
                        }
                    },
                    None => queued_idx,
                };

                if !self.queued_row_has_changes(row_idx, cx) {
                    // The row's edits were cleared before the pump reached it.
                    // It has nothing to write, so it must not park the batch.
                    continue;
                }
                if !remaining.dirty_rows.is_empty() {
                    self.pending_batch_remaining = Some(remaining);
                }
                self.batch_op_in_flight = true;
                self.handle_save_row(row_idx, true, cx);
                return;
            }

            self.finish_close_after_apply(cx);

            self.queue_reload_after_mutation(cx);
            cx.notify();
            return;
        }
    }

    /// Whether a queued insert still carries data to write.
    fn queued_insert_is_pending(&self, insert_idx: usize, cx: &App) -> bool {
        self.grid_table
            .table_state
            .as_ref()
            .is_some_and(|table_state| {
                table_state
                    .read(cx)
                    .edit_buffer()
                    .get_pending_insert_by_idx(insert_idx)
                    .is_some()
            })
    }

    /// Whether a queued row still holds unsaved edits to write.
    fn queued_row_has_changes(&self, row_idx: usize, cx: &App) -> bool {
        self.grid_table
            .table_state
            .as_ref()
            .is_some_and(|table_state| {
                let state = table_state.read(cx);
                state.is_editable() && !state.edit_buffer().row_changes(row_idx).is_empty()
            })
    }

    /// Keeps the queued insert indices valid after one insert leaves the edit
    /// buffer.
    ///
    /// `remove_pending_insert_by_idx` shifts every later insert down one, so the
    /// indices the batch still holds would otherwise point at the wrong insert
    /// or past the end.
    pub(super) fn renumber_queued_inserts_after_removal(
        &mut self,
        removed_idx: usize,
        cx: &mut Context<Self>,
    ) {
        self.renumber_held_insert_after_removal(removed_idx, cx);

        for remaining in self
            .pending_batch_remaining
            .iter_mut()
            .chain(self.edit_guard.parked_batch.iter_mut())
        {
            // The removed insert may still be queued (the user deleted a row the
            // batch had not started). Drop it, then shift the ones above it.
            remaining
                .pending_inserts
                .retain(|queued| *queued != removed_idx);

            for queued in &mut remaining.pending_inserts {
                if *queued > removed_idx {
                    *queued -= 1;
                }
            }
        }
    }

    /// Reports a close that was waiting on the staged edits, once the batch has
    /// nothing left to run.
    ///
    /// Called from every place a successful operation can find the batch empty,
    /// because the last operation of a batch is not reached through the drain: a
    /// batch of one parks no remaining work at all, so the save's own tail is what
    /// has to report it. A no-op when no close is waiting.
    fn finish_close_after_apply(&mut self, cx: &mut Context<Self>) {
        // Only reached with the intent still armed when no operation abandoned it,
        // so nothing failed by the time the batch ran out of work.
        if std::mem::take(&mut self.close_after_apply) {
            // A batch that ran out of work with edits still staged skipped some
            // (a reload moved them past the positions it was given): the tab
            // stays open over them rather than closing on unsaved edits.
            if self.has_pending_edits(cx) {
                cx.emit(DataGridEvent::MutationFinished { landed: false });
                return;
            }
            cx.emit(DataGridEvent::MutationFinished { landed: true });
            cx.emit(DataGridEvent::RequestClose);
        }
    }

    /// The key of every row in `row_indices`, read now. `None` when nothing
    /// can be deleted; that is already reported and the batch released.
    /// The count is of the rows left out for want of a readable key.
    pub(super) fn capture_delete_identities(
        &mut self,
        row_indices: &[usize],
        cx: &mut Context<Self>,
    ) -> Option<(Vec<(usize, RowIdentity)>, usize)> {
        let table_state = self.grid_table.table_state.as_ref()?;

        let count = row_indices.len();
        let pk_indices = {
            let state = table_state.read(cx);
            state.pk_columns()
        };

        if pk_indices.is_empty() {
            self.abandon_close_apply(cx);
            report_error(
                UserFacingError::new(
                    ErrorKind::Driver,
                    dbflux_i18n::t!("document.data.mutation.error.delete_no_primary_key"),
                ),
                cx,
            );
            return None;
        }

        // Collect PK identity for every row upfront before any async work
        let mut identities = Vec::with_capacity(count);
        let mut unidentified = 0;
        {
            let state = table_state.read(cx);
            let model = state.model();

            for &row_idx in row_indices {
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
                    log::warn!(
                        "[BULK DELETE] Failed to build row identity for row {}",
                        row_idx
                    );
                    unidentified += 1;
                    continue;
                }

                identities.push((row_idx, RowIdentity::new(pk_columns, pk_values)));
            }
        }

        if identities.is_empty() {
            self.abandon_close_apply(cx);
            report_error(
                UserFacingError::new(
                    ErrorKind::Driver,
                    dbflux_i18n::t!("document.data.mutation.error.bulk_delete_no_rows_identified"),
                ),
                cx,
            );
            return None;
        }

        Some((identities, unidentified))
    }

    /// Resolves each captured key to the row that holds it now, so the marks
    /// the delete clears are the right ones after a reload moved the rows.
    /// A row that left the grid is not deleted.
    fn run_captured_deletes(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        table_ref: TableRef,
        identities: Vec<(usize, RowIdentity)>,
        unidentified: usize,
        cx: &mut Context<Self>,
    ) {
        let captured = identities.len();
        let identities: Vec<(usize, RowIdentity)> = identities
            .into_iter()
            .filter_map(|(_, identity)| {
                let row_idx = self.row_index_of_key(identity.values(), cx)?;
                Some((row_idx, identity))
            })
            .collect();
        // A row a reload moved off the page is reported with the rows that
        // had no readable key, not dropped.
        let unidentified = unidentified + (captured - identities.len());

        if identities.is_empty() {
            self.abandon_close_apply(cx);
            report_error(
                UserFacingError::new(
                    ErrorKind::Driver,
                    dbflux_i18n::t!("document.data.mutation.error.bulk_delete_no_rows_identified"),
                ),
                cx,
            );
            return;
        }

        self.start_captured_deletes(
            profile_id,
            database,
            table_ref,
            identities,
            unidentified,
            cx,
        );
    }

    pub(super) fn start_captured_deletes(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        table_ref: TableRef,
        identities: Vec<(usize, RowIdentity)>,
        unidentified: usize,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = self.grid_table.table_state.clone() else {
            return;
        };

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::Query,
            crate::labels::mutation_delete_task_label(
                crate::labels::MutationItemKind::Row,
                identities.len(),
            ),
            cx,
        );

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();
        let table_state_clone = table_state.clone();
        let table_name = table_ref.name.clone();
        let schema_name = table_ref.schema.clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx.update(|cx| {
                app_state
                    .read(cx)
                    .connections()
                    .get(&profile_id)
                    .and_then(|connected| {
                        connected
                            .resolve_connection_for_execution(database.as_deref())
                            .ok()
                    })
            });

            let Some(conn) = conn else {
                log::error!("[BULK DELETE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                        panel.abandon_close_apply(cx);
                    });
                });
                return;
            };

            let mut successful_results = Vec::new();
            let mut last_error: Option<dbflux_core::DbError> = None;

            for (_row_idx, identity) in &identities {
                let delete =
                    RowDelete::new(identity.clone(), table_name.clone(), schema_name.clone());

                let conn = conn.clone();
                let result = cx
                    .background_executor()
                    .spawn(async move { conn.delete_row(&delete) })
                    .await;

                match result {
                    Ok(crud_result) => {
                        successful_results.push(crud_result);
                    }
                    Err(e) => {
                        last_error = Some(e);
                        // Stop on first error — remaining rows may depend on consistency
                        break;
                    }
                }
            }

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    crate::result_warnings::handoff_bulk_crud_returning_results(
                        &mut successful_results,
                        |warning| report_error(warning, cx),
                    );

                    let outcome = BulkDeleteOutcome {
                        kind: crate::labels::MutationItemKind::Row,
                        task_id,
                        attempted_rows: identities.iter().map(|(row_idx, _)| *row_idx).collect(),
                        deleted: successful_results.len(),
                        error: last_error,
                        unidentified,
                    };
                    panel.finish_bulk_delete(&table_state_clone, outcome, cx);
                });
            });
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
        let Some(table_state) = &self.grid_table.table_state else {
            return;
        };

        let count = row_indices.len();
        let pk_columns = extract_pk_columns(&self.result);

        // Collect filters for every document upfront
        let mut filters = Vec::with_capacity(count);
        let mut unidentified = 0;
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
                            log::warn!(
                                "[BULK DELETE] Invalid _id value for document at row {}",
                                row_idx
                            );
                            unidentified += 1;
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
            self.abandon_close_apply(cx);
            report_error(
                UserFacingError::new(
                    ErrorKind::Driver,
                    dbflux_i18n::t!(
                        "document.data.mutation.error.bulk_delete_no_documents_identified"
                    ),
                ),
                cx,
            );
            return;
        }

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::Query,
            crate::labels::mutation_delete_task_label(
                crate::labels::MutationItemKind::Document,
                filters.len(),
            ),
            cx,
        );

        let app_state = self.app_state.clone();
        let entity = cx.entity().clone();
        let table_state_clone = table_state.clone();
        let collection_name = collection.name.clone();
        let collection_db = collection.database.clone();

        cx.spawn(async move |_this, cx| {
            let conn = cx.update(|cx| {
                app_state
                    .read(cx)
                    .connections()
                    .get(&profile_id)
                    .map(|c| c.connection.clone())
            });

            let Some(conn) = conn else {
                log::error!("[BULK DELETE] No connection for profile {}", profile_id);
                cx.update(|cx| {
                    entity.update(cx, |panel, cx| {
                        panel.runner.fail_mutation(task_id, "No connection", cx);
                        panel.abandon_close_apply(cx);
                    });
                });
                return;
            };

            let mut successful_results = Vec::new();
            let mut last_error: Option<dbflux_core::DbError> = None;

            for (_row_idx, filter) in &filters {
                let delete =
                    dbflux_core::DocumentDelete::new(collection_name.clone(), filter.clone())
                        .with_database(collection_db.clone());

                let conn = conn.clone();
                let result = cx
                    .background_executor()
                    .spawn(async move { conn.delete_document(&delete) })
                    .await;

                match result {
                    Ok(crud_result) => {
                        successful_results.push(crud_result);
                    }
                    Err(e) => {
                        last_error = Some(e);
                        break;
                    }
                }
            }

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    crate::result_warnings::handoff_bulk_crud_returning_results(
                        &mut successful_results,
                        |warning| report_error(warning, cx),
                    );

                    let outcome = BulkDeleteOutcome {
                        kind: crate::labels::MutationItemKind::Document,
                        task_id,
                        attempted_rows: filters.iter().map(|(row_idx, _)| *row_idx).collect(),
                        deleted: successful_results.len(),
                        error: last_error,
                        unidentified,
                    };
                    panel.finish_bulk_delete(&table_state_clone, outcome, cx);
                });
            });
        })
        .detach();
    }
}

impl DataGridPanel {
    // === Panel Events ===

    pub fn request_hide(&mut self, cx: &mut Context<Self>) {
        cx.emit(DataGridEvent::RequestHide);
    }

    pub fn request_toggle_maximize(&mut self, cx: &mut Context<Self>) {
        cx.emit(DataGridEvent::RequestToggleMaximize);
    }

    /// Starts applying the staged edits because a close is waiting on them.
    ///
    /// Returns `true` when an apply is in flight, which is also when the caller
    /// must leave the tab open: the grid asks for the close itself once the edits
    /// land (`DataGridEvent::RequestClose`). `false` means there was nothing to
    /// apply, so the caller may close the tab now.
    pub fn apply_for_close(&mut self, cx: &mut Context<Self>) -> bool {
        if self.pending_edit_counts(cx) == (0, 0, 0) {
            return false;
        }

        // Armed before the apply starts: a source that answers synchronously can
        // drain the whole batch inside `request_save_all`.
        self.close_after_apply = true;

        // A live delete confirmation resumes the batch it was held for, and a
        // batch already in flight is the same apply. Re-requesting either would
        // duplicate the rows they already carry.
        if self.batch_op_in_flight {
            // A write is already running; it chains the rows it still holds.
            return true;
        }
        if self.pending_delete_confirm.is_some() || self.pending_expression_confirm.is_some() {
            return true;
        }
        if self.pending_batch_remaining.is_some() {
            // The pump holds unstarted rows for the write in flight; re-entering
            // it here would run two writes at once.
            return true;
        }

        let Some(table_state) = self.grid_table.table_state.clone() else {
            self.close_after_apply = false;
            return false;
        };

        table_state.update(cx, |state, cx| state.request_save_all(cx));
        true
    }

    /// Shows the result of a bulk delete in the grid and reports it.
    ///
    /// Deletes run one by one and stop at the first error, so a failed run can
    /// already have deleted rows. Those rows lose their mark, so saving again
    /// does not retry a row that no longer exists, and a reload drops them from
    /// the grid while carrying the marks of the rows that were not deleted over
    /// by identity.
    fn finish_bulk_delete(
        &mut self,
        table_state: &Entity<DataTableState>,
        outcome: BulkDeleteOutcome,
        cx: &mut Context<Self>,
    ) {
        let BulkDeleteOutcome {
            kind,
            task_id,
            attempted_rows,
            deleted,
            error,
            unidentified,
        } = outcome;

        table_state.update(cx, |state, cx| {
            for row_idx in attempted_rows.iter().take(deleted) {
                state.edit_buffer_mut().unmark_delete(*row_idx);
            }
            cx.notify();
        });

        if let Some(error) = error {
            self.runner.fail_mutation(task_id, error.to_string(), cx);
            self.abandon_close_apply(cx);
            report_error(
                UserFacingError::new(
                    ErrorKind::Driver,
                    crate::labels::partial_delete_label(
                        kind,
                        deleted,
                        attempted_rows.len(),
                        &error.to_string(),
                    ),
                ),
                cx,
            );
            if unidentified > 0 {
                report_unidentified_deletes(kind, deleted, unidentified, cx);
            }
            if deleted > 0 {
                self.queue_reload_after_mutation(cx);
            }
            cx.notify();
            return;
        }

        self.runner.complete_mutation(task_id, cx);

        if unidentified > 0 {
            // The rows left out are still staged, so a close waiting on this
            // apply must keep the tab instead of closing over them.
            self.abandon_close_apply_independent(cx);
            report_unidentified_deletes(kind, deleted, unidentified, cx);
        } else {
            self.pending.toast = Some(PendingToast {
                message: crate::labels::bulk_delete_success_label(kind, deleted),
                is_error: false,
            });
        }

        self.batch_op_in_flight = false;
        if self.pending_batch_remaining.is_some() {
            self.process_next_batch_op(cx);
        } else {
            // A batch of deletes parks on the confirmation instead of staging
            // remaining work, so the pump never runs and this tail is what
            // reports the batch.
            self.finish_close_after_apply(cx);
            self.queue_reload_after_mutation(cx);
        }
        cx.notify();
    }

    /// Gives up on the apply a close is waiting on, because one of its operations
    /// failed.
    ///
    /// The batch never chains past a failure, so waiting for it to drain would
    /// leave the intent armed over a batch that can never finish — and the next
    /// unrelated row save would drain that batch and close a tab nobody asked to
    /// close. Reporting the run as not landed here is what keeps the tab open with
    /// the rows that did not land.
    pub(super) fn abandon_close_apply(&mut self, cx: &mut Context<Self>) {
        self.batch_op_in_flight = false;
        // The batch never chains past a failure, so the rows it still held are
        // gone with it. Leaving them queued would make the next close mistake
        // them for a write still in flight and park instead of applying.
        self.pending_batch_remaining = None;

        if std::mem::take(&mut self.close_after_apply) {
            cx.emit(DataGridEvent::MutationFinished { landed: false });
        }
    }

    /// Reports a failed mutation that is not part of the close-apply batch.
    ///
    /// It may still be the failure the user is looking at, so a waiting close is
    /// told the run did not land. It must not clear the batch state, which
    /// belongs to the batch operation still in flight: doing so would let the
    /// next close start a second write beside it.
    pub(super) fn abandon_close_apply_independent(&mut self, cx: &mut Context<Self>) {
        if std::mem::take(&mut self.close_after_apply) {
            cx.emit(DataGridEvent::MutationFinished { landed: false });
        }
    }
}

/// What a bulk delete did, as the grid needs it to show the result.
struct BulkDeleteOutcome {
    kind: crate::labels::MutationItemKind,
    task_id: Uuid,
    /// Rows the delete ran against, in the order it ran them.
    attempted_rows: Vec<usize>,
    /// How many of `attempted_rows`, from the first, were deleted.
    deleted: usize,
    /// The error that stopped the run.
    error: Option<dbflux_core::DbError>,
    /// Marked rows left out because their identity could not be read.
    unidentified: usize,
}

/// Tells the user that marked rows were left out of a delete, as a warning:
/// the rest of the delete may have landed.
type SentCell = dbflux_components::components::data_table::model::CellValue;

fn same_staged_value(left: &SentCell, right: &SentCell) -> bool {
    left.to_value() == right.to_value() && left.as_expression() == right.as_expression()
}

/// Drops the row's staged cells that equal what a save sent and keeps the rest,
/// which the user staged after the patch was built.
/// A row save to send: the patch, and the staged cells it carries, so only
/// those are cleared once it lands.
pub(super) struct RowSave {
    pub(super) patch: RowPatch,
    pub(super) sent: Vec<(usize, SentCell)>,
}

fn clear_sent_cells(
    buffer: &mut dbflux_components::components::data_table::model::EditBuffer,
    row_idx: usize,
    sent: &[(usize, SentCell)],
) {
    let newer: Vec<(usize, SentCell)> = buffer
        .row_changes(row_idx)
        .into_iter()
        .filter(|(col, staged)| {
            !sent
                .iter()
                .any(|(sent_col, cell)| sent_col == col && same_staged_value(cell, staged))
        })
        .map(|(col, staged)| (col, staged.clone()))
        .collect();

    buffer.clear_row(row_idx);
    for (col, cell) in newer {
        buffer.set_cell(row_idx, col, cell);
    }
}

fn report_unidentified_deletes(
    kind: crate::labels::MutationItemKind,
    deleted: usize,
    unidentified: usize,
    cx: &mut App,
) {
    report_error(
        UserFacingError::new(
            ErrorKind::Driver,
            crate::labels::bulk_delete_unidentified_label(kind, deleted, unidentified),
        )
        .with_severity(dbflux_core::observability::EventSeverity::Warn),
        cx,
    );
}

#[cfg(test)]
mod tests {
    /// M5-T1: Stubs documenting the mutation error paths that are now routed
    /// through `report_error`. Full integration assertions require a GPUI test
    /// harness with a live `AppStateGlobal` and `ToastGlobal` — deferred until
    /// the test-support crate gains that infrastructure.

    #[test]
    #[ignore = "requires GPUI harness with AppStateGlobal and ToastGlobal"]
    fn insert_failure_routed_through_report_error() {
        // When an INSERT fails the error path calls report_error(ErrorKind::Driver, …)
        // which increments AppStateEntity::unread_error_count and emits a toast.
        // Verified manually: trigger a duplicate-key INSERT and confirm toast + audit row.
    }

    #[test]
    #[ignore = "requires GPUI harness with AppStateGlobal and ToastGlobal"]
    fn update_failure_routed_through_report_error() {
        // When a row UPDATE (save) fails the error path calls report_error.
        // Verified manually: edit a read-only column and confirm toast + audit row.
    }

    #[test]
    #[ignore = "requires GPUI harness with AppStateGlobal and ToastGlobal"]
    fn delete_failure_routed_through_report_error() {
        // When a DELETE fails the error path calls report_error.
        // Verified manually: delete a row with a FK violation and confirm toast + audit row.
    }
}
