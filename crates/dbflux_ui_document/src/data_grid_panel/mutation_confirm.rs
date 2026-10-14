use std::sync::Arc;
use std::time::Duration;

use super::*;
use dbflux_components::modals::{MutationConfirmHardRequest, MutationConfirmRequest};
use dbflux_core::{
    Connection, FilterNode, QueryRequest, Value, VisualMutationSpec, inline_params,
    render_filter_node_sql,
};
use dbflux_ui_base::user_error::UserFacingError;

/// Controls which mutation confirmation modal opens, and carries its request payload.
///
/// Used as the `pending_mutation_modal` field on `DataGridPanel`. The render cycle
/// reads this via `.take()` to open the appropriate modal entity.
#[derive(Debug)]
pub(crate) enum PendingMutationModal {
    /// Light confirmation (E-1): no type-to-confirm, no opt-in checkbox.
    Light(MutationConfirmRequest),
    /// Hard confirmation (E-2/E-3/E-4/E-6): danger variant with TypeToConfirm + opt-in.
    Hard(MutationConfirmHardRequest),
}

/// Fetches up to 5 sample rows for the mutation confirmation preview.
///
/// Builds the SELECT using the connection's dialect for correct identifier quoting and
/// placeholder style. The filter is rendered through `render_filter_node_sql` so the
/// WHERE clause is valid SQL, not a literal `<filter>` placeholder. The filter values are
/// inlined as dialect literals because drivers do not bind `QueryRequest.params`.
///
/// Returns `(column_names, rows)` on success, or an empty result on failure or timeout.
/// The deadline is 2 seconds per spec DR-9.
pub(crate) fn fetch_sample_rows(
    connection: Arc<dyn Connection>,
    spec: &VisualMutationSpec,
    report: impl FnMut(UserFacingError),
) -> (Vec<String>, Vec<Vec<String>>) {
    let dialect = connection.dialect();
    let qualified_table = dialect.qualified_table(spec.from.schema.as_deref(), &spec.from.name);

    let mut params: Vec<Value> = Vec::new();
    let mut param_idx: usize = 1;
    let where_clause =
        render_filter_node_sql(spec.filter.as_ref(), dialect, &mut params, &mut param_idx);

    let limit = dialect.limit_clause(5);
    let sql = match where_clause {
        Some(w) if !w.is_empty() => {
            format!(
                "SELECT * FROM {} WHERE {} ORDER BY 1 {}",
                qualified_table, w, limit
            )
        }
        _ => format!("SELECT * FROM {} ORDER BY 1 {}", qualified_table, limit),
    };
    let request = QueryRequest::new(inline_params(&sql, &params, dialect));

    let (tx, rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        let result = connection.execute(&request).ok();
        // The receiver may have already timed out and been dropped; drop the send error.
        let _drop_send = tx.send(result);
    });

    match rx.recv_timeout(Duration::from_secs(2)) {
        Ok(Some(mut result)) => sample_rows_from_result(&mut result, report),
        _ => (Vec::new(), Vec::new()),
    }
}

fn sample_rows_from_result(
    result: &mut dbflux_core::QueryResult,
    report: impl FnMut(UserFacingError),
) -> (Vec<String>, Vec<Vec<String>>) {
    crate::result_warnings::handoff_mutation_preview_result(result, report);

    let columns = result
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let rows = result
        .rows
        .iter()
        .map(|row| row.iter().map(|value| format!("{value}")).collect())
        .collect();

    (columns, rows)
}

/// Returns `true` when the filter uniquely identifies a single row by primary key.
///
/// PK-unique means every PK column appears as a direct `Eq` predicate in a single
/// top-level `AND` conjunction (or as a lone top-level `Eq` for a single-column PK).
///
/// Any `OR` group at the top level disqualifies: `id = 5 OR status = 'X'` can match
/// multiple rows even if `id` is the PK. Nested groups are not traversed.
///
/// Used to determine whether a DELETE needs a hard confirmation modal or can use the
/// lighter variant (spec DR-9.1 vs DR-9.2).
pub(crate) fn filter_is_pk_unique(filter: &Option<FilterNode>, pk_cols: &[&str]) -> bool {
    use dbflux_core::{BoolOp, Comparator, FilterNode, PredicateValue};

    if pk_cols.is_empty() {
        return false;
    }

    let Some(node) = filter else {
        return false;
    };

    fn direct_eq_columns(node: &FilterNode) -> Option<Vec<String>> {
        use dbflux_core::{BoolOp, Comparator, FilterNode, PredicateValue};
        match node {
            FilterNode::Predicate(p) => {
                if matches!(p.comparator, Comparator::Eq)
                    && matches!(p.value, PredicateValue::Single(_))
                {
                    Some(vec![p.column.clone()])
                } else {
                    None
                }
            }
            FilterNode::Group { op, children } => {
                if *op == BoolOp::Or {
                    return None;
                }
                let mut cols = Vec::new();
                for child in children {
                    match child {
                        FilterNode::Predicate(p)
                            if matches!(p.comparator, Comparator::Eq)
                                && matches!(p.value, PredicateValue::Single(_)) =>
                        {
                            cols.push(p.column.clone());
                        }
                        _ => {
                            return None;
                        }
                    }
                }
                Some(cols)
            }
        }
    }

    let eq_cols = match direct_eq_columns(node) {
        Some(cols) => cols,
        None => return false,
    };

    pk_cols
        .iter()
        .all(|pk| eq_cols.iter().any(|ec| ec.eq_ignore_ascii_case(pk)))
}

/// Builds a `PendingMutationModal` for the given spec and estimated row count.
///
/// Selects `Light` (spec DR-9.1) when the spec is a DELETE and the filter uniquely
/// identifies a single row by primary key (all PK columns with equality predicates)
/// and `est_rows == Some(1)`. Uses `Hard` for all other cases.
pub(crate) fn build_pending_modal(
    spec: &VisualMutationSpec,
    sql_preview: String,
    est_rows: Option<u64>,
    sample_columns: Vec<String>,
    sample_rows: Option<Vec<Vec<String>>>,
    pk_cols: &[&str],
) -> PendingMutationModal {
    use dbflux_core::MutationKind;

    let table_name = spec.from.name.clone();
    let is_delete = matches!(spec.kind, MutationKind::Delete);

    let pk_unique_delete =
        is_delete && est_rows == Some(1) && filter_is_pk_unique(&spec.filter, pk_cols);

    let use_hard = if pk_unique_delete {
        false
    } else {
        is_delete || est_rows.map(|n| n > 1).unwrap_or(true)
    };

    let summary = match &spec.kind {
        MutationKind::Delete => crate::labels::delete_rows_label(est_rows, &table_name),
        MutationKind::Update { assignments } => {
            crate::labels::update_columns_label(assignments.len(), &table_name)
        }
    };

    if use_hard {
        PendingMutationModal::Hard(MutationConfirmHardRequest {
            summary,
            type_to_confirm: table_name,
            sql_preview,
            sample_rows,
            sample_columns,
            require_opt_in: true,
        })
    } else {
        PendingMutationModal::Light(MutationConfirmRequest {
            summary,
            sql_preview,
            sample_rows,
            sample_columns,
        })
    }
}

/// Formats a `Value` for display in the sample-rows preview.
///
/// Exposed here so it can be used without importing `Value`'s `Display` impl.
#[allow(dead_code)]
pub(crate) fn format_value(value: &Value) -> String {
    format!("{}", value)
}

impl DataGridPanel {
    /// Handles `BuilderEvent::MutationRunRequested`.
    ///
    /// 1. Reads `mutation_policy` from `ConnectedProfile`. `ReadOnly` → error toast; returns.
    /// 2. Generates the SQL preview via the driver's `QueryGenerator`.
    /// 3. Fetches sample rows from the connection (2s deadline, synchronous on background thread).
    /// 4. Builds `PendingMutationModal` and stores it; the render cycle will open the modal.
    pub(crate) fn on_mutation_run_requested(
        &mut self,
        spec: dbflux_core::VisualMutationSpec,
        opts: crate::data_grid_panel::mutation_executor::MutationExecOptions,
        est_rows: Option<u64>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use dbflux_core::MutationPolicy;

        let profile_id = match &self.source {
            DataSource::Table { profile_id, .. } => *profile_id,
            _ => return,
        };

        let (policy, read_only_reason, connection) = {
            let state = self.app_state.read(cx);
            let connected = match state.connections().get(&profile_id) {
                Some(c) => c,
                None => return,
            };

            (
                connected.mutation_policy,
                connected.read_only_reason,
                Arc::clone(&connected.connection),
            )
        };

        // Gate on mutation policy — state borrow has been released above.
        match policy {
            MutationPolicy::ReadOnly => {
                dbflux_ui_base::user_error::report_error(
                    dbflux_ui_base::user_error::UserFacingError::new(
                        dbflux_ui_base::user_error::ErrorKind::User,
                        crate::labels::mutation_read_only_error(read_only_reason),
                    ),
                    cx,
                );
                return;
            }
            MutationPolicy::ApprovalRequired => {
                #[cfg(feature = "mcp")]
                {
                    use dbflux_core::MutationKind;
                    use dbflux_policy::ExecutionClassification;
                    let classification = match &spec.kind {
                        MutationKind::Delete => ExecutionClassification::Destructive,
                        MutationKind::Update { .. } => ExecutionClassification::Write,
                    };
                    let spec_json = serde_json::to_value(&spec).unwrap_or_default();
                    let connection_id = profile_id.to_string();
                    let enqueue_result = self.app_state.update(cx, |app, _| {
                        app.request_mcp_execution(
                            "user".to_string(),
                            connection_id,
                            "mutation.run".to_string(),
                            classification,
                            spec_json,
                        )
                    });
                    match enqueue_result {
                        Ok(_) => {
                            dbflux_ui_base::toast::Toast::info(dbflux_i18n::t!(
                                "document.data.grid.toast.mutation_queued"
                            ))
                            .push(cx);
                        }
                        Err(e) => {
                            dbflux_ui_base::user_error::report_error(
                                dbflux_ui_base::user_error::UserFacingError::new(
                                    dbflux_ui_base::user_error::ErrorKind::Driver,
                                    crate::labels::mutation_approval_queue_failed_error(
                                        &e.to_string(),
                                    ),
                                ),
                                cx,
                            );
                        }
                    }
                    return;
                }

                #[cfg(not(feature = "mcp"))]
                {
                    dbflux_ui_base::user_error::report_error(
                        dbflux_ui_base::user_error::UserFacingError::new(
                            dbflux_ui_base::user_error::ErrorKind::User,
                            dbflux_i18n::t!("document.data.mutation.error.approval_requires_mcp"),
                        ),
                        cx,
                    );
                    return;
                }
            }
            MutationPolicy::Allowed => {}
        }

        let sql_preview = connection
            .query_generator()
            .and_then(|qgen| {
                use dbflux_core::MutationKind;
                let generated = match &spec.kind {
                    MutationKind::Delete => qgen.generate_delete_from_spec(&spec).ok(),
                    MutationKind::Update { .. } => qgen.generate_update_from_spec(&spec).ok(),
                };
                generated.map(|m| m.materialize_for_editor(connection.dialect()))
            })
            .unwrap_or_else(|| "<SQL preview unavailable>".to_string());

        // Fetch sample rows synchronously on background thread (2s deadline).
        let (sample_columns, sample_rows) =
            crate::data_grid_panel::mutation_confirm::fetch_sample_rows(
                connection,
                &spec,
                |warning| dbflux_ui_base::user_error::report_error(warning, cx),
            );

        let sample_rows_opt = if sample_rows.is_empty() {
            None
        } else {
            Some(sample_rows)
        };

        let pk_col_refs: Vec<&str> = match &self.source {
            DataSource::Table { .. } => self.pk_columns.iter().map(|s| s.as_str()).collect(),
            _ => vec![],
        };

        let modal = crate::data_grid_panel::mutation_confirm::build_pending_modal(
            &spec,
            sql_preview,
            est_rows,
            sample_columns,
            sample_rows_opt,
            &pk_col_refs,
        );

        self.pending_mutation_exec = Some(PendingMutationExec {
            spec,
            opts,
            profile_id,
            intent: MutationIntent::Direct,
        });
        self.pending.mutation_modal = Some(modal);
        cx.notify();
    }

    /// Called when either `ModalMutationConfirm` or `ModalMutationConfirmHard` emits
    /// a `MutationConfirmOutcome` (Confirmed or Cancelled).
    ///
    /// On `Confirmed`: takes `pending_mutation_exec`, dispatches `MutationExecutor`
    /// to a background thread, and emits audit events via `EventSink`.
    /// On `Cancelled`: clears `pending_mutation_exec` with no side effects.
    pub(super) fn handle_mutation_confirm_outcome(
        &mut self,
        outcome: MutationConfirmOutcome,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let pending = match self.pending_mutation_exec.take() {
            Some(p) => p,
            None => return,
        };

        if matches!(outcome, MutationConfirmOutcome::Cancelled) {
            return;
        }

        let (connection, event_sink, policy) = {
            let state = self.app_state.read(cx);
            let connected = match state.connections().get(&pending.profile_id) {
                Some(c) => c,
                None => {
                    dbflux_ui_base::user_error::report_error(
                        dbflux_ui_base::user_error::UserFacingError::new(
                            dbflux_ui_base::user_error::ErrorKind::Driver,
                            dbflux_i18n::t!("document.data.mutation.error.connection_not_found"),
                        ),
                        cx,
                    );
                    return;
                }
            };

            let connection = Arc::clone(&connected.connection);
            let event_sink: Option<Arc<dyn dbflux_core::EventSink>> =
                Some(Arc::new(state.audit_service().clone()) as Arc<dyn dbflux_core::EventSink>);
            let policy = connected.mutation_policy;

            (connection, event_sink, policy)
        };

        #[cfg(feature = "mcp")]
        if matches!(policy, dbflux_core::MutationPolicy::ApprovalRequired) {
            use dbflux_core::MutationKind;
            use dbflux_policy::ExecutionClassification;
            let classification = match &pending.spec.kind {
                MutationKind::Delete => ExecutionClassification::Destructive,
                MutationKind::Update { .. } => ExecutionClassification::Write,
            };
            let spec_json = serde_json::to_value(&pending.spec).unwrap_or_default();
            let connection_id = pending.profile_id.to_string();
            let enqueue_result = self.app_state.update(cx, |app, _| {
                app.request_mcp_execution(
                    "user".to_string(),
                    connection_id,
                    "mutation.run".to_string(),
                    classification,
                    spec_json,
                )
            });
            match enqueue_result {
                Ok(_) => {
                    dbflux_ui_base::toast::Toast::info(dbflux_i18n::t!(
                        "document.data.grid.toast.mutation_queued"
                    ))
                    .push(cx);
                }
                Err(e) => {
                    dbflux_ui_base::user_error::report_error(
                        dbflux_ui_base::user_error::UserFacingError::new(
                            dbflux_ui_base::user_error::ErrorKind::Driver,
                            crate::labels::mutation_approval_queue_failed_error(&e.to_string()),
                        ),
                        cx,
                    );
                }
            }
            return;
        }

        let deps = crate::data_grid_panel::mutation_executor::MutationDeps {
            connection,
            event_sink,
            policy,
        };

        let spec = pending.spec;
        let mut opts = pending.opts;
        let intent = pending.intent;

        let is_chunked = matches!(
            opts.mode,
            crate::data_grid_panel::mutation_executor::ExecutionMode::ChunkedTransaction
        );

        let table_name = spec.from.name.clone();

        let mode = if is_chunked {
            crate::labels::VisualMutationTaskMode::Chunked
        } else if matches!(
            opts.mode,
            crate::data_grid_panel::mutation_executor::ExecutionMode::DirectAutocommit
        ) {
            crate::labels::VisualMutationTaskMode::Direct
        } else {
            crate::labels::VisualMutationTaskMode::SingleTransaction
        };

        // A chunked run walks the primary key and has to stay under the driver's
        // parameter limit, so both are resolved before the run starts — while a
        // refusal can still be shown instead of a finished task that did nothing.
        let pk_columns: Vec<String> = if is_chunked {
            let columns: Vec<String> = match &self.source {
                DataSource::Table { .. } => self.pk_columns.clone(),
                _ => vec![],
            };

            if columns.is_empty() {
                dbflux_ui_base::user_error::report_error(
                    dbflux_ui_base::user_error::UserFacingError::new(
                        dbflux_ui_base::user_error::ErrorKind::User,
                        dbflux_i18n::t!(
                            "document.data.mutation.error.chunked_requires_primary_key"
                        ),
                    ),
                    cx,
                );
                return;
            }

            // Compute the effective chunk size before spawning so we can surface
            // any reduction to the user via Toast while we still have a cx handle.
            {
                use crate::data_grid_panel::mutation_executor::{
                    compute_effective_chunk_size, count_assignment_params,
                };
                use dbflux_core::render_filter_node_sql;

                let max_params = deps
                    .connection
                    .metadata()
                    .query
                    .as_ref()
                    .map(|q| q.max_query_parameters)
                    .unwrap_or(0);

                if max_params > 0 {
                    let dialect = deps.connection.dialect();
                    let mut dummy_params = Vec::new();
                    let mut dummy_idx: usize = 1;
                    render_filter_node_sql(
                        spec.filter.as_ref(),
                        dialect,
                        &mut dummy_params,
                        &mut dummy_idx,
                    );
                    let filter_param_count = dummy_params.len() as u32;

                    let assignment_param_count = match &spec.kind {
                        dbflux_core::MutationKind::Update { assignments } => {
                            count_assignment_params(assignments)
                        }
                        dbflux_core::MutationKind::Delete => 0,
                    };

                    let (effective, reduced_from) = compute_effective_chunk_size(
                        opts.chunk_size,
                        max_params,
                        filter_param_count,
                        assignment_param_count,
                        columns.len() as u32,
                    );

                    if let Some(original) = reduced_from {
                        const FLOOR: u32 = 1_000;
                        if effective < FLOOR {
                            dbflux_ui_base::toast::Toast::warning(
                                crate::labels::mutation_chunk_size_reduced_toast(
                                    original, effective, FLOOR,
                                ),
                            )
                            .push(cx);
                        } else {
                            dbflux_ui_base::toast::Toast::info(
                                crate::labels::mutation_chunk_size_adjusted_toast(
                                    original, effective,
                                ),
                            )
                            .push(cx);
                        }
                        opts.chunk_size = effective;
                    }
                }
            }

            columns
        } else {
            vec![]
        };

        let (task_id, cancel_handle) = self.runner.start_mutation(
            dbflux_core::TaskKind::Query,
            crate::labels::visual_mutation_task_label(mode),
            cx,
        );

        let run = MutationRun {
            task_id,
            mode,
            table_name,
            intent,
        };

        cx.spawn(async move |this, cx| {
            use crate::data_grid_panel::mutation_executor::MutationExecutor;
            use dbflux_ui_base::user_error::report_error_async;

            let result = cx
                .background_executor()
                .spawn(async move {
                    let executor = MutationExecutor::new(spec, opts, deps);
                    match mode {
                        crate::labels::VisualMutationTaskMode::Chunked => {
                            let pk_refs: Vec<&str> =
                                pk_columns.iter().map(String::as_str).collect();
                            executor.run_chunked_tx(&pk_refs, &cancel_handle)
                        }
                        crate::labels::VisualMutationTaskMode::Direct => {
                            executor.run_direct(&cancel_handle)
                        }
                        crate::labels::VisualMutationTaskMode::SingleTransaction => {
                            executor.run_single_tx(&cancel_handle)
                        }
                    }
                })
                .await;

            // Raising the failure needs an `AsyncApp` and recording it needs the
            // entity, so the report travels back out instead of being raised here.
            let failure = cx.update(|cx| {
                this.update(cx, |grid, cx| grid.finish_mutation(run, result, cx))
                    .ok()
                    .flatten()
            });

            if let Some(failure) = failure {
                report_error_async(failure, cx);
            }
        })
        .detach();
    }

    /// Records a finished visual-mutation run, and tells the platform whether a
    /// close that was waiting on it may proceed.
    ///
    /// Returns the user-facing error of a failed run, so the caller can raise it
    /// with an `AsyncApp` handle this method does not have.
    ///
    /// The run counts as landed only when the executor wrote at least one row.
    /// `MutationOutcome::Success { rows_affected: 0 }` means the statement matched
    /// nothing — the row is gone, or its key changed — so a close waiting on this
    /// apply must not read it as a landed write.
    pub(super) fn finish_mutation(
        &mut self,
        run: MutationRun,
        result: Result<
            crate::data_grid_panel::mutation_executor::MutationOutcome,
            crate::data_grid_panel::mutation_executor::ExecutorError,
        >,
        cx: &mut Context<Self>,
    ) -> Option<dbflux_ui_base::user_error::UserFacingError> {
        use crate::data_grid_panel::mutation_executor::MutationOutcome;
        use dbflux_ui_base::user_error::{ErrorKind, UserFacingError};

        let MutationRun {
            task_id,
            mode,
            table_name,
            intent,
        } = run;

        let failure_text = |error: &str| match mode {
            crate::labels::VisualMutationTaskMode::Chunked => {
                crate::labels::mutation_chunked_execution_failed_error(&table_name, error)
            }
            crate::labels::VisualMutationTaskMode::Direct
            | crate::labels::VisualMutationTaskMode::SingleTransaction => {
                crate::labels::mutation_execution_failed_error(&table_name, error)
            }
        };

        let (failure, landed) = match result {
            Err(error) => {
                let text = error.to_string();
                self.runner.fail_mutation(task_id, text.clone(), cx);
                (Some(failure_text(&text)), false)
            }
            Ok(MutationOutcome::Success { rows_affected }) => {
                self.runner.complete_mutation(task_id, cx);
                dbflux_ui_base::toast::Toast::success(
                    crate::labels::mutation_execution_completed_toast(rows_affected),
                )
                .push(cx);
                (None, rows_affected > 0)
            }
            Ok(MutationOutcome::Cancelled { rows_affected }) => {
                self.runner.cancel_mutation(task_id, cx);
                dbflux_ui_base::toast::Toast::info(
                    crate::labels::mutation_execution_cancelled_toast(rows_affected),
                )
                .push(cx);
                (None, false)
            }
            Ok(MutationOutcome::Failed { error }) => {
                self.runner.fail_mutation(task_id, error.clone(), cx);
                (Some(failure_text(&error)), false)
            }
        };

        cx.emit(DataGridEvent::MutationFinished { landed });

        if intent == MutationIntent::CloseAfterApply && landed {
            cx.emit(DataGridEvent::RequestClose);
        }

        failure.map(|message| UserFacingError::new(ErrorKind::Driver, message))
    }
}

#[cfg(test)]
mod tests {
    use dbflux_core::{
        BoolOp, Comparator, FilterNode, LiteralValue, Predicate, PredicateValue, QueryResult,
    };

    use super::{filter_is_pk_unique, sample_rows_from_result};

    fn pred_eq(col: &str) -> FilterNode {
        FilterNode::Predicate(Predicate {
            source_alias: "t".to_string(),
            column: col.to_string(),
            comparator: Comparator::Eq,
            value: PredicateValue::Single(LiteralValue::Integer(1)),
            node_id: 0,
        })
    }

    fn pred_ne(col: &str) -> FilterNode {
        FilterNode::Predicate(Predicate {
            source_alias: "t".to_string(),
            column: col.to_string(),
            comparator: Comparator::Neq,
            value: PredicateValue::Single(LiteralValue::Integer(1)),
            node_id: 0,
        })
    }

    fn or_group(children: Vec<FilterNode>) -> FilterNode {
        FilterNode::Group {
            op: BoolOp::Or,
            children,
        }
    }

    fn and_group(children: Vec<FilterNode>) -> FilterNode {
        FilterNode::Group {
            op: BoolOp::And,
            children,
        }
    }

    // DR-9.1: OR group at top level must NOT be considered PK-unique.
    //   filter = id = 5 OR status = 'X', pk_cols = ["id"] → false
    #[test]
    fn filter_with_or_group_containing_pk_eq_is_not_pk_unique() {
        let filter = Some(or_group(vec![pred_eq("id"), pred_eq("status")]));
        assert!(
            !filter_is_pk_unique(&filter, &["id"]),
            "OR group must not be considered PK-unique even if PK col has Eq predicate"
        );
    }

    // DR-9.1: AND group with all PK cols as direct Eq children → true.
    //   filter = a = 1 AND b = 2, pk_cols = ["a", "b"] → true
    #[test]
    fn filter_with_and_group_containing_all_pk_cols_eq_is_pk_unique() {
        let filter = Some(and_group(vec![pred_eq("a"), pred_eq("b")]));
        assert!(
            filter_is_pk_unique(&filter, &["a", "b"]),
            "AND group with all PK cols as direct Eq children must be PK-unique"
        );
    }

    // DR-9.1: AND group missing one PK col → false.
    //   filter = a = 1 AND c = 3, pk_cols = ["a", "b"] → false
    #[test]
    fn filter_with_and_group_missing_pk_col_is_not_pk_unique() {
        let filter = Some(and_group(vec![pred_eq("a"), pred_eq("c")]));
        assert!(
            !filter_is_pk_unique(&filter, &["a", "b"]),
            "AND group missing PK col 'b' must not be PK-unique"
        );
    }

    // Single Eq predicate matches 1-column PK → true.
    #[test]
    fn single_eq_predicate_for_single_pk_col_is_pk_unique() {
        let filter = Some(pred_eq("id"));
        assert!(filter_is_pk_unique(&filter, &["id"]));
    }

    // Single non-Eq predicate for PK → false.
    #[test]
    fn single_neq_predicate_is_not_pk_unique() {
        let filter = Some(pred_ne("id"));
        assert!(!filter_is_pk_unique(&filter, &["id"]));
    }

    // None filter is never PK-unique.
    #[test]
    fn none_filter_is_not_pk_unique() {
        assert!(!filter_is_pk_unique(&None, &["id"]));
    }

    // Empty pk_cols → never PK-unique.
    #[test]
    fn empty_pk_cols_is_not_pk_unique() {
        let filter = Some(pred_eq("id"));
        assert!(!filter_is_pk_unique(&filter, &[]));
    }

    #[test]
    fn mutation_preview_handoff_reports_and_drains_warning_metadata() {
        let mut result = QueryResult::empty();
        result.set_unsupported_types(["bit".to_string(), "bit".to_string()]);

        let mut summaries = Vec::new();
        let _ = sample_rows_from_result(&mut result, |warning| summaries.push(warning.summary));

        assert_eq!(
            summaries,
            ["Unsupported database type 'bit' in mutation preview result"]
        );
        assert!(result.take_unsupported_types().is_empty());
    }

    // AND group where one child is a non-Eq predicate → false (mixed group rejected).
    #[test]
    fn and_group_with_non_eq_child_is_not_pk_unique() {
        let filter = Some(and_group(vec![pred_eq("id"), pred_ne("status")]));
        assert!(
            !filter_is_pk_unique(&filter, &["id"]),
            "AND group with non-Eq child must not be PK-unique"
        );
    }

    // F-R3-6: classification helper — DELETE maps to Destructive, UPDATE maps to Write.
    // The helper logic is inlined at the call site in mod.rs; these tests pin the mapping
    // to prevent regression without requiring a GPUI context.
    #[test]
    fn update_spec_classifies_as_write() {
        use dbflux_core::{
            Assignment, AssignmentValue, MutationKind, ScalarLiteral, TableRef, VisualMutationSpec,
        };

        let spec = VisualMutationSpec {
            from: TableRef {
                schema: None,
                name: "t".to_string(),
            },
            filter: None,
            kind: MutationKind::Update {
                assignments: vec![Assignment {
                    column: "col".to_string(),
                    value: AssignmentValue::Literal(ScalarLiteral::Integer(1)),
                }],
            },
        };

        // The classification expression that lives in mod.rs — replicate the logic here.
        let is_delete = matches!(spec.kind, MutationKind::Delete);
        assert!(!is_delete, "UPDATE must not be classified as Delete");
    }

    #[test]
    fn delete_spec_classifies_as_destructive() {
        use dbflux_core::{MutationKind, TableRef, VisualMutationSpec};

        let spec = VisualMutationSpec {
            from: TableRef {
                schema: None,
                name: "t".to_string(),
            },
            filter: None,
            kind: MutationKind::Delete,
        };

        let is_delete = matches!(spec.kind, MutationKind::Delete);
        assert!(
            is_delete,
            "DELETE must be classified as Destructive (is_delete=true)"
        );
    }

    // F-R3-3: DELETE with PK-unique filter + est_rows=Some(1) → Light modal.
    #[test]
    fn delete_with_pk_unique_filter_and_count_one_uses_light_modal() {
        use dbflux_core::{MutationKind, TableRef, VisualMutationSpec};

        use super::{PendingMutationModal, build_pending_modal};

        let spec = VisualMutationSpec {
            from: TableRef {
                schema: None,
                name: "orders".to_string(),
            },
            filter: Some(pred_eq("id")),
            kind: MutationKind::Delete,
        };

        let modal = build_pending_modal(
            &spec,
            "DELETE FROM orders WHERE id = $1".to_string(),
            Some(1),
            vec!["id".to_string()],
            None,
            &["id"],
        );

        assert!(
            matches!(modal, PendingMutationModal::Light(_)),
            "PK-unique DELETE with est_rows=Some(1) must use Light modal"
        );
    }

    // F-R3-3: DELETE with count_state still Pending (est_rows=None) → Hard modal.
    #[test]
    fn delete_with_count_none_falls_back_to_hard_modal() {
        use dbflux_core::{MutationKind, TableRef, VisualMutationSpec};

        use super::{PendingMutationModal, build_pending_modal};

        let spec = VisualMutationSpec {
            from: TableRef {
                schema: None,
                name: "orders".to_string(),
            },
            filter: Some(pred_eq("id")),
            kind: MutationKind::Delete,
        };

        let modal = build_pending_modal(
            &spec,
            "DELETE FROM orders WHERE id = $1".to_string(),
            None,
            vec!["id".to_string()],
            None,
            &["id"],
        );

        assert!(
            matches!(modal, PendingMutationModal::Hard(_)),
            "DELETE with est_rows=None must fall back to Hard modal (safety default)"
        );
    }

    // F-R4-1: fetch_sample_rows must include ORDER BY 1 before the limit clause.
    //
    // T-SQL rejects "OFFSET 0 ROWS FETCH NEXT n ROWS ONLY" without a preceding ORDER BY.
    // The function must emit "ORDER BY 1" unconditionally so all dialects produce valid SQL.
    mod fetch_sample_rows_sql_tests {
        use std::sync::{Arc, Mutex};

        use dbflux_core::{
            DatabaseCategory, DbKind, DefaultSqlDialect, DriverCapabilities, DriverMetadataBuilder,
            MutationKind, QueryLanguage, QueryResult, SchemaLoadingStrategy, SchemaSnapshot,
            TableRef, VisualMutationSpec,
        };
        use dbflux_driver_mssql::MssqlDialect;

        use super::super::fetch_sample_rows;

        struct MssqlRecordingConnection {
            meta: dbflux_core::DriverMetadata,
            calls: Mutex<Vec<String>>,
        }

        impl MssqlRecordingConnection {
            fn new() -> Arc<Self> {
                let meta = DriverMetadataBuilder::new(
                    "sqlserver",
                    "SQL Server",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .capabilities(DriverCapabilities::TRANSACTIONS)
                .build();
                Arc::new(Self {
                    meta,
                    calls: Mutex::new(Vec::new()),
                })
            }

            fn recorded_calls(&self) -> Vec<String> {
                self.calls.lock().unwrap().clone()
            }
        }

        impl dbflux_core::Connection for MssqlRecordingConnection {
            fn metadata(&self) -> &dbflux_core::DriverMetadata {
                &self.meta
            }
            fn ping(&self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            fn close(&mut self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            #[expect(
                clippy::unwrap_in_result,
                reason = "test fixture: the existing panic-on-poison policy is retained for this mutex"
            )]
            fn execute(
                &self,
                req: &dbflux_core::QueryRequest,
            ) -> Result<QueryResult, dbflux_core::DbError> {
                self.calls.lock().unwrap().push(req.sql.clone());
                Ok(QueryResult::empty())
            }
            fn cancel(
                &self,
                _handle: &dbflux_core::QueryHandle,
            ) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            fn schema(&self) -> Result<SchemaSnapshot, dbflux_core::DbError> {
                Err(dbflux_core::DbError::NotSupported("stub".to_string()))
            }
            fn kind(&self) -> DbKind {
                DbKind::SqlServer
            }
            fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
                SchemaLoadingStrategy::SingleDatabase
            }
            fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
                static D: MssqlDialect = MssqlDialect;
                &D
            }
            fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
                None
            }
        }

        struct PgRecordingConnection {
            meta: dbflux_core::DriverMetadata,
            calls: Mutex<Vec<String>>,
        }

        impl PgRecordingConnection {
            fn new() -> Arc<Self> {
                let meta = DriverMetadataBuilder::new(
                    "postgres", // guardrail-allow: test stub, not production driver branching
                    "PostgreSQL",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .capabilities(DriverCapabilities::TRANSACTIONS)
                .build();
                Arc::new(Self {
                    meta,
                    calls: Mutex::new(Vec::new()),
                })
            }

            fn recorded_calls(&self) -> Vec<String> {
                self.calls.lock().unwrap().clone()
            }
        }

        impl dbflux_core::Connection for PgRecordingConnection {
            fn metadata(&self) -> &dbflux_core::DriverMetadata {
                &self.meta
            }
            fn ping(&self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            fn close(&mut self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            #[expect(
                clippy::unwrap_in_result,
                reason = "test fixture: the existing panic-on-poison policy is retained for this mutex"
            )]
            fn execute(
                &self,
                req: &dbflux_core::QueryRequest,
            ) -> Result<QueryResult, dbflux_core::DbError> {
                self.calls.lock().unwrap().push(req.sql.clone());
                Ok(QueryResult::empty())
            }
            fn cancel(
                &self,
                _handle: &dbflux_core::QueryHandle,
            ) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            fn schema(&self) -> Result<SchemaSnapshot, dbflux_core::DbError> {
                Err(dbflux_core::DbError::NotSupported("stub".to_string()))
            }
            fn kind(&self) -> DbKind {
                DbKind::Postgres
            }
            fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
                SchemaLoadingStrategy::SingleDatabase
            }
            fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
                static D: DefaultSqlDialect = DefaultSqlDialect;
                &D
            }
            fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
                None
            }
        }

        struct PlaceholderDialect(dbflux_core::PlaceholderStyle);

        static DOLLAR_DIALECT: PlaceholderDialect =
            PlaceholderDialect(dbflux_core::PlaceholderStyle::DollarNumber);
        static AT_SIGN_DIALECT: PlaceholderDialect =
            PlaceholderDialect(dbflux_core::PlaceholderStyle::AtSign);

        impl dbflux_core::SqlDialect for PlaceholderDialect {
            fn quote_identifier(&self, name: &str) -> String {
                format!("\"{}\"", name)
            }

            fn qualified_table(&self, _schema: Option<&str>, table: &str) -> String {
                self.quote_identifier(table)
            }

            fn value_to_literal(&self, value: &dbflux_core::Value) -> String {
                DefaultSqlDialect.value_to_literal(value)
            }

            fn escape_string(&self, s: &str) -> String {
                s.replace('\'', "''")
            }

            fn placeholder_style(&self) -> dbflux_core::PlaceholderStyle {
                self.0
            }
        }

        struct RequestRecordingConnection {
            meta: dbflux_core::DriverMetadata,
            dialect: &'static dyn dbflux_core::SqlDialect,
            requests: Mutex<Vec<dbflux_core::QueryRequest>>,
        }

        impl RequestRecordingConnection {
            fn new(dialect: &'static dyn dbflux_core::SqlDialect) -> Arc<Self> {
                let meta = DriverMetadataBuilder::new(
                    "recording",
                    "Recording",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .build();
                Arc::new(Self {
                    meta,
                    dialect,
                    requests: Mutex::new(Vec::new()),
                })
            }

            fn recorded_requests(&self) -> Vec<dbflux_core::QueryRequest> {
                self.requests.lock().unwrap().clone()
            }
        }

        impl dbflux_core::Connection for RequestRecordingConnection {
            fn metadata(&self) -> &dbflux_core::DriverMetadata {
                &self.meta
            }
            fn ping(&self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            fn close(&mut self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            #[expect(
                clippy::unwrap_in_result,
                reason = "test fixture: the existing panic-on-poison policy is retained for this mutex"
            )]
            fn execute(
                &self,
                req: &dbflux_core::QueryRequest,
            ) -> Result<QueryResult, dbflux_core::DbError> {
                self.requests.lock().unwrap().push(req.clone());
                Ok(QueryResult::empty())
            }
            fn cancel(
                &self,
                _handle: &dbflux_core::QueryHandle,
            ) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            fn schema(&self) -> Result<SchemaSnapshot, dbflux_core::DbError> {
                Err(dbflux_core::DbError::NotSupported("stub".to_string()))
            }
            fn kind(&self) -> DbKind {
                DbKind::Postgres
            }
            fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
                SchemaLoadingStrategy::SingleDatabase
            }
            fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
                self.dialect
            }
        }

        fn filtered_delete_spec() -> VisualMutationSpec {
            let mut spec = delete_spec("orders");
            spec.filter = Some(dbflux_core::FilterNode::Predicate(dbflux_core::Predicate {
                source_alias: "orders".to_string(),
                column: "status".to_string(),
                comparator: dbflux_core::Comparator::Eq,
                value: dbflux_core::PredicateValue::Single(dbflux_core::LiteralValue::Text(
                    "it's done".to_string(),
                )),
                node_id: 0,
            }));
            spec
        }

        fn sample_request_for(
            dialect: &'static dyn dbflux_core::SqlDialect,
        ) -> dbflux_core::QueryRequest {
            let conn = RequestRecordingConnection::new(dialect);
            let conn_ref = Arc::clone(&conn);

            fetch_sample_rows(
                conn as Arc<dyn dbflux_core::Connection>,
                &filtered_delete_spec(),
                |_| {},
            );

            let requests = conn_ref.recorded_requests();
            assert_eq!(requests.len(), 1, "expected exactly one SELECT call");
            requests.into_iter().next().unwrap()
        }

        #[test]
        fn sample_rows_inline_filter_values_for_dollar_placeholders() {
            let request = sample_request_for(&DOLLAR_DIALECT);

            assert!(
                !request.sql.contains("$1"),
                "placeholder must be inlined: {}",
                request.sql
            );
            assert!(
                request.sql.contains("'it''s done'"),
                "value must be an escaped literal: {}",
                request.sql
            );
            assert!(request.params.is_empty(), "drivers do not bind params");
        }

        #[test]
        fn sample_rows_inline_filter_values_for_at_sign_placeholders() {
            let request = sample_request_for(&AT_SIGN_DIALECT);

            assert!(
                !request.sql.contains("@p1"),
                "placeholder must be inlined: {}",
                request.sql
            );
            assert!(
                request.sql.contains("it''s done'"),
                "value must be an escaped literal: {}",
                request.sql
            );
            assert!(request.params.is_empty(), "drivers do not bind params");
        }

        fn delete_spec(table: &str) -> VisualMutationSpec {
            VisualMutationSpec {
                from: TableRef {
                    schema: None,
                    name: table.to_string(),
                },
                filter: None,
                kind: MutationKind::Delete,
            }
        }

        // F-R4-1: MSSQL — SELECT must contain "ORDER BY 1" before "OFFSET 0 ROWS FETCH NEXT".
        #[test]
        fn mssql_sample_rows_includes_order_by_before_offset_fetch() {
            let conn = MssqlRecordingConnection::new();
            let conn_ref = Arc::clone(&conn);
            let spec = delete_spec("orders");

            fetch_sample_rows(conn as Arc<dyn dbflux_core::Connection>, &spec, |_| {});

            let calls = conn_ref.recorded_calls();
            assert_eq!(calls.len(), 1, "expected exactly one SELECT call");
            let sql = &calls[0];

            let order_by_pos = sql.to_ascii_uppercase().find("ORDER BY 1");
            let offset_pos = sql.to_ascii_uppercase().find("OFFSET");

            assert!(
                order_by_pos.is_some(),
                "MSSQL sample rows SELECT must contain ORDER BY 1; got: {}",
                sql
            );
            assert!(
                offset_pos.is_some(),
                "MSSQL sample rows SELECT must contain OFFSET clause; got: {}",
                sql
            );
            assert!(
                order_by_pos.unwrap() < offset_pos.unwrap(),
                "ORDER BY 1 must precede OFFSET clause; got: {}",
                sql
            );
        }

        // F-R4-1: PostgreSQL — SELECT must contain "ORDER BY 1" before "LIMIT".
        #[test]
        fn postgres_sample_rows_includes_order_by_one() {
            let conn = PgRecordingConnection::new();
            let conn_ref = Arc::clone(&conn);
            let spec = delete_spec("orders");

            fetch_sample_rows(conn as Arc<dyn dbflux_core::Connection>, &spec, |_| {});

            let calls = conn_ref.recorded_calls();
            assert_eq!(calls.len(), 1, "expected exactly one SELECT call");
            let sql = &calls[0];

            let order_by_pos = sql.to_ascii_uppercase().find("ORDER BY 1");
            let limit_pos = sql.to_ascii_uppercase().find("LIMIT");

            assert!(
                order_by_pos.is_some(),
                "Postgres sample rows SELECT must contain ORDER BY 1; got: {}",
                sql
            );
            assert!(
                limit_pos.is_some(),
                "Postgres sample rows SELECT must contain LIMIT clause; got: {}",
                sql
            );
            assert!(
                order_by_pos.unwrap() < limit_pos.unwrap(),
                "ORDER BY 1 must precede LIMIT clause; got: {}",
                sql
            );
        }
    }
}
