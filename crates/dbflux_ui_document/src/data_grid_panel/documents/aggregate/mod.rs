//! Aggregate view of a document collection: a JSON pipeline editor, Run, and
//! the pipeline's result documents in the same Tree, Table and JSON views the
//! Documents tab uses.
//!
//! Drivers that report `DocumentFeatures::AGGREGATE` get the view; the
//! pipeline runs through `Connection::aggregate_collection`. Results are held
//! apart from the Documents page (their own table, tree and JSON viewer), and
//! they are read-only: the table has no key columns and inserts nothing, the
//! tree opens no inline editor, and every edit request the views emit is
//! refused. A pipeline that writes (`$out`, `$merge`) is classified through
//! the driver's language service and asks for confirmation like any other
//! dangerous query.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;

use dbflux_components::components::data_table::{
    ColumnGroupHeader, DataTable, DataTableEvent, DataTableState, DocumentColumnHeader,
    DocumentPresentation, ModelSwap, SortState,
};
use dbflux_components::components::document_tree::{
    DocumentTree, DocumentTreeEvent, DocumentTreeState,
};
use dbflux_components::controls::InputEvent;
use dbflux_components::modals::ModalFocus;
use dbflux_components::vim::VimBinding;
use dbflux_core::observability::actions as audit_actions;
use dbflux_core::observability::{
    EventCategory, EventOrigin, EventOutcome, EventRecord, EventSeverity,
};
use dbflux_core::{
    AggregatePipelineError, CollectionAggregateRequest, DangerousAction, DangerousQueryKind,
    ExecutionClassification, QueryResult, SortDirection, TaskKind, Value,
    classify_query_for_language_with_service, parse_aggregate_pipeline, value_to_document_json,
};
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::*;
use gpui_component::input::EditorState;

mod render;

use super::columns::{self, FlatCell, FlatView};
use crate::data_grid_panel::{DataGridEvent, DataGridPanel, DataSource};
use crate::data_view::DataViewMode;

/// Most documents one run shows. A pipeline that yields more is cut and the
/// footer says so.
pub(in crate::data_grid_panel) const AGGREGATE_RESULT_LIMIT: u32 = 1_000;

/// Pipelines kept in the per-collection history menu.
const PIPELINE_HISTORY_LIMIT: usize = 20;

/// The documents of the last run, and their flattened grid.
pub(in crate::data_grid_panel) struct AggregateResults {
    pub raw: QueryResult,
    pub documents: Vec<Value>,
    pub top_order: Vec<String>,
    pub flat: FlatView,
}

/// A run waiting for the dangerous-query confirmation.
pub(in crate::data_grid_panel) struct PendingAggregateRun {
    pub request: CollectionAggregateRequest,
    /// What the driver flagged; `None` when the pipeline could not be
    /// classified, which is confirmed with the generic wording.
    pub kind: Option<DangerousQueryKind>,
    /// Native text of the pipeline, shown in the confirmation.
    pub query_text: Option<String>,
    pub suppress: bool,
    /// The run came from the query builder's Run pipeline.
    pub from_builder: bool,
}

/// What happens to a pipeline before it runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::data_grid_panel) enum AggregateGate {
    Run,
    Confirm(Option<DangerousQueryKind>),
    Refuse(String),
}

/// Applies the dangerous-query rules to a pipeline.
///
/// `query_text` is the driver's native text for the pipeline, `dangerous`
/// its `LanguageService::detect_dangerous` verdict and `classification` the
/// execution classification of that text. A pipeline the driver flags goes
/// through the shared settings-based decision (`decide`). One it cannot
/// render as text, or that classifies as anything but a read, is always
/// confirmed: running it silently is never the fallback.
pub(in crate::data_grid_panel) fn aggregate_gate(
    query_text: Option<&str>,
    dangerous: Option<DangerousQueryKind>,
    classification: Option<ExecutionClassification>,
    decide: impl FnOnce(DangerousQueryKind) -> DangerousAction,
) -> AggregateGate {
    if let Some(kind) = dangerous {
        return match decide(kind) {
            DangerousAction::Allow => AggregateGate::Run,
            DangerousAction::Confirm(kind) => AggregateGate::Confirm(Some(kind)),
            DangerousAction::Block(message) => AggregateGate::Refuse(message),
        };
    }

    if query_text.is_none() {
        return AggregateGate::Confirm(None);
    }

    match classification {
        Some(ExecutionClassification::Read | ExecutionClassification::Metadata) => {
            AggregateGate::Run
        }
        _ => AggregateGate::Confirm(None),
    }
}

/// The inline message for a pipeline that does not parse.
pub(in crate::data_grid_panel) fn pipeline_error_message(error: &AggregatePipelineError) -> String {
    match error {
        AggregatePipelineError::Syntax(detail) => {
            dbflux_i18n::t!("document.collection.aggregate.error.syntax", error = detail)
        }
        AggregatePipelineError::NotArray => {
            dbflux_i18n::t!("document.collection.aggregate.error.not_array")
        }
        AggregatePipelineError::StageNotDocument { index } => dbflux_i18n::t!(
            "document.collection.aggregate.error.stage_not_document",
            stage = index + 1
        ),
        AggregatePipelineError::StageFieldCount { index } => dbflux_i18n::t!(
            "document.collection.aggregate.error.stage_field_count",
            stage = index + 1
        ),
        AggregatePipelineError::StageNotOperator { index, name } => dbflux_i18n::t!(
            "document.collection.aggregate.error.stage_not_operator",
            stage = index + 1,
            name = name
        ),
    }
}

/// Whether a table event asks to change the data. Aggregate results refuse
/// every one of them.
pub(in crate::data_grid_panel) fn is_edit_request(event: &DataTableEvent) -> bool {
    matches!(
        event,
        DataTableEvent::SaveRowRequested(_)
            | DataTableEvent::DeleteRowRequested(_)
            | DataTableEvent::AddRowRequested(_)
            | DataTableEvent::DuplicateRowRequested(_)
            | DataTableEvent::SetNullRequested { .. }
            | DataTableEvent::ModalEditRequested { .. }
            | DataTableEvent::CommitInsertRequested(_)
            | DataTableEvent::CommitDeleteRequested(_)
            | DataTableEvent::SaveAllRequested { .. }
    )
}

/// Whether a tree event asks to change the data.
pub(in crate::data_grid_panel) fn is_tree_edit_request(event: &DocumentTreeEvent) -> bool {
    matches!(
        event,
        DocumentTreeEvent::InlineEditCommitted { .. }
            | DocumentTreeEvent::DeleteRequested(_)
            | DocumentTreeEvent::DocumentPreviewRequested { .. }
    )
}

/// Header extras of the aggregate grid: the groups of expanded object
/// columns. There is no schema sample, so no presence bars.
fn aggregate_presentation(flat: &FlatView) -> DocumentPresentation {
    DocumentPresentation {
        headers: flat
            .columns
            .iter()
            .map(|column| DocumentColumnHeader {
                group: column.group.as_ref().map(|key| ColumnGroupHeader {
                    key: key.as_str().into(),
                    label: key.as_str().into(),
                    type_label: "obj".into(),
                }),
                presence: None,
            })
            .collect(),
    }
}

/// Orders the flattened rows by one column, missing cells last.
fn sort_flat_rows(flat: &mut FlatView, sort: SortState) {
    flat.rows.sort_by(|left, right| {
        let left = left.cells.get(sort.column_ix).and_then(FlatCell::value);
        let right = right.cells.get(sort.column_ix).and_then(FlatCell::value);

        let ordering = match (left, right) {
            (Some(left), Some(right)) => left.cmp(right),
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (None, None) => Ordering::Equal,
        };

        match sort.direction {
            SortDirection::Ascending => ordering,
            SortDirection::Descending => ordering.reverse(),
        }
    });
}

/// State of the Aggregate view of one collection tab.
pub(in crate::data_grid_panel) struct AggregateViewState {
    pub pipeline_editor: Entity<EditorState>,
    /// Vim mode for the pipeline editor.
    pub pipeline_vim: VimBinding,
    /// Why the pipeline cannot run, shown under the editor.
    pub pipeline_error: Option<String>,
    pub view_mode: DataViewMode,
    pub results: Option<AggregateResults>,
    pub running: bool,
    /// Bumped by every run, so a slower earlier run cannot replace the
    /// results of a later one.
    run_generation: u64,
    pub history: Vec<String>,
    pub history_open: bool,
    pub pending_run: Option<PendingAggregateRun>,
    /// Set by the query builder right before it runs its pipeline; taken by
    /// the next run.
    pub next_run_from_builder: bool,
    /// Whether the results shown came from the query builder, which then
    /// explains why they cannot be edited.
    pub results_from_builder: bool,
    pub confirm_focus: ModalFocus,
    expanded: BTreeSet<String>,
    sort: Option<SortState>,
    pub table_state: Option<Entity<DataTableState>>,
    pub data_table: Option<Entity<DataTable>>,
    pub tree_state: Option<Entity<DocumentTreeState>>,
    pub tree: Option<Entity<DocumentTree>>,
    /// The results as JSON, disabled for editing.
    pub json_viewer: Entity<EditorState>,
    json_reload: bool,
    _subscriptions: Vec<Subscription>,
}

impl AggregateViewState {
    pub fn new(window: &mut Window, cx: &mut Context<DataGridPanel>) -> Self {
        let pipeline_editor = cx.new(|cx| {
            EditorState::new(window, cx)
                .language("json")
                .line_number(true)
                .soft_wrap(false)
                .placeholder(dbflux_i18n::t!("document.collection.aggregate.placeholder"))
        });

        let json_viewer = cx.new(|cx| {
            EditorState::new(window, cx)
                .language("json")
                .line_number(true)
        });
        let pipeline_vim = VimBinding::new(pipeline_editor.clone(), window, cx);

        let subscriptions =
            vec![
                cx.subscribe(&pipeline_editor, |this, _, event: &InputEvent, cx| {
                    if let InputEvent::Change = event
                        && this.collection.aggregate.pipeline_error.take().is_some()
                    {
                        cx.notify();
                    }
                }),
            ];

        Self {
            pipeline_editor,
            pipeline_vim,
            pipeline_error: None,
            view_mode: DataViewMode::Document,
            results: None,
            running: false,
            run_generation: 0,
            history: Vec::new(),
            history_open: false,
            pending_run: None,
            next_run_from_builder: false,
            results_from_builder: false,
            confirm_focus: ModalFocus::new(cx),
            expanded: BTreeSet::new(),
            sort: None,
            table_state: None,
            data_table: None,
            tree_state: None,
            tree: None,
            json_viewer,
            json_reload: false,
            _subscriptions: subscriptions,
        }
    }

    /// Number of result documents, `None` before the first run.
    pub fn result_count(&self) -> Option<usize> {
        self.results.as_ref().map(|results| results.documents.len())
    }

    /// Whether the last run was cut at [`AGGREGATE_RESULT_LIMIT`].
    pub fn truncated(&self) -> bool {
        self.results
            .as_ref()
            .is_some_and(|results| results.raw.rows_truncated())
    }

    fn remember(&mut self, pipeline: &str) {
        let pipeline = pipeline.trim().to_string();
        if pipeline.is_empty() {
            return;
        }

        self.history.retain(|existing| *existing != pipeline);
        self.history.insert(0, pipeline);
        self.history.truncate(PIPELINE_HISTORY_LIMIT);
    }
}

impl DataGridPanel {
    /// Keyboard commands while the Aggregate view shows: Run, the results
    /// view cycle and the confirmation. Everything else is left to the
    /// focused editor or result view, never to the hidden documents grid.
    pub(in crate::data_grid_panel) fn dispatch_aggregate_command(
        &mut self,
        command: dbflux_app::keymap::Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        use dbflux_app::keymap::Command;

        if self.collection.aggregate.pending_run.is_some() {
            match command {
                Command::Cancel => self.cancel_aggregate_run(cx),
                Command::Execute => self.confirm_aggregate_run(cx),
                _ => {}
            }
            return true;
        }

        match command {
            Command::RunQuery => {
                match self.document_builder_in_aggregate(cx) {
                    Some(builder) => builder.update(cx, |builder, cx| builder.request_run(cx)),
                    None => self.run_aggregate(window, cx),
                }
                true
            }
            Command::CycleDocumentView => {
                self.cycle_aggregate_view(cx);
                true
            }
            Command::Cancel if self.collection.aggregate.history_open => {
                self.collection.aggregate.history_open = false;
                cx.notify();
                true
            }
            _ => false,
        }
    }

    // === Running ===

    /// Run (Ctrl+Enter): validates the pipeline, classifies it and runs it,
    /// or asks first when it writes.
    pub(in crate::data_grid_panel) fn run_aggregate(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let aggregate = &mut self.collection.aggregate;
        aggregate.history_open = false;
        let from_builder = std::mem::take(&mut aggregate.next_run_from_builder);

        if aggregate.running || aggregate.pending_run.is_some() {
            return;
        }

        let text = aggregate.pipeline_editor.read(cx).value().to_string();
        let pipeline = match parse_aggregate_pipeline(&text) {
            Ok(pipeline) => pipeline,
            Err(error) => {
                aggregate.pipeline_error = Some(pipeline_error_message(&error));
                cx.notify();
                return;
            }
        };

        aggregate.pipeline_error = None;
        aggregate.remember(&text);

        let DataSource::Collection {
            profile_id,
            collection,
            ..
        } = &self.source
        else {
            return;
        };
        let profile_id = *profile_id;

        let request =
            CollectionAggregateRequest::new(collection.clone(), pipeline, AGGREGATE_RESULT_LIMIT);

        let Some(connection) = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|connected| connected.connection.clone())
        else {
            report_error(
                UserFacingError::new(
                    ErrorKind::Network,
                    dbflux_i18n::t!("document.data.grid.error.connection_not_available"),
                ),
                cx,
            );
            return;
        };

        let query_text = connection
            .query_generator()
            .and_then(|generator| generator.aggregate_query(&request))
            .map(|query| query.text);

        let language_service = connection.language_service();
        let dangerous = query_text
            .as_deref()
            .and_then(|text| language_service.detect_dangerous(text));
        let classification = query_text.as_deref().map(|text| {
            classify_query_for_language_with_service(
                &connection.metadata().query_language,
                text,
                Some(language_service),
            )
        });

        let gate = aggregate_gate(query_text.as_deref(), dangerous, classification, |kind| {
            let state = self.app_state.read(cx);
            let is_suppressed = state.dangerous_query_suppressions().is_suppressed(kind);
            let effective = state.effective_settings_for_connection(Some(profile_id));

            crate::code::evaluate_dangerous_with_effective_settings(
                kind,
                is_suppressed,
                &effective,
                false,
            )
        });

        match gate {
            AggregateGate::Run => self.execute_aggregate(request, from_builder, cx),
            AggregateGate::Confirm(kind) => {
                let aggregate = &mut self.collection.aggregate;
                aggregate.pending_run = Some(PendingAggregateRun {
                    request,
                    kind,
                    query_text,
                    suppress: false,
                    from_builder,
                });
                aggregate.confirm_focus.focus(None, window, cx);
                cx.notify();
            }
            AggregateGate::Refuse(message) => {
                self.collection.aggregate.pipeline_error = Some(message.clone());
                report_error(UserFacingError::new(ErrorKind::User, message), cx);
                cx.notify();
            }
        }
    }

    /// "Run anyway" in the confirmation.
    pub(in crate::data_grid_panel) fn confirm_aggregate_run(&mut self, cx: &mut Context<Self>) {
        let aggregate = &mut self.collection.aggregate;
        let Some(pending) = aggregate.pending_run.take() else {
            return;
        };
        aggregate.confirm_focus.restore(cx);

        if let Some(kind) = pending.kind {
            if pending.suppress {
                self.app_state.update(cx, |state, _| {
                    state
                        .dangerous_query_suppressions_mut()
                        .set_suppressed(kind);
                });
            }
            self.emit_aggregate_confirmed_audit_event(kind, cx);
        }

        self.execute_aggregate(pending.request, pending.from_builder, cx);
    }

    /// "Cancel" in the confirmation: nothing runs.
    pub(in crate::data_grid_panel) fn cancel_aggregate_run(&mut self, cx: &mut Context<Self>) {
        let aggregate = &mut self.collection.aggregate;
        if aggregate.pending_run.take().is_some() {
            aggregate.confirm_focus.restore(cx);
            cx.notify();
        }
    }

    fn execute_aggregate(
        &mut self,
        request: CollectionAggregateRequest,
        from_builder: bool,
        cx: &mut Context<Self>,
    ) {
        let DataSource::Collection { profile_id, .. } = &self.source else {
            return;
        };

        let Some(connection) = self
            .app_state
            .read(cx)
            .connections()
            .get(profile_id)
            .map(|connected| connected.connection.clone())
        else {
            report_error(
                UserFacingError::new(
                    ErrorKind::Network,
                    dbflux_i18n::t!("document.data.grid.error.connection_not_available"),
                ),
                cx,
            );
            return;
        };

        let (task_id, _cancel) = self.runner.start_mutation(
            TaskKind::Query,
            dbflux_i18n::t!(
                "document.collection.aggregate.task",
                collection = request.collection.qualified_name()
            ),
            cx,
        );

        let aggregate = &mut self.collection.aggregate;
        aggregate.running = true;
        aggregate.run_generation += 1;
        let generation = aggregate.run_generation;
        cx.notify();

        let entity = cx.entity().clone();
        let task = cx
            .background_executor()
            .spawn(async move { connection.aggregate_collection(&request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(error) = &result {
                report_error_async(
                    UserFacingError::new(
                        ErrorKind::Driver,
                        dbflux_i18n::t!(
                            "document.collection.aggregate.failed",
                            error = error.to_string()
                        ),
                    ),
                    cx,
                );
            }

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    match &result {
                        Ok(_) => panel.runner.complete_mutation(task_id, cx),
                        Err(error) => panel.runner.fail_mutation(task_id, error.to_string(), cx),
                    }

                    if panel.collection.aggregate.run_generation != generation {
                        return;
                    }

                    panel.collection.aggregate.running = false;
                    if let Ok(result) = result {
                        panel.collection.aggregate.results_from_builder = from_builder;
                        panel.apply_aggregate_result(result, cx);
                    }
                    cx.notify();
                });
            });
        })
        .detach();
    }

    // === Results ===

    /// Keeps a run's documents and rebuilds the read-only table, tree and
    /// JSON viewer from them.
    pub(in crate::data_grid_panel) fn apply_aggregate_result(
        &mut self,
        result: QueryResult,
        cx: &mut Context<Self>,
    ) {
        let documents = columns::documents_from_result(&result);
        let top_order = result
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect();

        let aggregate = &mut self.collection.aggregate;
        aggregate.expanded.clear();
        aggregate.sort = None;
        aggregate.results = Some(AggregateResults {
            raw: result,
            documents,
            top_order,
            flat: FlatView::default(),
        });

        self.reflatten_aggregate(cx);
        self.rebuild_aggregate_tree(cx);

        let aggregate = &mut self.collection.aggregate;
        aggregate.json_reload = true;
        cx.notify();
    }

    /// Rebuilds the grid of the results after a run, an expansion or a sort.
    fn reflatten_aggregate(&mut self, cx: &mut Context<Self>) {
        let aggregate = &mut self.collection.aggregate;
        let Some(results) = aggregate.results.as_mut() else {
            return;
        };

        let mut flat = columns::flatten(
            &results.documents,
            &results.top_order,
            &aggregate.expanded,
            None,
        );
        if let Some(sort) = aggregate.sort {
            sort_flat_rows(&mut flat, sort);
        }

        let model = Arc::new(super::flat_table_model(&flat));
        let presentation = aggregate_presentation(&flat);
        results.flat = flat;
        let sort = aggregate.sort;

        if let Some(table_state) = aggregate.table_state.clone() {
            table_state.update(cx, |state, cx| {
                match sort {
                    Some(sort) => state.set_sort_without_emit(sort),
                    None => state.clear_sort_without_emit(),
                }
                state.set_model(model, ModelSwap::KeepCursor, cx);
                state.set_document_presentation(Some(presentation), cx);
            });
            return;
        }

        let table_state = cx.new(|cx| {
            let mut state = DataTableState::new(model, cx);
            state.set_pk_columns(Vec::new());
            state.set_insertable(false);
            state.set_document_presentation(Some(presentation), cx);
            state
        });
        let data_table = cx.new(|cx| DataTable::new("aggregate-table", table_state.clone(), cx));

        let subscription = cx.subscribe(&table_state, |this, _, event: &DataTableEvent, cx| {
            this.handle_aggregate_table_event(event, cx);
        });

        let aggregate = &mut self.collection.aggregate;
        aggregate.table_state = Some(table_state);
        aggregate.data_table = Some(data_table);
        aggregate._subscriptions.push(subscription);
    }

    fn rebuild_aggregate_tree(&mut self, cx: &mut Context<Self>) {
        let aggregate = &mut self.collection.aggregate;
        let Some(results) = aggregate.results.as_ref() else {
            return;
        };

        if let Some(tree_state) = aggregate.tree_state.clone() {
            let raw = results.raw.clone();
            tree_state.update(cx, |state, cx| state.load_from_result(&raw, cx));
            return;
        }

        let raw = results.raw.clone();
        let tree_state = cx.new(|cx| {
            let mut state = DocumentTreeState::new(cx);
            state.set_read_only(true, cx);
            state.load_from_result(&raw, cx);
            state
        });
        let tree = cx.new(|cx| DocumentTree::new("aggregate-tree", tree_state.clone(), cx));

        let subscription = cx.subscribe(&tree_state, |this, _, event: &DocumentTreeEvent, cx| {
            if matches!(event, DocumentTreeEvent::Focused) {
                cx.emit(DataGridEvent::Focused);
            } else if is_tree_edit_request(event) {
                this.refuse_aggregate_edit(cx);
            }
        });

        let aggregate = &mut self.collection.aggregate;
        aggregate.tree_state = Some(tree_state);
        aggregate.tree = Some(tree);
        aggregate._subscriptions.push(subscription);
    }

    /// Reacts to the aggregate table: expansion and local sort are allowed,
    /// every edit is refused. Returns whether the event was refused.
    pub(in crate::data_grid_panel) fn handle_aggregate_table_event(
        &mut self,
        event: &DataTableEvent,
        cx: &mut Context<Self>,
    ) -> bool {
        if is_edit_request(event) {
            self.refuse_aggregate_edit(cx);
            return true;
        }

        match event {
            DataTableEvent::Focused => cx.emit(DataGridEvent::Focused),
            DataTableEvent::ToggleColumnGroupRequested { col } => {
                self.toggle_aggregate_column_group(*col, cx);
            }
            DataTableEvent::SortChanged(sort) => {
                self.collection.aggregate.sort = *sort;
                self.reflatten_aggregate(cx);
                cx.notify();
            }
            DataTableEvent::CopyRowRequested(row) => self.copy_aggregate_document(*row, cx),
            _ => {}
        }

        false
    }

    fn refuse_aggregate_edit(&mut self, cx: &mut Context<Self>) {
        Toast::warning(dbflux_i18n::t!(
            "document.collection.aggregate.read_only_edit"
        ))
        .meta_right(now_hms())
        .push(cx);
    }

    fn toggle_aggregate_column_group(&mut self, col: usize, cx: &mut Context<Self>) {
        let aggregate = &mut self.collection.aggregate;
        let Some(column) = aggregate
            .results
            .as_ref()
            .and_then(|results| results.flat.columns.get(col))
            .cloned()
        else {
            return;
        };

        let key = match (&column.group, column.type_label.as_str()) {
            (Some(group), _) => group.clone(),
            (None, "obj") => column.dotted(),
            _ => return,
        };

        if !aggregate.expanded.remove(&key) {
            aggregate.expanded.insert(key);
        }
        aggregate.sort = None;

        self.reflatten_aggregate(cx);
        cx.notify();
    }

    /// `YY` on the aggregate table: the row's document as JSON.
    fn copy_aggregate_document(&mut self, row: usize, cx: &mut Context<Self>) {
        let Some(results) = self.collection.aggregate.results.as_ref() else {
            return;
        };
        let Some(document) = results
            .flat
            .rows
            .get(row)
            .and_then(|flat_row| results.documents.get(flat_row.document))
        else {
            return;
        };

        match serde_json::to_string_pretty(&value_to_document_json(document)) {
            Ok(json) => cx.write_to_clipboard(ClipboardItem::new_string(json)),
            Err(error) => report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    crate::labels::collection_inspector_json_failed(&error.to_string()),
                ),
                cx,
            ),
        }
    }

    /// Switches the results between Tree, Table and JSON.
    pub(in crate::data_grid_panel) fn set_aggregate_view_mode(
        &mut self,
        mode: DataViewMode,
        cx: &mut Context<Self>,
    ) {
        let aggregate = &mut self.collection.aggregate;
        if aggregate.view_mode == mode {
            return;
        }

        aggregate.view_mode = mode;
        if mode == DataViewMode::Json {
            aggregate.json_reload = true;
        }
        cx.notify();
    }

    /// Next results view in Tree, Table, JSON order.
    pub(in crate::data_grid_panel) fn cycle_aggregate_view(&mut self, cx: &mut Context<Self>) {
        let next = match self.collection.aggregate.view_mode {
            DataViewMode::Document => DataViewMode::Table,
            DataViewMode::Table => DataViewMode::Json,
            DataViewMode::Json => DataViewMode::Document,
        };
        self.set_aggregate_view_mode(next, cx);
    }

    /// Loads the results into the JSON viewer from the render pass (the
    /// editor needs a window to change its text).
    pub(in crate::data_grid_panel) fn flush_aggregate_json(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let aggregate = &mut self.collection.aggregate;
        if !std::mem::take(&mut aggregate.json_reload) {
            return;
        }

        let documents: Vec<serde_json::Value> = aggregate
            .results
            .as_ref()
            .map(|results| {
                results
                    .documents
                    .iter()
                    .map(value_to_document_json)
                    .collect()
            })
            .unwrap_or_default();
        let text = serde_json::to_string_pretty(&documents).unwrap_or_default();

        aggregate
            .json_viewer
            .update(cx, |viewer, cx| viewer.set_value(text, window, cx));
    }

    /// Restores a pipeline from the history into the editor.
    pub(in crate::data_grid_panel) fn load_aggregate_history_entry(
        &mut self,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let aggregate = &mut self.collection.aggregate;
        aggregate.history_open = false;

        let Some(pipeline) = aggregate.history.get(index).cloned() else {
            cx.notify();
            return;
        };

        aggregate
            .pipeline_editor
            .update(cx, |editor, cx| editor.set_value(pipeline, window, cx));
        aggregate.pipeline_error = None;
        cx.notify();
    }

    fn emit_aggregate_confirmed_audit_event(&self, kind: DangerousQueryKind, cx: &mut App) {
        let DataSource::Collection {
            profile_id,
            collection,
            ..
        } = &self.source
        else {
            return;
        };

        let driver_id = self
            .app_state
            .read(cx)
            .connections()
            .get(profile_id)
            .map(|connected| connected.profile.driver_id())
            .unwrap_or_default();

        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as i64)
            .unwrap_or(0);

        let mut event = EventRecord::new(
            timestamp_ms,
            EventSeverity::Warn,
            EventCategory::Query,
            EventOutcome::Success,
        )
        .with_typed_action(audit_actions::DANGEROUS_QUERY_CONFIRMED)
        .with_summary(format!("Dangerous query confirmed: {}", kind.message()))
        .with_connection_context(
            profile_id.to_string(),
            collection.database.clone(),
            driver_id,
        )
        .with_origin(EventOrigin::local());
        event.details_json =
            Some(serde_json::json!({ "dangerous_kind": kind.message() }).to_string());

        if let Err(error) = self.app_state.read(cx).audit_service().record(event) {
            log::warn!("Failed to emit dangerous query audit event: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    // Only what the tests need: `use super::*` would pull in `gpui::*`,
    // whose macros overflow the recursion limit under `#[test]`.
    use super::{
        AggregateGate, aggregate_gate, is_edit_request, is_tree_edit_request,
        pipeline_error_message,
    };
    use dbflux_components::components::data_table::DataTableEvent;
    use dbflux_components::components::document_tree::{DocumentTreeEvent, NodeId};
    use dbflux_core::{
        AggregatePipelineError, DangerousAction, DangerousQueryKind, ExecutionClassification,
    };

    #[test]
    fn a_read_pipeline_runs_without_asking() {
        assert_eq!(
            aggregate_gate(
                Some("db.orders.aggregate([])"),
                None,
                Some(ExecutionClassification::Read),
                |_| unreachable!("no dangerous kind to decide"),
            ),
            AggregateGate::Run
        );
    }

    #[test]
    fn a_writing_pipeline_goes_through_the_dangerous_query_decision() {
        let confirm = aggregate_gate(
            Some("db.orders.aggregate([{ $out: 'archive' }])"),
            Some(DangerousQueryKind::MongoAggregateWrite),
            Some(ExecutionClassification::Write),
            DangerousAction::Confirm,
        );
        assert_eq!(
            confirm,
            AggregateGate::Confirm(Some(DangerousQueryKind::MongoAggregateWrite))
        );

        let suppressed = aggregate_gate(
            Some("db.orders.aggregate([{ $out: 'archive' }])"),
            Some(DangerousQueryKind::MongoAggregateWrite),
            Some(ExecutionClassification::Write),
            |_| DangerousAction::Allow,
        );
        assert_eq!(suppressed, AggregateGate::Run);

        let blocked = aggregate_gate(
            Some("db.orders.aggregate([{ $out: 'archive' }])"),
            Some(DangerousQueryKind::MongoAggregateWrite),
            Some(ExecutionClassification::Write),
            |_| DangerousAction::Block("no".to_string()),
        );
        assert_eq!(blocked, AggregateGate::Refuse("no".to_string()));
    }

    #[test]
    fn an_unclassified_or_writing_pipeline_is_never_run_silently() {
        assert_eq!(
            aggregate_gate(None, None, None, |_| DangerousAction::Allow),
            AggregateGate::Confirm(None),
            "a pipeline the driver cannot render is confirmed"
        );
        assert_eq!(
            aggregate_gate(
                Some("db.orders.aggregate([])"),
                None,
                Some(ExecutionClassification::Write),
                |_| DangerousAction::Allow,
            ),
            AggregateGate::Confirm(None),
            "a write the driver did not flag is still confirmed"
        );
    }

    #[test]
    fn pipeline_errors_read_as_inline_messages() {
        let messages = [
            pipeline_error_message(&AggregatePipelineError::Syntax("EOF".to_string())),
            pipeline_error_message(&AggregatePipelineError::NotArray),
            pipeline_error_message(&AggregatePipelineError::StageNotDocument { index: 0 }),
            pipeline_error_message(&AggregatePipelineError::StageFieldCount { index: 1 }),
            pipeline_error_message(&AggregatePipelineError::StageNotOperator {
                index: 2,
                name: "match".to_string(),
            }),
        ];

        for message in &messages {
            assert!(
                !message.is_empty() && !message.starts_with("document."),
                "unresolved message: {message}"
            );
        }
        assert!(messages[0].contains("EOF"));
        assert!(messages[3].contains('2'), "stages are numbered from one");
        assert!(messages[4].contains('3') && messages[4].contains("match"));
    }

    #[test]
    fn every_edit_request_of_the_views_is_recognized() {
        for event in [
            DataTableEvent::SaveRowRequested(0),
            DataTableEvent::DeleteRowRequested(0),
            DataTableEvent::AddRowRequested(0),
            DataTableEvent::DuplicateRowRequested(0),
            DataTableEvent::SetNullRequested { row: 0, col: 0 },
            DataTableEvent::ModalEditRequested {
                row: 0,
                col: 0,
                value: String::new(),
                is_json: true,
            },
            DataTableEvent::CommitInsertRequested(0),
            DataTableEvent::CommitDeleteRequested(0),
        ] {
            assert!(is_edit_request(&event), "{event:?} must be refused");
        }

        assert!(!is_edit_request(&DataTableEvent::Focused));
        assert!(!is_edit_request(&DataTableEvent::CopyRowRequested(0)));

        assert!(is_tree_edit_request(
            &DocumentTreeEvent::InlineEditCommitted {
                node_id: NodeId::root(0).child("name"),
                new_value: "x".to_string(),
            }
        ));
        assert!(is_tree_edit_request(&DocumentTreeEvent::DeleteRequested(
            NodeId::root(0)
        )));
        assert!(!is_tree_edit_request(&DocumentTreeEvent::Focused));
    }
}

#[cfg(test)]
mod panel_tests {
    use super::super::{CollectionTab, available_collection_tabs};
    use crate::data_grid_panel::{DataGridPanel, DataSource};
    use crate::data_view::DataViewMode;
    use dbflux_components::components::data_table::DataTableEvent;
    use dbflux_components::components::data_table::selection::CellCoord;
    use dbflux_components::theme;
    use dbflux_core::{
        CollectionAggregateRequest, CollectionCountEstimate, CollectionRef, ColumnKind, ColumnMeta,
        DatabaseCategory, DbError, DocumentFeatures, GeneratedQuery, MutationCategory,
        MutationRequest, Pagination, QueryGenerator, QueryLanguage, QueryResult, Value,
    };
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::AppStateEntity;
    use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
    use gpui::{AppContext, Entity, TestAppContext, VisualTestContext};
    use gpui_component::Root;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    struct StubGenerator;

    impl QueryGenerator for StubGenerator {
        fn supported_categories(&self) -> &'static [MutationCategory] {
            &[MutationCategory::Document]
        }

        fn generate_mutation(&self, _mutation: &MutationRequest) -> Option<GeneratedQuery> {
            None
        }

        fn aggregate_query(&self, request: &CollectionAggregateRequest) -> Option<GeneratedQuery> {
            Some(GeneratedQuery {
                language: QueryLanguage::MongoQuery,
                text: format!(
                    "db.{}.aggregate({})",
                    request.collection.name,
                    serde_json::Value::Array(request.pipeline.clone())
                ),
            })
        }
    }

    /// A document connection that answers every pipeline with two grouped
    /// documents, and reports only the document features it is given.
    struct StubDocumentConnection {
        metadata: dbflux_core::DriverMetadata,
        features: DocumentFeatures,
    }

    impl dbflux_core::Connection for StubDocumentConnection {
        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            &self.metadata
        }

        fn kind(&self) -> dbflux_core::DbKind {
            dbflux_core::DbKind::MongoDB
        }

        fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
            dbflux_core::SchemaLoadingStrategy::SingleDatabase
        }

        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            unimplemented!("the aggregate tests never reach a dialect")
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, _request: &dbflux_core::QueryRequest) -> Result<QueryResult, DbError> {
            Err(DbError::NotSupported("stub".to_string()))
        }

        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, DbError> {
            Ok(dbflux_core::SchemaSnapshot::default())
        }

        fn document_features(&self) -> DocumentFeatures {
            self.features
        }

        fn query_generator(&self) -> Option<&dyn QueryGenerator> {
            Some(&StubGenerator)
        }

        fn aggregate_collection(
            &self,
            _request: &CollectionAggregateRequest,
        ) -> Result<QueryResult, DbError> {
            let column = |name: &str, kind| ColumnMeta {
                name: name.to_string(),
                type_name: String::new(),
                kind,
                nullable: true,
                is_primary_key: false,
            };

            Ok(QueryResult::json(
                vec![
                    column("_id", ColumnKind::Text),
                    column("total", ColumnKind::Integer),
                ],
                vec![
                    vec![Value::Text("north".to_string()), Value::Int(20)],
                    vec![Value::Text("south".to_string()), Value::Int(16)],
                ],
                Duration::from_millis(3),
            ))
        }
    }

    fn register_document_connection(
        cx: &mut TestAppContext,
        features: DocumentFeatures,
    ) -> (Entity<AppStateEntity>, Uuid) {
        cx.update(gpui_component::init);
        cx.update(theme::init);
        cx.update(|cx| {
            let host = cx.new(|_cx| ToastHost::new());
            cx.set_global(ToastGlobal { host });
        });

        let metadata = dbflux_core::DriverMetadata {
            id: "stub-document".to_string(),
            display_name: "Stub".to_string(),
            description: "test stub".to_string(),
            category: DatabaseCategory::Document,
            transfer_family: dbflux_core::TransferFamily::Incompatible,
            deployment_class: None,
            query_language: QueryLanguage::MongoQuery,
            capabilities: dbflux_core::DriverCapabilities::empty(),
            default_port: None,
            uri_scheme: "stub".to_string(),
            icon: dbflux_core::Icon::Database,
            syntax: None,
            query: None,
            mutation: None,
            ddl: None,
            transactions: None,
            limits: None,
            ssl_modes: None,
            ssl_cert_fields: None,
            classification_override: None,
            default_chunk_size: None,
            supports_lock_timeout: false,
            editor_profile: None,
        };

        let profile_id = Uuid::new_v4();
        let app_state = cx.update(|cx| {
            cx.new(|_| {
                let storage_runtime =
                    StorageRuntime::in_memory().expect("isolated storage runtime");
                AppStateEntity::new_with_storage_runtime(storage_runtime)
                    .expect("test storage setup")
            })
        });

        cx.update(|cx| {
            app_state.update(cx, |app, _cx| {
                let profile = dbflux_core::ConnectionProfile::new(
                    "test",
                    dbflux_core::DbConfig::SQLite {
                        path: std::path::PathBuf::from(":memory:"),
                        connection_id: None,
                    },
                );
                let connected = dbflux_core::ConnectedProfile {
                    profile,
                    connection: Arc::new(StubDocumentConnection { metadata, features }),
                    schema: None,
                    mutation_policy: dbflux_core::MutationPolicy::default(),
                    read_only_reason: None,
                    database_schemas: Default::default(),
                    table_details: Default::default(),
                    collection_children: Default::default(),
                    schema_types: Default::default(),
                    schema_columns: Default::default(),
                    schema_indexes: Default::default(),
                    schema_foreign_keys: Default::default(),
                    schema_routines: Default::default(),
                    dependents_cache: Default::default(),
                    active_database: None,
                    redis_key_cache: Default::default(),
                    database_connections: Default::default(),
                    proxy_tunnel: None,
                };
                app.connections_mut().insert(profile_id, connected);
            });
        });

        (app_state, profile_id)
    }

    fn collection_panel(
        cx: &mut TestAppContext,
        features: DocumentFeatures,
    ) -> (Entity<DataGridPanel>, &mut VisualTestContext) {
        let (app_state, profile_id) = register_document_connection(cx, features);
        let holder = Rc::new(RefCell::new(None));
        let holder_clone = holder.clone();

        let (_, window) = cx.add_window_view(move |window, cx| {
            let panel = cx.new(|cx| {
                let source = DataSource::Collection {
                    profile_id,
                    collection: CollectionRef::new("shop", "orders"),
                    pagination: Pagination::default(),
                    total_docs: None,
                };
                DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
            });
            holder_clone.replace(Some(panel.clone()));
            Root::new(panel, window, cx)
        });

        let panel = holder.borrow().clone().expect("panel should be created");
        (panel, window)
    }

    fn run_pipeline(panel: &Entity<DataGridPanel>, window: &mut VisualTestContext, text: &str) {
        let text = text.to_string();
        window.update(|window, app| {
            panel.update(app, |panel, cx| {
                panel.collection.tab = CollectionTab::Aggregate;
                panel
                    .collection
                    .aggregate
                    .pipeline_editor
                    .update(cx, |editor, cx| editor.set_value(text, window, cx));
                panel.run_aggregate(window, cx);
            });
        });
        window.run_until_parked();
    }

    /// Both collection editors follow the Vim mode setting while open.
    #[gpui::test]
    fn the_collection_editors_follow_the_vim_setting(cx: &mut TestAppContext) {
        use dbflux_components::vim::VimMode;

        let (panel, window) = collection_panel(
            cx,
            DocumentFeatures::QUERY_SLOTS | DocumentFeatures::AGGREGATE,
        );
        let modes = |window: &mut VisualTestContext| {
            window.update(|_, app| {
                let collection = &panel.read(app).collection;
                (
                    collection.json_vim.mode(),
                    collection.aggregate.pipeline_vim.mode(),
                )
            })
        };
        assert_eq!(modes(window), (None, None));

        window.update(|_, app| dbflux_components::vim::set_vim_enabled(app, true));
        window.run_until_parked();
        assert_eq!(
            modes(window),
            (Some(VimMode::Normal), Some(VimMode::Normal))
        );

        window.update(|_, app| dbflux_components::vim::set_vim_enabled(app, false));
        window.run_until_parked();
        assert_eq!(modes(window), (None, None));
    }

    /// With Vim mode on, the pipeline editor takes Vim keys: `j` moves
    /// down, `i` inserts, and Escape returns to Normal mode.
    #[gpui::test]
    fn the_pipeline_editor_takes_vim_keys(cx: &mut TestAppContext) {
        cx.update(|cx| dbflux_components::vim::set_vim_enabled(cx, true));
        let (panel, window) = collection_panel(
            cx,
            DocumentFeatures::QUERY_SLOTS | DocumentFeatures::AGGREGATE,
        );

        window.update(|window, app| {
            panel.update(app, |panel, cx| {
                panel.collection.tab = CollectionTab::Aggregate;
                panel
                    .collection
                    .aggregate
                    .pipeline_editor
                    .update(cx, |editor, cx| {
                        editor.set_value("ab\ncd", window, cx);
                        editor.set_selected_range(0..0, cx);
                        editor.focus(window, cx);
                    });
                cx.notify();
            });
        });
        window.run_until_parked();

        let text_and_cursor = |window: &mut VisualTestContext| {
            window.update(|_, app| {
                let editor = panel
                    .read(app)
                    .collection
                    .aggregate
                    .pipeline_editor
                    .read(app);
                (editor.value().to_string(), editor.cursor())
            })
        };

        window.simulate_keystrokes("j");
        window.run_until_parked();
        assert_eq!(text_and_cursor(window), ("ab\ncd".into(), 3));

        window.simulate_keystrokes("i");
        window.simulate_input("X");
        window.simulate_keystrokes("escape");
        window.simulate_keystrokes("x");
        window.run_until_parked();
        assert_eq!(
            text_and_cursor(window).0,
            "ab\ncd",
            "x deleted in Normal mode"
        );
    }

    #[test]
    fn the_aggregate_tab_follows_the_aggregate_feature() {
        assert_eq!(
            available_collection_tabs(DocumentFeatures::QUERY_SLOTS),
            vec![CollectionTab::Documents, CollectionTab::Schema]
        );
        assert_eq!(
            available_collection_tabs(DocumentFeatures::QUERY_SLOTS | DocumentFeatures::AGGREGATE),
            vec![
                CollectionTab::Documents,
                CollectionTab::Schema,
                CollectionTab::Aggregate
            ]
        );
        assert_eq!(
            available_collection_tabs(DocumentFeatures::AGGREGATE),
            vec![CollectionTab::Documents, CollectionTab::Aggregate]
        );
    }

    #[gpui::test]
    fn a_driver_without_aggregate_gets_no_aggregate_tab(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(cx, DocumentFeatures::QUERY_SLOTS);

        window.update(|_, app| {
            panel.update(app, |panel, cx| {
                assert!(
                    !panel
                        .collection_tabs(cx)
                        .contains(&CollectionTab::Aggregate)
                );

                panel.set_collection_tab(CollectionTab::Aggregate, cx);
                assert_eq!(
                    panel.collection.tab,
                    CollectionTab::Documents,
                    "a tab the driver does not offer cannot be selected"
                );
            });
        });
    }

    #[gpui::test]
    fn a_driver_with_aggregate_gets_the_aggregate_tab(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            DocumentFeatures::QUERY_SLOTS | DocumentFeatures::AGGREGATE,
        );

        window.update(|_, app| {
            panel.update(app, |panel, cx| {
                assert!(
                    panel
                        .collection_tabs(cx)
                        .contains(&CollectionTab::Aggregate)
                );
                panel.set_collection_tab(CollectionTab::Aggregate, cx);
                assert_eq!(panel.collection.tab, CollectionTab::Aggregate);
            });
        });
    }

    #[gpui::test]
    fn an_invalid_pipeline_is_reported_inline_and_not_run(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(cx, DocumentFeatures::AGGREGATE);

        run_pipeline(&panel, window, "[{ $match: {} }, { match: {} }]");

        window.update(|_, app| {
            let aggregate = &panel.read(app).collection.aggregate;
            let error = aggregate
                .pipeline_error
                .as_deref()
                .expect("an inline error");
            assert!(error.contains('2'), "the bad stage is named: {error}");
            assert!(aggregate.results.is_none(), "nothing ran");
            assert!(
                aggregate.history.is_empty(),
                "an invalid pipeline is not kept"
            );
        });
    }

    #[gpui::test]
    fn aggregate_results_are_held_apart_and_read_only(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(cx, DocumentFeatures::AGGREGATE);

        run_pipeline(
            &panel,
            window,
            "[{ $group: { _id: '$region', total: { $sum: '$amount' } } }]",
        );

        window.update(|window, app| {
            panel.update(app, |panel, cx| {
                assert!(
                    panel.collection.raw.is_none(),
                    "the Documents page is untouched by a run"
                );

                let aggregate = &panel.collection.aggregate;
                assert_eq!(aggregate.result_count(), Some(2));
                assert_eq!(aggregate.history.len(), 1);

                let table_state = aggregate.table_state.clone().expect("aggregate table");
                let tree_state = aggregate.tree_state.clone().expect("aggregate tree");

                let started = table_state.update(cx, |state, cx| {
                    assert!(!state.is_editable(), "no key columns, no edits");
                    state.start_editing(CellCoord::new(0, 1), window, cx)
                });
                assert!(!started, "a result cell never opens an editor");
                assert!(tree_state.read(cx).is_read_only());

                for event in [
                    DataTableEvent::DeleteRowRequested(0),
                    DataTableEvent::SetNullRequested { row: 0, col: 1 },
                    DataTableEvent::SaveRowRequested(0),
                ] {
                    assert!(
                        panel.handle_aggregate_table_event(&event, cx),
                        "{event:?} must be refused"
                    );
                }
                assert!(!panel.handle_aggregate_table_event(&DataTableEvent::Focused, cx));
            });
        });
    }

    #[gpui::test]
    fn a_pipeline_that_writes_waits_for_confirmation(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(cx, DocumentFeatures::AGGREGATE);

        run_pipeline(&panel, window, "[{ $match: {} }, { $out: 'archive' }]");

        window.update(|_, app| {
            let aggregate = &panel.read(app).collection.aggregate;
            let pending = aggregate
                .pending_run
                .as_ref()
                .expect("the run waits for confirmation");
            assert!(
                pending
                    .query_text
                    .as_deref()
                    .is_some_and(|text| text.contains("$out")),
                "the confirmation shows the pipeline"
            );
            assert!(aggregate.results.is_none(), "nothing ran yet");
        });

        window.update(|_, app| {
            panel.update(app, |panel, cx| panel.cancel_aggregate_run(cx));
        });
        window.run_until_parked();
        window.update(|_, app| {
            let aggregate = &panel.read(app).collection.aggregate;
            assert!(aggregate.pending_run.is_none());
            assert!(aggregate.results.is_none(), "a cancelled run never runs");
        });

        run_pipeline(&panel, window, "[{ $match: {} }, { $out: 'archive' }]");
        window.update(|_, app| {
            panel.update(app, |panel, cx| panel.confirm_aggregate_run(cx));
        });
        window.run_until_parked();
        window.update(|_, app| {
            assert_eq!(
                panel.read(app).collection.aggregate.result_count(),
                Some(2),
                "a confirmed run runs"
            );
        });
    }

    #[gpui::test]
    fn the_results_switch_views_on_their_own(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(cx, DocumentFeatures::AGGREGATE);

        window.update(|_, app| {
            panel.update(app, |panel, cx| {
                assert_eq!(
                    panel.collection.aggregate.view_mode,
                    DataViewMode::Document,
                    "aggregate results open in the tree"
                );

                let documents_mode = panel.view_config.mode;
                panel.set_aggregate_view_mode(DataViewMode::Json, cx);
                assert_eq!(panel.collection.aggregate.view_mode, DataViewMode::Json);
                assert_eq!(
                    panel.view_config.mode, documents_mode,
                    "the Documents view keeps its own mode"
                );
            });
        });
    }

    #[gpui::test]
    fn staging_an_edit_highlights_it_in_the_document_panel(cx: &mut TestAppContext) {
        use dbflux_components::components::data_table::model::CellValue;

        let (panel, window) = collection_panel(cx, DocumentFeatures::FIELD_PATCH);

        window.update(|_, app| {
            panel.update(app, |panel, cx| {
                let id = ColumnMeta {
                    name: "_id".to_string(),
                    type_name: "ObjectId".to_string(),
                    kind: ColumnKind::Text,
                    nullable: false,
                    is_primary_key: true,
                };
                let tier = ColumnMeta {
                    name: "tier".to_string(),
                    type_name: "String".to_string(),
                    kind: ColumnKind::Text,
                    nullable: true,
                    is_primary_key: false,
                };

                let page = QueryResult::json(
                    vec![id, tier],
                    vec![vec![
                        Value::ObjectId("66f0c2a1e4b0c2a1e4b0c2a1".to_string()),
                        Value::Text("team".to_string()),
                    ]],
                    Duration::from_millis(1),
                );
                assert!(panel.apply_document_page(&page, cx));
                panel.rebuild_table(None, cx);
                panel.open_document_inspector(0, 1, cx);
            });
        });

        window.update(|_, app| {
            panel.update(app, |panel, cx| {
                let table_state = panel.grid_table.table_state.clone().expect("grid");
                table_state.update(cx, |state, cx| {
                    state.stage_cell_value(0, 1, CellValue::text("enterprise"));
                    cx.notify();
                });
            });
        });
        window.run_until_parked();

        window.update(|_, app| {
            let content = panel
                .read(app)
                .inspector
                .document_inspector_content
                .clone()
                .expect("the Document panel is open");
            let rows = content.read(app).rows().to_vec();
            let tier = rows.iter().find(|row| row.key == "tier").expect("tier row");

            assert!(tier.pending, "the staged field is highlighted");
            assert_eq!(
                tier.value, "\"enterprise\"",
                "the panel shows the staged value"
            );
            assert_eq!(rows.iter().filter(|row| row.pending).count(), 1);
        });
    }

    #[gpui::test]
    fn the_schema_view_hides_the_document_count_chip(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(cx, DocumentFeatures::QUERY_SLOTS);

        window.update(|_, app| {
            panel.update(app, |panel, cx| {
                panel.collection.count = Some(CollectionCountEstimate {
                    count: 48_211,
                    exact: false,
                });
                assert!(panel.collection_meta_label().is_some());

                panel.set_collection_tab(CollectionTab::Schema, cx);
                assert!(
                    panel.collection_meta_label().is_none(),
                    "the Schema view shows the count in its own toolbar"
                );
            });
        });
    }
}
