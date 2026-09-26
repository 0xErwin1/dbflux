//! Document collections in the data grid.
//!
//! A collection on a `DatabaseCategory::Document` connection opens as a table
//! of its documents, flattened by [`columns`]: `_id` first, the page's field
//! order after it, objects expandable in place into column groups and arrays
//! that can be stepped into. The same page is also shown as a tree or as JSON.
//!
//! Drivers that report `DocumentFeatures::QUERY_SLOTS` get the four-slot query bar
//! (filter, project, sort, limit), field-path completion and the Schema view,
//! all fed by `Connection::sample_collection_schema`. Drivers that advertise
//! `DocumentFeatures::FIELD_PATCH` get per-field edits committed as field patches (and
//! whole-document replacement from the JSON view), each checked against the
//! server copy before it is written.

pub(super) mod aggregate;
pub(super) mod columns;
pub(super) mod completion;
pub(super) mod inspector;
mod render;

use std::cell::RefCell;
use std::collections::{BTreeSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;

use dbflux_components::components::data_table::model::{
    CellValue, ColumnKind, ColumnSpec, RowData,
};
use dbflux_components::components::data_table::{
    ColumnGroupHeader, DocumentColumnHeader, DocumentPresentation, TableModel,
};
use dbflux_components::components::document_tree::NodeId;
use dbflux_components::controls::{Dropdown, DropdownItem, DropdownSelectionChanged, InputEvent};
use dbflux_core::{
    CollectionCountEstimate, CollectionSchemaRequest, CollectionSchemaSample, DatabaseCategory,
    DocumentFeatures, DocumentFetchRequest, DocumentIdentity, DocumentPatch, DocumentPatchRequest,
    DocumentReplaceRequest, DocumentServerState, FieldChange, FieldPath, QueryResult, ServerChange,
    TaskKind, Value, assess_server_change, coerce_edited_value, document_json_to_value,
    field_path_to_dotted, value_at_path, value_to_document_json,
};
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::*;
use gpui_component::input::EditorState;

use self::columns::{FlatCell, FlatView, StepTarget};
use super::{DataGridPanel, DataSource};
use crate::data_view::DataViewMode;

/// Documents a fresh schema sample reads.
pub(super) const DEFAULT_SAMPLE_SIZE: u32 = 1_000;

/// Sample sizes offered in the Schema view.
pub(super) const SAMPLE_SIZES: [u32; 4] = [100, 500, 1_000, 5_000];

/// Query history entries kept per collection tab.
const HISTORY_LIMIT: usize = 20;

/// Documents, Schema or Aggregate, the views of a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) enum CollectionTab {
    #[default]
    Documents,
    Schema,
    Aggregate,
}

/// The collection views a driver's document features offer, in header
/// order: Documents always, Schema with the query slots, Aggregate with
/// aggregation pipelines.
pub(super) fn available_collection_tabs(features: DocumentFeatures) -> Vec<CollectionTab> {
    let mut tabs = vec![CollectionTab::Documents];

    if features.contains(DocumentFeatures::QUERY_SLOTS) {
        tabs.push(CollectionTab::Schema);
    }

    if features.contains(DocumentFeatures::AGGREGATE) {
        tabs.push(CollectionTab::Aggregate);
    }

    tabs
}

/// Progress of the schema sample.
#[derive(Debug, Clone, PartialEq, Default)]
pub(super) enum SchemaLoad {
    #[default]
    Idle,
    Loading,
    Failed(String),
}

/// One query the user ran, for the history menu.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct QueryHistoryEntry {
    pub filter: String,
    pub projection: String,
    pub sort: String,
    pub limit: String,
}

impl QueryHistoryEntry {
    /// One-line description for the history menu.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();

        for (keyword, text) in [
            ("filter", &self.filter),
            ("project", &self.projection),
            ("sort", &self.sort),
        ] {
            if !text.trim().is_empty() {
                parts.push(format!("{keyword} {}", super::utils::single_line(text)));
            }
        }

        if !self.limit.trim().is_empty() {
            parts.push(format!("limit {}", self.limit.trim()));
        }

        parts.join("  ")
    }
}

/// What a commit writes to one document.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum DocumentEdit {
    Patch(DocumentPatch),
    Replace(Value),
}

impl DocumentEdit {
    fn touched_paths(&self) -> Vec<FieldPath> {
        match self {
            DocumentEdit::Patch(patch) => patch.touched_paths(),
            DocumentEdit::Replace(_) => Vec::new(),
        }
    }

    fn replaces_whole_document(&self) -> bool {
        matches!(self, DocumentEdit::Replace(_))
    }
}

/// A queued write to one document.
#[derive(Debug, Clone)]
pub(super) struct DocumentCommit {
    identity: DocumentIdentity,
    loaded: Value,
    edit: DocumentEdit,
    /// Grid rows whose staged edits this commit carries.
    rows: Vec<usize>,
    /// A tree edit: the node and its new value, applied in place once the
    /// write lands, so the tree keeps its expansion instead of reloading.
    tree_edit: Option<(usize, NodeId, Value)>,
}

/// The grid edits staged on one document, before they become a patch.
struct StagedDocumentChanges {
    document: usize,
    changes: Vec<(FieldPath, FieldChange)>,
    rows: Vec<usize>,
}

/// A commit held back because the document changed on the server.
pub(super) struct PendingConflict {
    pub label: String,
    pub change: ServerChange,
    pub edit_paths: Vec<FieldPath>,
    pub replaces_whole_document: bool,
    /// Native text of the write that "Apply my change" sends.
    pub preview: Option<String>,
    commit: DocumentCommit,
}

/// Documents changed in the JSON view, or why the text cannot be committed.
#[derive(Debug, Clone, PartialEq, Default)]
pub(super) struct JsonDraft {
    pub changed: usize,
    pub error: Option<String>,
}

/// State of a document collection shown by the grid.
pub(super) struct CollectionViewState {
    /// Page as the driver returned it.
    pub raw: Option<QueryResult>,
    /// The page's documents, indexed like the driver's rows.
    pub documents: Vec<Value>,
    /// Field order of the page.
    pub top_order: Vec<String>,
    /// Expanded object columns (dotted paths) of the view shown now.
    pub expanded: BTreeSet<String>,
    /// Nested values stepped into, outermost first.
    pub steps: Vec<StepTarget>,
    pub flat: FlatView,
    pub tab: CollectionTab,
    pub schema: Option<CollectionSchemaSample>,
    pub schema_load: SchemaLoad,
    pub sample_size: u32,
    pub sample_dropdown: Entity<Dropdown>,
    /// Field paths offered by the query bar completion.
    pub field_paths: Rc<RefCell<Vec<String>>>,
    pub projection_input: Entity<EditorState>,
    pub sort_input: Entity<EditorState>,
    pub history: Vec<QueryHistoryEntry>,
    pub history_open: bool,
    pub count: Option<CollectionCountEstimate>,
    /// Filter the count belongs to, so a new filter recounts.
    pub counted_filter: Option<Option<serde_json::Value>>,
    pub conflict: Option<PendingConflict>,
    queue: VecDeque<DocumentCommit>,
    pub committing: bool,
    committed: usize,
    /// A landed commit needs the page reloaded to show it.
    reload_after_commit: bool,
    pub json_editor: Entity<EditorState>,
    pub json_draft: JsonDraft,
    /// JSON text the editor was last loaded with.
    json_baseline: String,
    /// The Aggregate view, held apart from the Documents page.
    pub aggregate: aggregate::AggregateViewState,
    /// Refreshes the Document panel's pending-edit highlight when the grid's
    /// staged edits change.
    inspector_edits_observation: Option<Subscription>,
    _subscriptions: Vec<Subscription>,
}

impl CollectionViewState {
    pub fn new(window: &mut Window, cx: &mut Context<DataGridPanel>) -> Self {
        let field_paths = Rc::new(RefCell::new(Vec::new()));

        let make_slot =
            |placeholder: &str, window: &mut Window, cx: &mut Context<DataGridPanel>| {
                let provider: Rc<dyn dbflux_components::controls::CompletionProvider> = Rc::new(
                    completion::DocumentFieldCompletionProvider::new(field_paths.clone()),
                );
                let placeholder = placeholder.to_string();
                cx.new(|cx| {
                    let mut state = crate::completion_support::new_single_line_completion_state(
                        window,
                        cx,
                        placeholder,
                    );
                    state.lsp_mut().completion_provider = Some(provider);
                    state
                })
            };

        let projection_input = make_slot("{ }", window, cx);
        let sort_input = make_slot("{ }", window, cx);

        let json_editor = cx.new(|cx| {
            EditorState::new(window, cx)
                .language("json")
                .line_number(true)
        });

        let sample_dropdown = cx.new(|_cx| {
            let items = SAMPLE_SIZES
                .iter()
                .map(|size| DropdownItem::new(crate::labels::collection_sample_size_label(*size)))
                .collect();
            let selected = SAMPLE_SIZES
                .iter()
                .position(|size| *size == DEFAULT_SAMPLE_SIZE);

            Dropdown::new("collection-sample-size")
                .items(items)
                .selected_index(selected)
        });

        let mut subscriptions = Vec::new();

        for slot in [projection_input.clone(), sort_input.clone()] {
            subscriptions.push(cx.subscribe_in(
                &slot,
                window,
                |this, _, event: &InputEvent, window, cx| {
                    if let InputEvent::PressEnter { .. } = event {
                        this.find_documents(window, cx);
                    }
                },
            ));
        }

        subscriptions.push(
            cx.subscribe(&json_editor, |this, _, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    this.refresh_json_draft(cx);
                }
            }),
        );

        subscriptions.push(cx.subscribe_in(
            &sample_dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, _window, cx| {
                if let Some(size) = SAMPLE_SIZES.get(event.index) {
                    this.collection.sample_size = *size;
                    this.load_collection_schema(cx);
                }
            },
        ));

        Self {
            raw: None,
            documents: Vec::new(),
            top_order: Vec::new(),
            expanded: BTreeSet::new(),
            steps: Vec::new(),
            flat: FlatView::default(),
            tab: CollectionTab::Documents,
            schema: None,
            schema_load: SchemaLoad::Idle,
            sample_size: DEFAULT_SAMPLE_SIZE,
            sample_dropdown,
            field_paths,
            projection_input,
            sort_input,
            history: Vec::new(),
            history_open: false,
            count: None,
            counted_filter: None,
            conflict: None,
            queue: VecDeque::new(),
            committing: false,
            committed: 0,
            reload_after_commit: false,
            json_editor,
            json_draft: JsonDraft::default(),
            json_baseline: String::new(),
            aggregate: aggregate::AggregateViewState::new(window, cx),
            inspector_edits_observation: None,
            _subscriptions: subscriptions,
        }
    }

    pub fn current_step(&self) -> Option<&StepTarget> {
        self.steps.last()
    }
}

/// A copy of `document` without top-level nulls. The grid cannot tell a null
/// field from a missing one at the top level (the driver fills absent fields
/// with null), so both copies are compared without them.
fn without_top_level_nulls(document: &Value) -> Value {
    match document {
        Value::Document(fields) => Value::Document(
            fields
                .iter()
                .filter(|(_, value)| !value.is_null())
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Short name of a document for messages: its first short text field, or its
/// identity.
fn document_label(document: &Value, order: &[String], identity: &DocumentIdentity) -> String {
    if let Value::Document(fields) = document {
        let first_text = order
            .iter()
            .filter(|key| key.as_str() != "_id")
            .filter_map(|key| fields.get(key))
            .find_map(|value| match value {
                Value::Text(text) if !text.is_empty() && text.chars().count() <= 40 => {
                    Some(text.clone())
                }
                _ => None,
            });

        if let Some(text) = first_text {
            return text;
        }
    }

    identity
        .iter()
        .map(|(_, value)| value.as_display_string_truncated(24))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Table model for flattened documents: missing fields and nested values get
/// their own cells.
pub(super) fn flat_table_model(flat: &FlatView) -> TableModel {
    let columns = flat
        .columns
        .iter()
        .map(|column| ColumnSpec {
            id: column.dotted().into(),
            title: column.label.as_str().into(),
            kind: match column.type_label.as_str() {
                "int" => ColumnKind::Integer,
                "dbl" | "dec" => ColumnKind::Float,
                "bool" => ColumnKind::Bool,
                "obj" | "arr" => ColumnKind::Json,
                _ => ColumnKind::Text,
            },
            align: TextAlign::Left,
            type_name: column.type_label.as_str().into(),
        })
        .collect();

    let rows = flat
        .rows
        .iter()
        .map(|row| RowData {
            cells: row
                .cells
                .iter()
                .map(|cell| match cell {
                    FlatCell::Missing => CellValue::missing(),
                    FlatCell::Value(Value::Array(items)) => CellValue::nested(true, items.len()),
                    FlatCell::Value(Value::Document(fields)) => {
                        CellValue::nested(false, fields.len())
                    }
                    FlatCell::Value(value) => CellValue::from(value),
                })
                .collect(),
        })
        .collect();

    TableModel::new(columns, rows)
}

impl DataGridPanel {
    // === Capabilities ===

    fn collection_capabilities(&self, cx: &App) -> Option<(DatabaseCategory, DocumentFeatures)> {
        let DataSource::Collection { profile_id, .. } = &self.source else {
            return None;
        };

        self.app_state
            .read(cx)
            .connections()
            .get(profile_id)
            .map(|connected| {
                let connection = &connected.connection;
                (
                    connection.metadata().category,
                    connection.document_features(),
                )
            })
    }

    /// Whether this grid shows a document collection.
    pub(super) fn is_document_collection(&self, cx: &App) -> bool {
        self.collection_capabilities(cx)
            .is_some_and(|(category, _)| category == DatabaseCategory::Document)
    }

    /// Whether the driver offers the four-slot query bar and the schema sample.
    pub(super) fn has_document_query_slots(&self, cx: &App) -> bool {
        self.collection_capabilities(cx)
            .is_some_and(|(category, capabilities)| {
                category == DatabaseCategory::Document
                    && capabilities.contains(DocumentFeatures::QUERY_SLOTS)
            })
    }

    /// Whether edits are committed as field patches with the server check.
    pub(super) fn has_document_field_patch(&self, cx: &App) -> bool {
        self.collection_capabilities(cx)
            .is_some_and(|(category, capabilities)| {
                category == DatabaseCategory::Document
                    && capabilities.contains(DocumentFeatures::FIELD_PATCH)
            })
    }

    /// Views offered for the source: Tree, Table and JSON for a document
    /// collection, the source's defaults otherwise.
    pub(super) fn available_view_modes(&self, cx: &App) -> Vec<DataViewMode> {
        if self.is_document_collection(cx) {
            vec![
                DataViewMode::Document,
                DataViewMode::Table,
                DataViewMode::Json,
            ]
        } else {
            DataViewMode::available_for(&self.source)
        }
    }

    /// Switches between Tree, Table and JSON. Refused while edits made in the
    /// current view are staged, since the other views cannot show them.
    pub(super) fn set_document_view_mode(&mut self, mode: DataViewMode, cx: &mut Context<Self>) {
        if self.view_config.mode == mode || !self.available_view_modes(cx).contains(&mode) {
            return;
        }

        if self.has_uncommitted_document_edits(cx) {
            Toast::warning(crate::labels::grid_reload_blocked_by_pending_edits())
                .meta_right(now_hms())
                .push(cx);
            return;
        }

        self.view_config.mode = mode;
        if mode == DataViewMode::Json {
            self.load_json_editor(cx);
        }
        cx.notify();
    }

    /// Next view in Tree, Table, JSON order.
    pub(super) fn cycle_document_view(&mut self, cx: &mut Context<Self>) {
        let modes = self.available_view_modes(cx);
        if modes.len() <= 1 {
            return;
        }

        let current = modes
            .iter()
            .position(|mode| *mode == self.view_config.mode)
            .unwrap_or(0);
        let next = modes
            .get((current + 1) % modes.len())
            .copied()
            .unwrap_or(DataViewMode::Table);

        self.set_document_view_mode(next, cx);
    }

    /// The collection views this connection offers.
    pub(super) fn collection_tabs(&self, cx: &App) -> Vec<CollectionTab> {
        match self.collection_capabilities(cx) {
            Some((DatabaseCategory::Document, features)) => available_collection_tabs(features),
            _ => vec![CollectionTab::Documents],
        }
    }

    pub(super) fn set_collection_tab(&mut self, tab: CollectionTab, cx: &mut Context<Self>) {
        if self.collection.tab == tab || !self.collection_tabs(cx).contains(&tab) {
            return;
        }

        self.collection.tab = tab;
        if tab == CollectionTab::Schema && self.collection.schema.is_none() {
            self.load_collection_schema(cx);
        }
        cx.notify();
    }

    // === Page ===

    /// Keeps the driver's page and replaces `self.result` with its flattened
    /// view. Returns `false` when the source is not a document collection and
    /// the page is used as is.
    pub(super) fn apply_document_page(
        &mut self,
        result: &QueryResult,
        cx: &mut Context<Self>,
    ) -> bool {
        if !self.is_document_collection(cx) {
            self.collection.raw = None;
            return false;
        }

        self.collection.documents = columns::documents_from_result(result);
        self.collection.top_order = result
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect();
        self.collection.raw = Some(result.clone());

        // Expanded groups name paths of the documents, so they survive a new
        // page; paths relative to a nested value do not.
        if !self.collection.steps.is_empty() {
            self.collection.steps.clear();
            self.collection.expanded.clear();
        }

        self.collection.conflict = None;
        self.collection.queue.clear();
        self.collection.committing = false;

        self.result = self.flattened_document_result();

        if self.view_config.mode == DataViewMode::Json {
            self.load_json_editor(cx);
        }

        if self.has_document_query_slots(cx)
            && self.collection.schema.is_none()
            && self.collection.schema_load == SchemaLoad::Idle
        {
            self.load_collection_schema(cx);
        }

        true
    }

    fn identity_columns(&self) -> Vec<String> {
        let marked: Vec<String> = self
            .collection
            .raw
            .as_ref()
            .map(|raw| {
                raw.columns
                    .iter()
                    .filter(|column| column.is_primary_key)
                    .map(|column| column.name.clone())
                    .collect()
            })
            .unwrap_or_default();

        if marked.is_empty() {
            vec!["_id".to_string()]
        } else {
            marked
        }
    }

    /// Flattens the page for the view shown now.
    fn flattened_document_result(&mut self) -> QueryResult {
        let step = self.collection.steps.last().cloned();
        self.collection.flat = columns::flatten(
            &self.collection.documents,
            &self.collection.top_order,
            &self.collection.expanded,
            step.as_ref(),
        );

        let identity = if step.is_none() {
            self.identity_columns()
        } else {
            Vec::new()
        };

        match &self.collection.raw {
            Some(raw) => self.collection.flat.to_query_result(raw, &identity),
            None => QueryResult::empty(),
        }
    }

    /// Rebuilds the grid after the expansion or the step changed.
    fn reflatten_documents(&mut self, cx: &mut Context<Self>) {
        self.result = self.flattened_document_result();
        self.grid_table.reload = super::TableReload::NewColumns;
        self.rebuild_table(None, cx);
        cx.notify();
    }

    /// Table model for a document grid: missing fields and nested values get
    /// their own cells. `None` when the grid does not show documents.
    pub(super) fn document_table_model(&self) -> Option<TableModel> {
        self.collection.raw.as_ref()?;
        Some(flat_table_model(&self.collection.flat))
    }

    /// Header extras for the document grid: groups and presence from the
    /// schema sample.
    pub(super) fn document_presentation(&self) -> Option<DocumentPresentation> {
        self.collection.raw.as_ref()?;

        let prefix: Vec<String> = self
            .collection
            .current_step()
            .map(|step| step.path.clone())
            .unwrap_or_default();

        let headers = self
            .collection
            .flat
            .columns
            .iter()
            .map(|column| {
                let group = column.group.as_ref().map(|key| ColumnGroupHeader {
                    key: key.as_str().into(),
                    label: key.as_str().into(),
                    type_label: "obj".into(),
                });

                let presence = self.collection.schema.as_ref().and_then(|schema| {
                    if !prefix.is_empty() {
                        return None;
                    }
                    schema
                        .field(&column.dotted())
                        .map(|field| field.presence_ratio(schema.sampled_documents))
                });

                DocumentColumnHeader { group, presence }
            })
            .collect();

        Some(DocumentPresentation { headers })
    }

    /// Whether the grid is inside a nested value.
    pub(super) fn is_stepped_into(&self) -> bool {
        !self.collection.steps.is_empty()
    }

    // === Expand, step in, step out ===

    /// `e` on a column: expands an object column into a group, or collapses
    /// the group the column belongs to.
    pub(super) fn toggle_document_column_group(&mut self, col: usize, cx: &mut Context<Self>) {
        let Some(column) = self.collection.flat.columns.get(col).cloned() else {
            return;
        };

        let key = match (&column.group, column.type_label.as_str()) {
            (Some(group), _) => group.clone(),
            (None, "obj") => column.dotted(),
            _ => return,
        };

        if self.reload_blocked_by_pending_edits(cx) {
            return;
        }

        if !self.collection.expanded.remove(&key) {
            self.collection.expanded.insert(key);
        }

        self.reflatten_documents(cx);
    }

    /// Enter on a nested value: shows its contents as rows.
    pub(super) fn step_into_document_value(
        &mut self,
        row: usize,
        col: usize,
        cx: &mut Context<Self>,
    ) {
        let (Some(flat_row), Some(column)) = (
            self.collection.flat.rows.get(row),
            self.collection.flat.columns.get(col),
        ) else {
            return;
        };

        if self.reload_blocked_by_pending_edits(cx) {
            return;
        }

        let mut path = flat_row.base_path.clone();
        path.extend(column.path.iter().cloned());

        self.collection.steps.push(StepTarget {
            document: flat_row.document,
            path,
        });
        self.collection.expanded.clear();
        self.reflatten_documents(cx);
    }

    /// Backspace: back to the enclosing level.
    pub(super) fn step_out_of_document_value(&mut self, cx: &mut Context<Self>) {
        if self.collection.steps.is_empty() || self.reload_blocked_by_pending_edits(cx) {
            return;
        }

        self.collection.steps.pop();
        self.collection.expanded.clear();
        self.reflatten_documents(cx);
    }

    /// Jumps back to breadcrumb level `depth` (0 = the documents).
    pub(super) fn step_to_depth(&mut self, depth: usize, cx: &mut Context<Self>) {
        if depth >= self.collection.steps.len() || self.reload_blocked_by_pending_edits(cx) {
            return;
        }

        self.collection.steps.truncate(depth);
        self.collection.expanded.clear();
        self.reflatten_documents(cx);
    }

    /// Breadcrumb labels of the steps: the collection, then for each step the
    /// document it entered (first step only) and the path segments.
    pub(super) fn document_step_labels(&self) -> Vec<String> {
        let mut labels = Vec::new();

        for (depth, step) in self.collection.steps.iter().enumerate() {
            let previous_len = depth
                .checked_sub(1)
                .and_then(|previous| self.collection.steps.get(previous))
                .map_or(0, |previous| previous.path.len());

            let mut segment = String::new();
            if depth == 0 {
                let identity = self.document_identity(step.document).unwrap_or_default();
                let document = self.collection.documents.get(step.document);
                segment = document.map_or_else(String::new, |document| {
                    document_label(document, &self.collection.top_order, &identity)
                });
                segment.push_str(" \u{203a} ");
            }

            let relative: Vec<String> = step.path.iter().skip(previous_len).cloned().collect();
            segment.push_str(&field_path_to_dotted(&relative));
            labels.push(segment);
        }

        labels
    }

    // === Query bar ===

    /// Runs the query in the slots (Find, Ctrl+Enter) and records it.
    pub(super) fn find_documents(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.reload_blocked_by_pending_edits(cx) {
            return;
        }

        let entry = self.current_query_entry(cx);
        if !entry.summary().is_empty() {
            self.collection
                .history
                .retain(|existing| *existing != entry);
            self.collection.history.insert(0, entry);
            self.collection.history.truncate(HISTORY_LIMIT);
        }

        self.collection.history_open = false;
        self.grid_table.reload = super::TableReload::ResetRows;
        if let DataSource::Collection { pagination, .. } = &mut self.source {
            *pagination = pagination.clone().reset_offset();
        }
        self.refresh(window, cx);
        self.focus_table(window, cx);
    }

    fn current_query_entry(&self, cx: &App) -> QueryHistoryEntry {
        QueryHistoryEntry {
            filter: self.filter_bar.filter_input.read(cx).value().to_string(),
            projection: self
                .collection
                .projection_input
                .read(cx)
                .value()
                .to_string(),
            sort: self.collection.sort_input.read(cx).value().to_string(),
            limit: self.filter_bar.limit_input.read(cx).value().to_string(),
        }
    }

    /// Restores a history entry into the slots and runs it.
    pub(super) fn run_history_entry(
        &mut self,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(entry) = self.collection.history.get(index).cloned() else {
            return;
        };

        self.filter_bar
            .filter_input
            .update(cx, |input, cx| input.set_value(&entry.filter, window, cx));
        self.collection.projection_input.update(cx, |input, cx| {
            input.set_value(&entry.projection, window, cx)
        });
        self.collection
            .sort_input
            .update(cx, |input, cx| input.set_value(&entry.sort, window, cx));
        self.filter_bar
            .limit_input
            .update(cx, |input, cx| input.set_value(&entry.limit, window, cx));

        self.find_documents(window, cx);
    }

    /// Parses one slot as relaxed JSON. An empty slot is `Ok(None)`.
    pub(super) fn parse_query_slot(
        text: &str,
        slot: &str,
        cx: &mut App,
    ) -> Result<Option<serde_json::Value>, ()> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }

        match dbflux_core::parse_relaxed_json(trimmed) {
            Ok(json) if json.is_object() => Ok(Some(json)),
            Ok(_) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::User,
                        crate::labels::collection_slot_not_object(slot),
                    ),
                    cx,
                );
                Err(())
            }
            Err(error) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::User,
                        crate::labels::collection_slot_invalid(slot, &error.to_string()),
                    ),
                    cx,
                );
                Err(())
            }
        }
    }

    /// Projection and sort documents from their slots, or `Err` after the
    /// parse error was reported.
    pub(super) fn document_query_options(
        &self,
        cx: &mut App,
    ) -> Result<(Option<serde_json::Value>, Option<serde_json::Value>), ()> {
        let projection_text = self
            .collection
            .projection_input
            .read(cx)
            .value()
            .to_string();
        let sort_text = self.collection.sort_input.read(cx).value().to_string();

        let projection = Self::parse_query_slot(&projection_text, "project", cx)?;
        let sort = Self::parse_query_slot(&sort_text, "sort", cx)?;

        Ok((projection, sort))
    }

    /// Adds `path: value` to the filter slot and runs it (clicking a value in
    /// the Schema view).
    pub(super) fn add_value_to_filter(
        &mut self,
        path: &str,
        value: &Value,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let current = self.filter_bar.filter_input.read(cx).value().to_string();

        let mut filter = match dbflux_core::parse_relaxed_json(current.trim()) {
            Ok(serde_json::Value::Object(map)) => map,
            _ => serde_json::Map::new(),
        };
        filter.insert(path.to_string(), value_to_document_json(value));

        let text = serde_json::Value::Object(filter).to_string();
        self.filter_bar
            .filter_input
            .update(cx, |input, cx| input.set_value(&text, window, cx));

        self.collection.tab = CollectionTab::Documents;
        self.find_documents(window, cx);
    }

    // === Count ===

    /// Asks for a (possibly estimated) count when the filter differs from the
    /// one last counted. Paging keeps the count.
    pub(super) fn refresh_document_count(
        &mut self,
        filter: Option<serde_json::Value>,
        cx: &mut Context<Self>,
    ) {
        if self.collection.counted_filter.as_ref() == Some(&filter) {
            return;
        }

        let DataSource::Collection {
            profile_id,
            collection,
            ..
        } = &self.source
        else {
            return;
        };

        let Some(connection) = self
            .app_state
            .read(cx)
            .connections()
            .get(profile_id)
            .map(|connected| connected.connection.clone())
        else {
            return;
        };

        self.collection.counted_filter = Some(filter.clone());
        self.collection.count = None;

        let mut request = dbflux_core::CollectionCountRequest::new(collection.clone());
        if let Some(filter) = filter {
            request = request.with_filter(filter);
        }

        let qualified = collection.qualified_name();
        let entity = cx.entity().clone();
        let task = cx
            .background_executor()
            .spawn(async move { connection.estimate_collection_count(&request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| match result {
                    Ok(estimate) => {
                        panel.collection.count = Some(estimate);
                        panel.pending.total_count = Some(super::PendingTotalCount {
                            source_qualified: qualified,
                            total: estimate.count,
                        });
                        cx.notify();
                    }
                    Err(error) => {
                        log::warn!("Collection count failed: {error}");
                    }
                });
            });
        })
        .detach();
    }

    // === Schema ===

    /// Samples the collection's schema with the chosen sample size and the
    /// filter in the query bar.
    pub(super) fn load_collection_schema(&mut self, cx: &mut Context<Self>) {
        let DataSource::Collection {
            profile_id,
            collection,
            ..
        } = &self.source
        else {
            return;
        };

        let Some(connection) = self
            .app_state
            .read(cx)
            .connections()
            .get(profile_id)
            .map(|connected| connected.connection.clone())
        else {
            return;
        };

        let filter_text = self.filter_bar.filter_input.read(cx).value().to_string();
        let filter = dbflux_core::parse_relaxed_json(filter_text.trim())
            .ok()
            .filter(serde_json::Value::is_object);

        let mut request =
            CollectionSchemaRequest::new(collection.clone(), self.collection.sample_size);
        request.filter = filter;

        self.collection.schema_load = SchemaLoad::Loading;
        cx.notify();

        let entity = cx.entity().clone();
        let task = cx
            .background_executor()
            .spawn(async move { connection.sample_collection_schema(&request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            match result {
                Ok(sample) => {
                    cx.update(|cx| {
                        entity.update(cx, |panel, cx| {
                            panel.apply_schema_sample(sample, cx);
                        });
                    });
                }
                Err(error) => {
                    let message = error.to_string();
                    report_error_async(
                        UserFacingError::new(
                            ErrorKind::Driver,
                            crate::labels::collection_schema_failed(&message),
                        ),
                        cx,
                    );
                    cx.update(|cx| {
                        entity.update(cx, |panel, cx| {
                            panel.collection.schema_load = SchemaLoad::Failed(message);
                            cx.notify();
                        });
                    });
                }
            }
        })
        .detach();
    }

    fn apply_schema_sample(&mut self, sample: CollectionSchemaSample, cx: &mut Context<Self>) {
        *self.collection.field_paths.borrow_mut() = sample.field_paths();
        self.collection.schema = Some(sample);
        self.collection.schema_load = SchemaLoad::Idle;

        if let Some(presentation) = self.document_presentation()
            && let Some(table_state) = &self.grid_table.table_state
        {
            table_state.update(cx, |state, cx| {
                state.set_document_presentation(Some(presentation), cx);
            });
        }

        cx.notify();
    }

    // === Edits ===

    /// Number of staged field edits in the table, or changed documents in the
    /// JSON view.
    pub(super) fn pending_document_edit_count(&self, cx: &App) -> usize {
        if self.view_config.mode == DataViewMode::Json {
            return self.collection.json_draft.changed;
        }

        self.grid_table
            .table_state
            .as_ref()
            .map(|table_state| {
                let buffer = table_state.read(cx).edit_buffer();
                buffer
                    .dirty_rows()
                    .into_iter()
                    .map(|row| buffer.row_changes(row).len())
                    .sum()
            })
            .unwrap_or(0)
    }

    /// The dotted path of the only staged edit, for the "Pending:" label.
    pub(super) fn single_pending_document_path(&self, cx: &App) -> Option<String> {
        if self.view_config.mode == DataViewMode::Json {
            return None;
        }

        let table_state = self.grid_table.table_state.as_ref()?;
        let buffer = table_state.read(cx).edit_buffer();
        let rows = buffer.dirty_rows();
        let [row] = rows.as_slice() else {
            return None;
        };
        let changes = buffer.row_changes(*row);
        let [(col, _)] = changes.as_slice() else {
            return None;
        };

        let flat_row = self.collection.flat.rows.get(*row)?;
        let column = self.collection.flat.columns.get(*col)?;
        let mut path = flat_row.base_path.clone();
        path.extend(column.path.iter().cloned());
        Some(field_path_to_dotted(&path))
    }

    fn has_uncommitted_document_edits(&self, cx: &App) -> bool {
        self.pending_document_edit_count(cx) > 0 || self.has_pending_edits(cx)
    }

    /// Whether commits go through the field-patch path instead of the
    /// generic row save.
    pub(super) fn commits_document_patches(&self, cx: &App) -> bool {
        self.collection.raw.is_some() && self.has_document_field_patch(cx)
    }

    /// Drops every staged edit of the current view.
    pub(super) fn revert_document_edits(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.view_config.mode == DataViewMode::Json {
            self.load_json_editor_with_window(window, cx);
            return;
        }

        if let Some(table_state) = &self.grid_table.table_state {
            table_state.update(cx, |state, cx| state.revert_all(cx));
        }
        cx.notify();
    }

    fn document_identity(&self, document: usize) -> Option<DocumentIdentity> {
        let Value::Document(fields) = self.collection.documents.get(document)? else {
            return None;
        };

        self.identity_columns()
            .into_iter()
            .map(|column| {
                fields
                    .get(&column)
                    .filter(|value| !value.is_null())
                    .map(|value| (column, value.clone()))
            })
            .collect()
    }

    fn new_commit(
        &self,
        document: usize,
        edit: DocumentEdit,
        rows: Vec<usize>,
        cx: &mut App,
    ) -> Option<DocumentCommit> {
        let Some(identity) = self.document_identity(document) else {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.data.mutation.error.save_document_unsupported_id"),
                ),
                cx,
            );
            return None;
        };

        let loaded = self.collection.documents.get(document)?.clone();

        Some(DocumentCommit {
            identity,
            loaded,
            edit,
            rows,
            tree_edit: None,
        })
    }

    /// Commit (Ctrl+S): turns the staged edits into one write per document and
    /// sends them one after another.
    pub(super) fn commit_document_edits(&mut self, cx: &mut Context<Self>) {
        if self.collection.committing || self.collection.conflict.is_some() {
            return;
        }

        let commits = if self.view_config.mode == DataViewMode::Json {
            self.json_commits(cx)
        } else {
            self.table_commits(cx)
        };

        let Some(commits) = commits else {
            return;
        };

        if commits.is_empty() {
            self.commit_pending_row_operations(cx);
            return;
        }

        self.collection.queue.extend(commits);
        self.collection.committing = true;
        self.collection.committed = 0;
        self.process_next_document_commit(cx);
    }

    /// Writes staged row deletes after the field edits, through the generic
    /// save path.
    fn commit_pending_row_operations(&mut self, cx: &mut Context<Self>) {
        let Some(table_state) = &self.grid_table.table_state else {
            return;
        };

        let (deletes, inserts) = {
            let buffer = table_state.read(cx).edit_buffer();
            (
                buffer.pending_delete_rows(),
                buffer
                    .pending_insert_rows()
                    .into_iter()
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>(),
            )
        };

        if !deletes.is_empty() || !inserts.is_empty() {
            self.handle_save_all(deletes, inserts, Vec::new(), cx);
        }
    }

    /// One commit per document from the grid's staged cells. `None` when an
    /// edited value does not parse as its field's type (already reported).
    fn table_commits(&mut self, cx: &mut Context<Self>) -> Option<Vec<DocumentCommit>> {
        let table_state = self.grid_table.table_state.clone()?;

        let staged: Vec<(usize, Vec<(usize, CellValue)>)> = {
            let buffer = table_state.read(cx).edit_buffer();
            buffer
                .dirty_rows()
                .into_iter()
                .map(|row| {
                    let changes = buffer
                        .row_changes(row)
                        .into_iter()
                        .map(|(col, cell)| (col, cell.clone()))
                        .collect();
                    (row, changes)
                })
                .collect()
        };

        let mut per_document: Vec<StagedDocumentChanges> = Vec::new();

        for (row, changes) in staged {
            let flat_row = self.collection.flat.rows.get(row)?.clone();

            for (col, cell) in changes {
                let column = self.collection.flat.columns.get(col)?;
                let original = flat_row.cells.get(col).and_then(FlatCell::value);

                let mut path = flat_row.base_path.clone();
                path.extend(column.path.iter().cloned());

                let change = if cell.is_missing() {
                    FieldChange::Unset
                } else if cell.is_null() {
                    FieldChange::Set(Value::Null)
                } else {
                    match coerce_edited_value(original, &cell.edit_text()) {
                        Ok(value) => FieldChange::Set(value),
                        Err(reason) => {
                            report_error(
                                UserFacingError::new(
                                    ErrorKind::User,
                                    crate::labels::collection_invalid_edit(
                                        &field_path_to_dotted(&path),
                                        &reason,
                                    ),
                                ),
                                cx,
                            );
                            return None;
                        }
                    }
                };

                match per_document
                    .iter_mut()
                    .find(|staged| staged.document == flat_row.document)
                {
                    Some(staged) => {
                        staged.changes.push((path, change));
                        if !staged.rows.contains(&row) {
                            staged.rows.push(row);
                        }
                    }
                    None => per_document.push(StagedDocumentChanges {
                        document: flat_row.document,
                        changes: vec![(path, change)],
                        rows: vec![row],
                    }),
                }
            }
        }

        let mut commits = Vec::new();
        for staged in per_document {
            let patch = DocumentPatch::from_changes(staged.changes);
            if patch.is_empty() {
                continue;
            }
            commits.push(self.new_commit(
                staged.document,
                DocumentEdit::Patch(patch),
                staged.rows,
                cx,
            )?);
        }

        Some(commits)
    }

    /// A tree edit is written at once, through the same checked path.
    pub(super) fn commit_tree_edit(
        &mut self,
        node_id: &NodeId,
        text: &str,
        cx: &mut Context<Self>,
    ) {
        if self.collection.committing || self.collection.conflict.is_some() {
            Toast::warning(dbflux_i18n::t!("document.collection.commit.busy"))
                .meta_right(now_hms())
                .push(cx);
            return;
        }

        let Some(document) = node_id.doc_index() else {
            return;
        };
        let path: FieldPath = node_id.path.iter().skip(1).cloned().collect();
        if path.is_empty() {
            return;
        }

        let original = self
            .collection
            .documents
            .get(document)
            .and_then(|loaded| value_at_path(loaded, &path));

        let value = match coerce_edited_value(original, text) {
            Ok(value) => value,
            Err(reason) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::User,
                        crate::labels::collection_invalid_edit(
                            &field_path_to_dotted(&path),
                            &reason,
                        ),
                    ),
                    cx,
                );
                return;
            }
        };

        let patch = DocumentPatch::from_changes(vec![(path, FieldChange::Set(value.clone()))]);
        let Some(mut commit) =
            self.new_commit(document, DocumentEdit::Patch(patch), Vec::new(), cx)
        else {
            return;
        };
        commit.tree_edit = Some((document, node_id.clone(), value));

        self.collection.queue.push_back(commit);
        self.collection.committing = true;
        self.collection.committed = 0;
        self.process_next_document_commit(cx);
    }

    fn process_next_document_commit(&mut self, cx: &mut Context<Self>) {
        let Some(commit) = self.collection.queue.pop_front() else {
            self.finish_document_commits(cx);
            return;
        };

        let DataSource::Collection {
            profile_id,
            collection,
            ..
        } = &self.source
        else {
            self.collection.committing = false;
            return;
        };

        let Some(connection) = self
            .app_state
            .read(cx)
            .connections()
            .get(profile_id)
            .map(|connected| connected.connection.clone())
        else {
            self.collection.committing = false;
            self.collection.queue.clear();
            report_error(
                UserFacingError::new(
                    ErrorKind::Network,
                    dbflux_i18n::t!("document.data.grid.error.connection_not_available"),
                ),
                cx,
            );
            return;
        };

        let request = DocumentFetchRequest {
            collection: collection.clone(),
            identity: commit.identity.clone(),
        };

        let entity = cx.entity().clone();
        let task = cx
            .background_executor()
            .spawn(async move { connection.fetch_document(&request) });

        cx.spawn(async move |_this, cx| {
            let fetched = task.await;

            match fetched {
                Ok(current) => {
                    cx.update(|cx| {
                        entity.update(cx, |panel, cx| {
                            panel.review_server_copy(commit, current, cx);
                        });
                    });
                }
                Err(error) => {
                    report_error_async(
                        UserFacingError::new(
                            ErrorKind::Driver,
                            dbflux_i18n::t!(
                                "document.data.mutation.error.update_document_failed",
                                error = error
                            ),
                        ),
                        cx,
                    );
                    cx.update(|cx| {
                        entity.update(cx, |panel, cx| {
                            panel.abort_document_commits(cx);
                        });
                    });
                }
            }
        })
        .detach();
    }

    /// Compares the server copy with the loaded one and either writes the
    /// commit or holds it for the user's decision.
    fn review_server_copy(
        &mut self,
        commit: DocumentCommit,
        current: Option<Value>,
        cx: &mut Context<Self>,
    ) {
        let loaded = without_top_level_nulls(&commit.loaded);
        let current = current.map(|document| without_top_level_nulls(&document));
        let edit_paths = commit.edit.touched_paths();

        match assess_server_change(
            &loaded,
            current.as_ref(),
            &edit_paths,
            commit.edit.replaces_whole_document(),
        ) {
            DocumentServerState::Unchanged => self.write_document_commit(commit, cx),
            DocumentServerState::Deleted => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::User,
                        dbflux_i18n::t!("document.collection.conflict.deleted"),
                    ),
                    cx,
                );
                self.abort_document_commits(cx);
            }
            DocumentServerState::Changed(change) => {
                let label =
                    document_label(&commit.loaded, &self.collection.top_order, &commit.identity);
                let preview = self.document_commit_preview(&commit, cx);

                self.collection.conflict = Some(PendingConflict {
                    label,
                    change,
                    edit_paths,
                    replaces_whole_document: commit.edit.replaces_whole_document(),
                    preview,
                    commit,
                });
                cx.notify();
            }
        }
    }

    /// Native text of the write a commit sends, from the driver's generator.
    fn document_commit_preview(&self, commit: &DocumentCommit, cx: &App) -> Option<String> {
        let DataSource::Collection {
            profile_id,
            collection,
            ..
        } = &self.source
        else {
            return None;
        };

        let state = self.app_state.read(cx);
        let connected = state.connections().get(profile_id)?;
        let generator = connected.connection.query_generator()?;

        let query = match &commit.edit {
            DocumentEdit::Patch(patch) => generator.document_patch_query(&DocumentPatchRequest {
                collection: collection.clone(),
                identity: commit.identity.clone(),
                patch: patch.clone(),
            }),
            DocumentEdit::Replace(document) => {
                generator.document_replace_query(&DocumentReplaceRequest {
                    collection: collection.clone(),
                    identity: commit.identity.clone(),
                    document: document.clone(),
                })
            }
        };

        query.map(|query| query.text)
    }

    /// "Apply my change": writes the held commit over the server copy.
    pub(super) fn apply_conflicting_commit(&mut self, cx: &mut Context<Self>) {
        let Some(conflict) = self.collection.conflict.take() else {
            return;
        };

        self.write_document_commit(conflict.commit, cx);
    }

    /// "Reload document": drops the held change and reloads the page, keeping
    /// the edits staged on other documents.
    pub(super) fn reload_conflicting_document(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(conflict) = self.collection.conflict.take() else {
            return;
        };

        self.collection.queue.clear();
        self.collection.committing = false;

        if let Some(table_state) = &self.grid_table.table_state {
            table_state.update(cx, |state, cx| {
                for row in &conflict.commit.rows {
                    state.edit_buffer_mut().clear_row(*row);
                }
                cx.notify();
            });
        }

        if self.view_config.mode == DataViewMode::Json {
            self.collection.json_draft = JsonDraft::default();
        }

        self.refresh_keeping_edits(window, cx);
    }

    fn write_document_commit(&mut self, commit: DocumentCommit, cx: &mut Context<Self>) {
        let DataSource::Collection {
            profile_id,
            collection,
            ..
        } = &self.source
        else {
            return;
        };

        let Some(connection) = self
            .app_state
            .read(cx)
            .connections()
            .get(profile_id)
            .map(|connected| connected.connection.clone())
        else {
            self.abort_document_commits(cx);
            return;
        };

        let collection = collection.clone();
        let (task_id, _cancel) = self.runner.start_mutation(
            TaskKind::Query,
            crate::labels::mutation_save_document_task_label(),
            cx,
        );

        if let Some(table_state) = &self.grid_table.table_state {
            table_state.update(cx, |state, cx| {
                for row in &commit.rows {
                    state
                        .edit_buffer_mut()
                        .set_row_state(*row, dbflux_core::RowState::Saving);
                }
                cx.notify();
            });
        }

        let edit = commit.edit.clone();
        let identity = commit.identity.clone();
        let entity = cx.entity().clone();

        let task = cx.background_executor().spawn(async move {
            match edit {
                DocumentEdit::Patch(patch) => connection.patch_document(&DocumentPatchRequest {
                    collection,
                    identity,
                    patch,
                }),
                DocumentEdit::Replace(document) => {
                    connection.replace_document(&DocumentReplaceRequest {
                        collection,
                        identity,
                        document,
                    })
                }
            }
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(error) = &result {
                report_error_async(
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

            cx.update(|cx| {
                entity.update(cx, |panel, cx| match result {
                    Ok(_) => {
                        panel.runner.complete_mutation(task_id, cx);
                        panel.collection.committed += 1;

                        match &commit.tree_edit {
                            Some((document, node_id, value)) => {
                                panel.apply_tree_edit_locally(*document, node_id, value, cx);
                            }
                            None => panel.collection.reload_after_commit = true,
                        }

                        if let Some(table_state) = &panel.grid_table.table_state {
                            table_state.update(cx, |state, cx| {
                                for row in &commit.rows {
                                    state.edit_buffer_mut().clear_row(*row);
                                }
                                cx.notify();
                            });
                        }

                        panel.process_next_document_commit(cx);
                    }
                    Err(error) => {
                        panel.runner.fail_mutation(task_id, error.to_string(), cx);

                        if let Some(table_state) = &panel.grid_table.table_state {
                            table_state.update(cx, |state, cx| {
                                for row in &commit.rows {
                                    state.edit_buffer_mut().set_row_state(
                                        *row,
                                        dbflux_core::RowState::Error(error.to_string()),
                                    );
                                }
                                cx.notify();
                            });
                        }

                        panel.abort_document_commits(cx);
                    }
                });
            });
        })
        .detach();
    }

    fn abort_document_commits(&mut self, cx: &mut Context<Self>) {
        self.collection.queue.clear();
        self.collection.committing = false;
        self.collection.committed = 0;

        if std::mem::take(&mut self.collection.reload_after_commit) {
            self.queue_reload_after_mutation(cx);
        }

        cx.notify();
    }

    /// Shows a landed tree edit without reloading: the page's copy of the
    /// document, the flattened grid and the tree node take the new value.
    fn apply_tree_edit_locally(
        &mut self,
        document: usize,
        node_id: &NodeId,
        value: &Value,
        cx: &mut Context<Self>,
    ) {
        let path: FieldPath = node_id.path.iter().skip(1).cloned().collect();

        if let Some(loaded) = self.collection.documents.get_mut(document) {
            dbflux_core::set_value_at_path(loaded, &path, value.clone());
        }

        if let (Some(raw), Some(top_level)) = (self.collection.raw.as_mut(), path.first())
            && let Some(column) = raw
                .columns
                .iter()
                .position(|column| &column.name == top_level)
            && let Some(cell) = raw
                .rows
                .get_mut(document)
                .and_then(|row| row.get_mut(column))
        {
            let mut wrapper =
                Value::Document([(top_level.clone(), cell.clone())].into_iter().collect());
            dbflux_core::set_value_at_path(&mut wrapper, &path, value.clone());
            if let Value::Document(mut fields) = wrapper
                && let Some(updated) = fields.remove(top_level)
            {
                *cell = updated;
            }
        }

        self.result = self.flattened_document_result();

        // The grid takes the new model in place: a full rebuild would also
        // rebuild the tree and drop its expansion.
        if let (Some(model), Some(table_state)) = (
            self.document_table_model(),
            self.grid_table.table_state.clone(),
        ) {
            table_state.update(cx, |state, cx| {
                state.set_model(
                    Arc::new(model),
                    dbflux_components::components::data_table::ModelSwap::KeepCursor,
                    cx,
                );
            });
        }

        if let Some(tree_state) = self.document_view.document_tree_state.clone() {
            tree_state.update(cx, |state, cx| {
                state.apply_inline_edit_value(node_id, value.clone(), cx);
            });
        }
    }

    fn finish_document_commits(&mut self, cx: &mut Context<Self>) {
        self.collection.committing = false;
        let committed = std::mem::take(&mut self.collection.committed);

        if committed > 0 {
            Toast::success(crate::labels::collection_committed_toast(committed))
                .meta_right(now_hms())
                .push(cx);
        }

        self.commit_pending_row_operations(cx);

        if std::mem::take(&mut self.collection.reload_after_commit) {
            self.collection.json_draft = JsonDraft::default();
            self.queue_reload_after_mutation(cx);
        }

        cx.notify();
    }

    // === Document preview ===

    // === Document panel ===

    /// Opens the Document panel on the document behind grid row `row`: the
    /// side panel a collection shows where a table shows the row inspector.
    ///
    /// Follows the row inspector's bookkeeping, so selection changes, tab
    /// activation and refreshes move it the same way. A row outside the
    /// loaded page closes it.
    pub(super) fn open_document_inspector(
        &mut self,
        row: usize,
        col: usize,
        cx: &mut Context<Self>,
    ) {
        use self::inspector::{DocumentInspectorContent, DocumentInspectorSnapshot};

        let document = self
            .collection
            .flat
            .rows
            .get(row)
            .map(|flat_row| flat_row.document)
            .and_then(|index| {
                self.collection
                    .documents
                    .get(index)
                    .map(|document| (index, document.clone()))
            });

        let Some((document_index, document)) = document else {
            self.inspector.follow_selection = false;
            self.inspector.pinned = false;
            self.inspector.inspector_row = None;
            self.inspector.document_inspector_content = None;
            self.inspector._document_inspector_subscription = None;
            cx.emit(super::DataGridEvent::CloseInspector);
            return;
        };

        let snapshot = DocumentInspectorSnapshot {
            document_index,
            document,
            field_order: self.collection.top_order.clone(),
            pending: self.staged_document_edits(document_index, cx),
        };

        let content = match &self.inspector.document_inspector_content {
            Some(existing) => {
                existing.update(cx, |content, cx| content.open(snapshot, cx));
                existing.clone()
            }
            None => {
                let content = cx.new(|cx| DocumentInspectorContent::new(snapshot, cx));
                self.inspector._document_inspector_subscription =
                    Some(cx.subscribe(&content, |this, content, event, cx| {
                        this.handle_document_inspector_event(content, *event, cx);
                    }));
                self.inspector.document_inspector_content = Some(content.clone());
                content
            }
        };

        self.inspector.follow_selection = true;
        self.inspector.inspector_row = Some((row, col));

        // The grid notifies when an edit is staged, reverted or committed;
        // the panel re-reads the staged edits of its document then.
        self.collection.inspector_edits_observation =
            self.grid_table.table_state.as_ref().map(|table_state| {
                cx.observe(table_state, |this, _, cx| {
                    this.refresh_document_inspector_edits(cx);
                })
            });

        cx.emit(super::DataGridEvent::OpenInspector {
            title: SharedString::from(dbflux_i18n::t!("document.collection.inspector.title")),
            content: AnyView::from(content),
            content_has_header: true,
        });
        cx.notify();
    }

    /// The grid edits staged on document `document` of the page, as the
    /// Document panel shows them: each edited field path with the value it
    /// takes on commit. A value that does not parse as its field's type is
    /// shown as typed; the commit reports it.
    pub(super) fn staged_document_edits(
        &self,
        document: usize,
        cx: &App,
    ) -> self::inspector::PendingFieldEdits {
        use self::inspector::PendingFieldEdit;

        let Some(table_state) = &self.grid_table.table_state else {
            return Vec::new();
        };
        let buffer = table_state.read(cx).edit_buffer();

        let mut edits = Vec::new();
        for row in buffer.dirty_rows() {
            let Some(flat_row) = self
                .collection
                .flat
                .rows
                .get(row)
                .filter(|flat_row| flat_row.document == document)
            else {
                continue;
            };

            for (col, cell) in buffer.row_changes(row) {
                let Some(column) = self.collection.flat.columns.get(col) else {
                    continue;
                };

                let mut path = flat_row.base_path.clone();
                path.extend(column.path.iter().cloned());

                let edit = if cell.is_missing() {
                    PendingFieldEdit::Unset
                } else if cell.is_null() {
                    PendingFieldEdit::Set(Value::Null)
                } else {
                    let original = flat_row.cells.get(col).and_then(FlatCell::value);
                    let text = cell.edit_text();
                    PendingFieldEdit::Set(
                        coerce_edited_value(original, &text).unwrap_or(Value::Text(text)),
                    )
                };

                edits.push((path, edit));
            }
        }

        edits
    }

    /// Shows the current staged edits in the open Document panel.
    fn refresh_document_inspector_edits(&mut self, cx: &mut Context<Self>) {
        let Some(content) = self.inspector.document_inspector_content.clone() else {
            return;
        };

        let document = content.read(cx).document_index();
        let pending = self.staged_document_edits(document, cx);
        content.update(cx, |content, cx| content.set_pending(pending, cx));
    }

    /// Carries out a request from the Document panel's buttons: close the
    /// panel, or open its document in the JSON editor.
    fn handle_document_inspector_event(
        &mut self,
        content: Entity<self::inspector::DocumentInspectorContent>,
        event: self::inspector::DocumentInspectorEvent,
        cx: &mut Context<Self>,
    ) {
        use self::inspector::DocumentInspectorEvent;

        match event {
            DocumentInspectorEvent::Close => {
                self.clear_inspector_state(cx);
                cx.emit(super::DataGridEvent::CloseInspector);
            }
            DocumentInspectorEvent::Expand => {
                let doc_index = content.read(cx).document_index();
                let Some(document) = self.collection.documents.get(doc_index) else {
                    return;
                };

                let document_json =
                    match serde_json::to_string_pretty(&value_to_document_json(document)) {
                        Ok(json) => json,
                        Err(error) => {
                            report_error(
                                UserFacingError::new(
                                    ErrorKind::User,
                                    crate::labels::collection_inspector_json_failed(
                                        &error.to_string(),
                                    ),
                                ),
                                cx,
                            );
                            return;
                        }
                    };

                self.pending.document_preview = Some(super::PendingDocumentPreview {
                    doc_index,
                    document_json,
                });
            }
        }

        cx.notify();
    }

    /// The loaded document `document` as document JSON for the preview
    /// editor, when edits go through field patches.
    pub(super) fn document_preview_json(&self, document: usize, cx: &App) -> Option<String> {
        if !self.commits_document_patches(cx) {
            return None;
        }

        let loaded = self.collection.documents.get(document)?;
        serde_json::to_string_pretty(&value_to_document_json(&without_top_level_nulls(loaded))).ok()
    }

    /// Saves the document preview: the edited JSON is diffed against the
    /// loaded document and written as a minimal field patch, after the same
    /// server-change check as a grid commit.
    pub(super) fn commit_document_preview_edit(
        &mut self,
        document: usize,
        text: &str,
        cx: &mut Context<Self>,
    ) {
        if self.collection.committing || self.collection.conflict.is_some() {
            Toast::warning(dbflux_i18n::t!("document.collection.commit.busy"))
                .meta_right(now_hms())
                .push(cx);
            return;
        }

        let edited = match serde_json::from_str::<serde_json::Value>(text) {
            Ok(json) => document_json_to_value(&json),
            Err(error) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::User,
                        crate::labels::collection_json_invalid(&error.to_string()),
                    ),
                    cx,
                );
                return;
            }
        };

        let Some(loaded) = self
            .collection
            .documents
            .get(document)
            .map(without_top_level_nulls)
        else {
            return;
        };

        let (Value::Document(edited_fields), Value::Document(loaded_fields)) = (&edited, &loaded)
        else {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.collection.json.not_document"),
                ),
                cx,
            );
            return;
        };

        let identity_kept = self
            .identity_columns()
            .iter()
            .all(|column| edited_fields.get(column) == loaded_fields.get(column));
        if !identity_kept {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.collection.json.identity_changed"),
                ),
                cx,
            );
            return;
        }

        let patch = DocumentPatch::diff(&loaded, &edited);
        if patch.is_empty() {
            return;
        }

        let Some(commit) = self.new_commit(document, DocumentEdit::Patch(patch), Vec::new(), cx)
        else {
            return;
        };

        self.collection.queue.push_back(commit);
        self.collection.committing = true;
        self.collection.committed = 0;
        self.process_next_document_commit(cx);
    }

    // === JSON view ===

    fn page_json_text(&self) -> String {
        let documents: Vec<serde_json::Value> = self
            .collection
            .documents
            .iter()
            .map(|document| value_to_document_json(&without_top_level_nulls(document)))
            .collect();

        serde_json::to_string_pretty(&documents).unwrap_or_default()
    }

    /// Loads the page into the JSON editor on the next frame (the editor
    /// needs a window to change its text).
    fn load_json_editor(&mut self, cx: &mut Context<Self>) {
        self.collection.json_baseline = self.page_json_text();
        self.collection.json_draft = JsonDraft::default();
        self.pending.json_reload = true;
        cx.notify();
    }

    fn load_json_editor_with_window(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.collection.json_baseline = self.page_json_text();
        self.collection.json_draft = JsonDraft::default();
        let text = self.collection.json_baseline.clone();
        self.collection
            .json_editor
            .update(cx, |editor, cx| editor.set_value(text, window, cx));
        cx.notify();
    }

    /// Applies a deferred JSON reload from the render pass.
    pub(super) fn flush_json_reload(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if std::mem::take(&mut self.pending.json_reload) {
            self.load_json_editor_with_window(window, cx);
        }
    }

    /// Parses the JSON view against the loaded page: which documents
    /// changed, or why the text cannot be committed.
    fn parse_json_draft(&self, text: &str) -> Result<Vec<(usize, Value)>, String> {
        let parsed: serde_json::Value =
            serde_json::from_str(text).map_err(|error| error.to_string())?;

        let serde_json::Value::Array(items) = parsed else {
            return Err(dbflux_i18n::t!("document.collection.json.not_array"));
        };

        if items.len() != self.collection.documents.len() {
            return Err(dbflux_i18n::t!("document.collection.json.count_changed"));
        }

        let identity_columns = self.identity_columns();
        let mut changed = Vec::new();

        for (index, (item, loaded)) in items.iter().zip(&self.collection.documents).enumerate() {
            let edited = document_json_to_value(item);
            let Value::Document(edited_fields) = &edited else {
                return Err(dbflux_i18n::t!("document.collection.json.not_document"));
            };

            let loaded = without_top_level_nulls(loaded);
            let Value::Document(loaded_fields) = &loaded else {
                continue;
            };

            let identity_kept = identity_columns
                .iter()
                .all(|column| edited_fields.get(column) == loaded_fields.get(column));
            if !identity_kept {
                return Err(dbflux_i18n::t!("document.collection.json.identity_changed"));
            }

            if edited != loaded {
                changed.push((index, edited));
            }
        }

        Ok(changed)
    }

    fn refresh_json_draft(&mut self, cx: &mut Context<Self>) {
        let text = self.collection.json_editor.read(cx).value().to_string();

        self.collection.json_draft = if text == self.collection.json_baseline {
            JsonDraft::default()
        } else {
            match self.parse_json_draft(&text) {
                Ok(changed) => JsonDraft {
                    changed: changed.len(),
                    error: None,
                },
                Err(error) => JsonDraft {
                    changed: 0,
                    error: Some(error),
                },
            }
        };

        cx.notify();
    }

    fn json_commits(&mut self, cx: &mut Context<Self>) -> Option<Vec<DocumentCommit>> {
        let text = self.collection.json_editor.read(cx).value().to_string();

        let changed = match self.parse_json_draft(&text) {
            Ok(changed) => changed,
            Err(error) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::User,
                        crate::labels::collection_json_invalid(&error),
                    ),
                    cx,
                );
                return None;
            }
        };

        let mut commits = Vec::new();
        for (document, edited) in changed {
            commits.push(self.new_commit(
                document,
                DocumentEdit::Replace(edited),
                Vec::new(),
                cx,
            )?);
        }

        Some(commits)
    }
}

#[cfg(test)]
mod tests {
    // Only what the tests need: `use super::*` would pull in `gpui::*`,
    // whose macros overflow the recursion limit under `#[test]`.
    use super::{QueryHistoryEntry, document_label, without_top_level_nulls};
    use dbflux_core::{DocumentServerState, Value, assess_server_change};
    use std::collections::BTreeMap;

    fn document(entries: &[(&str, Value)]) -> Value {
        Value::Document(
            entries
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect::<BTreeMap<_, _>>(),
        )
    }

    #[test]
    fn top_level_nulls_are_dropped_for_the_server_comparison() {
        let loaded = document(&[("_id", Value::Int(1)), ("note", Value::Null)]);
        let current = document(&[("_id", Value::Int(1))]);

        assert_eq!(
            assess_server_change(
                &without_top_level_nulls(&loaded),
                Some(&without_top_level_nulls(&current)),
                &[],
                false
            ),
            DocumentServerState::Unchanged
        );
    }

    #[test]
    fn document_label_prefers_the_first_text_field() {
        let product = document(&[
            ("_id", Value::Int(7)),
            ("sku", Value::Text("CAT-00735".into())),
            ("name", Value::Text("Walnut lamp".into())),
        ]);
        let order: Vec<String> = vec!["_id".into(), "sku".into(), "name".into()];

        assert_eq!(
            document_label(&product, &order, &vec![("_id".into(), Value::Int(7))]),
            "CAT-00735"
        );

        let bare = document(&[("_id", Value::Int(7))]);
        assert_eq!(
            document_label(&bare, &order, &vec![("_id".into(), Value::Int(7))]),
            "7"
        );
    }

    #[test]
    fn history_summary_skips_empty_slots() {
        let entry = QueryHistoryEntry {
            filter: "{ status: \"failed\" }".into(),
            projection: String::new(),
            sort: "{ created_at: -1 }".into(),
            limit: "50".into(),
        };

        assert_eq!(
            entry.summary(),
            "filter { status: \"failed\" }  sort { created_at: -1 }  limit 50"
        );
    }
}
