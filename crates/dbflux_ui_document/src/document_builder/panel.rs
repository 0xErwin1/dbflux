//! The builder rail's entity: the draft, its sync with the slots and the
//! inputs keyed by draft node.
//!
//! In Find mode edits are written to the slots and Find runs them. In
//! Aggregate mode the slots are left alone: the rail renders the pipeline
//! and Run pipeline hands its text to the collection's Aggregate view.

use std::collections::HashMap;
use std::sync::Arc;

use dbflux_components::controls::InputEvent;
use dbflux_core::{
    CollectionRef, CollectionSchemaSample, Connection, DocumentCombinator, DocumentFeatures,
    DocumentFieldType, DocumentFindSlots, DocumentOperator, DocumentProjectionMode,
    DocumentQueryCodec, DocumentQueryMode, DocumentQuerySpec, DocumentSortDirection,
    parse_aggregate_pipeline,
};
use gpui::{
    App, AppContext, Context, Entity, EventEmitter, FocusHandle, Focusable, Subscription, Window,
};
use gpui_component::input::InputState;

use super::catalog::FieldCatalog;
use super::model::{AccumulatorOp, BuilderDraft, DraftProblem, NodeId, Operand};
use super::sync::{SlotSync, SlotWrite};
use super::values::{ScalarKind, ValueEditor, ValueProblem, operator_choices};

/// What the rail asks of the grid that owns the slots.
#[derive(Debug, Clone)]
pub enum DocumentBuilderEvent {
    /// Write these texts into the query slots.
    WriteSlots(SlotWrite),
    /// Run the slots, which hold the builder's query.
    FindRequested,
    /// Run this pipeline text in the collection's Aggregate view.
    RunPipelineRequested(String),
    /// Switched between Find and Aggregate; the query bar shows the slots
    /// or the pipeline summary.
    ModeChanged,
    /// Open the query text in a new editor tab.
    OpenInEditorRequested(String),
    /// Hide the rail, keeping the draft.
    CloseRequested,
    /// Save `spec` under `name` for this collection.
    SaveRequested {
        name: String,
        spec: Box<DocumentQuerySpec>,
    },
    /// The saved-queries list opened: send the saved queries of this
    /// collection with [`DocumentBuilderPanel::set_saved_queries`].
    SavedQueriesRequested,
    /// Open the saved query `id` in the builder.
    OpenSavedRequested { id: String },
    /// Delete the saved query `id`.
    DeleteSavedRequested { id: String },
}

/// A saved query as the rail lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedQueryEntry {
    pub id: String,
    pub name: String,
    pub mode: DocumentQueryMode,
}

/// What a pick in the field picker fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickTarget {
    Condition(NodeId),
    Projection,
    Sort,
    GroupKey,
    /// The field a `$sum` or `$avg` accumulator reads.
    Accumulator(NodeId),
}

pub(super) struct FieldPicker {
    pub target: PickTarget,
    pub search: Entity<InputState>,
    _subscription: Subscription,
}

pub(super) struct NodeInput {
    pub state: Entity<InputState>,
    _subscription: Subscription,
}

/// The open operator list of one condition.
pub(super) struct OperatorMenu {
    pub condition: NodeId,
    /// Position of the keyboard highlight in the condition's choices.
    pub highlighted: usize,
    pub focus: FocusHandle,
}

/// Visual find builder for one collection.
pub struct DocumentBuilderPanel {
    connection: Arc<dyn Connection>,
    pub(super) collection: CollectionRef,
    pub(super) draft: BuilderDraft,
    pub(super) sync: SlotSync,
    pub(super) catalog: FieldCatalog,
    pub(super) sampled_documents: Option<u64>,
    pub(super) sampling: bool,
    pub(super) problems: Vec<DraftProblem>,
    pub(super) render_error: Option<String>,
    pub(super) preview: String,
    /// Pipeline text of the last aggregate the draft rendered to.
    pub(super) pipeline: Option<String>,
    /// Whether the Filter card is open in Aggregate mode, instead of its
    /// `$match` summary.
    pub(super) filter_expanded: bool,
    pub(super) accumulator_inputs: HashMap<NodeId, NodeInput>,
    pub(super) picker: Option<FieldPicker>,
    pub(super) chip_problems: HashMap<NodeId, ValueProblem>,
    pub(super) value_inputs: HashMap<NodeId, NodeInput>,
    pub(super) operator_menu: Option<OperatorMenu>,
    /// Texts to put into value inputs on the next render, which has the
    /// window `set_value` needs.
    pub(super) pending_texts: HashMap<NodeId, String>,
    pub(super) limit_input: Entity<InputState>,
    pub(super) skip_input: Entity<InputState>,
    pub(super) limit_problem: bool,
    pub(super) skip_problem: bool,
    pub(super) pending_paging_texts: bool,
    /// Name the query is saved under.
    pub(super) name_input: Entity<InputState>,
    /// Text to put into the name input on the next render.
    pending_name: Option<String>,
    /// The saved query the draft was last saved as or opened from.
    pub(super) loaded_id: Option<String>,
    /// Saved queries of the collection, while their list is open.
    pub(super) saved_queries: Vec<SavedQueryEntry>,
    pub(super) saved_menu_open: bool,
    focus_handle: FocusHandle,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<DocumentBuilderEvent> for DocumentBuilderPanel {}

impl Focusable for DocumentBuilderPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl DocumentBuilderPanel {
    pub fn new(
        connection: Arc<dyn Connection>,
        collection: CollectionRef,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let limit_input = cx.new(|cx| InputState::new(window, cx));
        let skip_input = cx.new(|cx| InputState::new(window, cx).placeholder("0"));
        let name_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.collection.builder.saved.name_placeholder"
            ))
        });

        let subscriptions = vec![
            cx.subscribe(&name_input, |_this, _input, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    cx.notify();
                }
            }),
            cx.subscribe(&limit_input, |this, input, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    let text = input.read(cx).value().to_string();
                    this.set_limit_text(&text, cx);
                }
            }),
            cx.subscribe(&skip_input, |this, input, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    let text = input.read(cx).value().to_string();
                    this.set_skip_text(&text, cx);
                }
            }),
        ];

        Self {
            connection,
            collection,
            draft: BuilderDraft::default(),
            sync: SlotSync::default(),
            catalog: FieldCatalog::default(),
            sampled_documents: None,
            sampling: false,
            problems: Vec::new(),
            render_error: None,
            preview: String::new(),
            pipeline: None,
            filter_expanded: false,
            accumulator_inputs: HashMap::new(),
            picker: None,
            chip_problems: HashMap::new(),
            value_inputs: HashMap::new(),
            operator_menu: None,
            pending_texts: HashMap::new(),
            limit_input,
            skip_input,
            limit_problem: false,
            skip_problem: false,
            pending_paging_texts: false,
            name_input,
            pending_name: None,
            loaded_id: None,
            saved_queries: Vec::new(),
            saved_menu_open: false,
            focus_handle: cx.focus_handle(),
            _subscriptions: subscriptions,
        }
    }

    fn codec(&self) -> Option<&dyn DocumentQueryCodec> {
        self.connection.document_query_codec()
    }

    // ---- state the grid reads --------------------------------------------

    #[cfg(test)]
    pub fn draft(&self) -> &BuilderDraft {
        &self.draft
    }

    pub fn skip(&self) -> Option<u64> {
        self.draft.skip
    }

    /// Whether some slot holds clauses the builder could not read.
    pub fn is_conflicted(&self) -> bool {
        self.sync.is_conflicted()
    }

    pub fn mode(&self) -> DocumentQueryMode {
        self.draft.mode
    }

    /// Whether the connection runs aggregations, which Aggregate mode needs.
    pub fn aggregate_available(&self) -> bool {
        self.connection
            .document_features()
            .contains(DocumentFeatures::AGGREGATE)
    }

    /// Stage names of the pipeline, in order, while in Aggregate mode; empty
    /// until the draft renders to a pipeline.
    pub fn pipeline_stages(&self) -> Option<Vec<String>> {
        if self.draft.mode != DocumentQueryMode::Aggregate {
            return None;
        }

        let stages = self
            .pipeline
            .as_deref()
            .and_then(|text| parse_aggregate_pipeline(text).ok())
            .unwrap_or_default()
            .iter()
            .filter_map(|stage| stage.as_object()?.keys().next().cloned())
            .collect();

        Some(stages)
    }

    /// Whether the primary action can run: Find, or Run pipeline.
    pub fn can_run(&self) -> bool {
        match self.draft.mode {
            DocumentQueryMode::Find => self.can_find(),
            DocumentQueryMode::Aggregate => {
                self.codec().is_some()
                    && self.aggregate_available()
                    && self.problems.is_empty()
                    && self.render_error.is_none()
                    && !self.limit_problem
                    && !self.skip_problem
                    && self.pipeline.is_some()
            }
        }
    }

    /// Whether the slots hold the builder's query, so Find runs it.
    pub fn can_find(&self) -> bool {
        self.codec().is_some()
            && self.problems.is_empty()
            && self.render_error.is_none()
            && !self.limit_problem
            && !self.skip_problem
            && self.sync.held().is_empty()
    }

    // ---- slots -> builder ------------------------------------------------

    /// Reads the slots into the draft unless they only echo what the
    /// builder last wrote or read. Never writes back.
    /// In Aggregate mode the slots are not the builder's query, so they are
    /// not read either.
    pub fn read_slots(&mut self, slots: DocumentFindSlots, cx: &mut Context<Self>) {
        if self.draft.mode == DocumentQueryMode::Aggregate || self.sync.is_echo(&slots) {
            return;
        }

        self.reload(slots, cx);
    }

    fn reload(&mut self, mut slots: DocumentFindSlots, cx: &mut Context<Self>) {
        let Some(codec) = self.codec() else {
            return;
        };

        // The slots have no skip; it lives in the builder.
        slots.skip = self.draft.skip;
        let parse = codec.parse_find(&slots);

        self.draft.load(&parse.spec);
        self.sync.read(&slots, &parse);

        self.picker = None;
        self.operator_menu = None;
        self.chip_problems.clear();
        self.pending_paging_texts = true;
        self.limit_problem = false;
        self.sweep_inputs();
        self.recompute(false, cx);
    }

    /// Takes a new schema sample for the field picker and type tags.
    pub fn set_schema(&mut self, sample: &CollectionSchemaSample, cx: &mut Context<Self>) {
        let catalog = match self.codec() {
            Some(codec) => FieldCatalog::new(sample, |type_name| codec.field_type(type_name)),
            None => FieldCatalog::default(),
        };

        self.catalog = catalog;
        self.sampled_documents = Some(sample.sampled_documents);
        self.sampling = false;
        cx.notify();
    }

    pub fn set_sampling(&mut self, sampling: bool, cx: &mut Context<Self>) {
        self.sampling = sampling;
        cx.notify();
    }

    // ---- builder -> slots ------------------------------------------------

    /// Rebuilds the preview and, when `write` is set, sends the changed
    /// parts to the slots.
    fn recompute(&mut self, write: bool, cx: &mut Context<Self>) {
        let spec = match self.draft.to_spec() {
            Ok(spec) => spec,
            Err(problems) => {
                self.problems = problems;
                self.render_error = None;
                cx.notify();
                return;
            }
        };

        self.problems.clear();

        let Some(codec) = self.connection.document_query_codec() else {
            cx.notify();
            return;
        };

        match self.draft.mode {
            DocumentQueryMode::Find => {
                self.pipeline = None;
                match codec.render_find(&spec) {
                    Ok(rendered) => {
                        self.render_error = None;
                        if write {
                            let slot_write = self.sync.plan(&spec, &rendered);
                            if !slot_write.is_empty() {
                                cx.emit(DocumentBuilderEvent::WriteSlots(slot_write));
                            }
                        }
                    }
                    Err(error) => {
                        self.render_error = Some(error.to_string());
                    }
                }
            }
            DocumentQueryMode::Aggregate => match codec.render_pipeline(&spec) {
                Ok(pipeline) => {
                    self.render_error = None;
                    self.pipeline = Some(pipeline);
                }
                Err(error) => {
                    self.render_error = Some(error.to_string());
                    self.pipeline = None;
                }
            },
        }

        match codec.render_preview(&spec, &self.collection.name) {
            Ok(preview) => self.preview = preview,
            Err(error) => self.render_error = Some(error.to_string()),
        }

        cx.notify();
    }

    fn edited(&mut self, cx: &mut Context<Self>) {
        self.recompute(true, cx);
    }

    /// Discards the edits held back by a slot the builder could not read and
    /// shows that slot's text again.
    pub fn keep_text(&mut self, cx: &mut Context<Self>) {
        let slots = self.sync.texts().clone();
        self.reload(slots, cx);
    }

    /// Replaces every slot with the builder's query, dropping the clauses it
    /// could not read.
    pub fn rewrite_from_builder(&mut self, cx: &mut Context<Self>) {
        if self.draft.mode != DocumentQueryMode::Find {
            return;
        }
        let Ok(spec) = self.draft.to_spec() else {
            return;
        };
        let Some(codec) = self.connection.document_query_codec() else {
            return;
        };

        match codec.render_find(&spec) {
            Ok(rendered) => {
                let slot_write = self.sync.rewrite(&spec, &rendered);
                if !slot_write.is_empty() {
                    cx.emit(DocumentBuilderEvent::WriteSlots(slot_write));
                }
                self.render_error = None;
            }
            Err(error) => self.render_error = Some(error.to_string()),
        }

        cx.notify();
    }

    pub fn request_find(&mut self, cx: &mut Context<Self>) {
        if self.can_find() {
            cx.emit(DocumentBuilderEvent::FindRequested);
        }
    }

    /// The primary action of the rail: Find, or Run pipeline.
    pub fn request_run(&mut self, cx: &mut Context<Self>) {
        match self.draft.mode {
            DocumentQueryMode::Find => self.request_find(cx),
            DocumentQueryMode::Aggregate => {
                if self.can_run()
                    && let Some(pipeline) = self.pipeline.clone()
                {
                    cx.emit(DocumentBuilderEvent::RunPipelineRequested(pipeline));
                }
            }
        }
    }

    pub fn request_open_in_editor(&mut self, cx: &mut Context<Self>) {
        if self.problems.is_empty() && self.render_error.is_none() && !self.preview.is_empty() {
            cx.emit(DocumentBuilderEvent::OpenInEditorRequested(
                self.preview.clone(),
            ));
        }
    }

    pub fn request_close(&mut self, cx: &mut Context<Self>) {
        self.picker = None;
        self.saved_menu_open = false;
        cx.emit(DocumentBuilderEvent::CloseRequested);
    }

    // ---- saved queries ---------------------------------------------------

    /// The name in the header, trimmed. A name set by opening a saved
    /// query counts before the next render puts it into the input.
    pub fn query_name(&self, cx: &App) -> String {
        match &self.pending_name {
            Some(name) => name.trim().to_string(),
            None => self.name_input.read(cx).value().trim().to_string(),
        }
    }

    pub fn loaded_id(&self) -> Option<&str> {
        self.loaded_id.as_deref()
    }

    /// The saved queries of the collection, as last sent by the grid.
    pub fn saved_queries(&self) -> &[SavedQueryEntry] {
        &self.saved_queries
    }

    #[cfg(test)]
    pub fn set_query_name(&mut self, name: &str, window: &mut Window, cx: &mut Context<Self>) {
        self.pending_name = None;
        self.name_input.update(cx, |state, cx| {
            state.set_value(name.to_string(), window, cx)
        });
    }

    /// Whether Save can run: the query has a name and describes a spec.
    pub fn can_save(&self, cx: &App) -> bool {
        !self.query_name(cx).is_empty() && self.draft.to_spec().is_ok()
    }

    /// Asks the grid to save the draft under the name in the header.
    pub fn request_save(&mut self, cx: &mut Context<Self>) {
        if !self.can_save(cx) {
            return;
        }
        let Ok(spec) = self.draft.to_spec() else {
            return;
        };

        cx.emit(DocumentBuilderEvent::SaveRequested {
            name: self.query_name(cx),
            spec: Box::new(spec),
        });
    }

    /// Records that the draft was saved as `id`.
    pub fn mark_saved(&mut self, id: String, cx: &mut Context<Self>) {
        self.loaded_id = Some(id);
        cx.notify();
    }

    /// Opens or closes the list of saved queries. Opening asks the grid for
    /// the current list.
    pub fn toggle_saved_menu(&mut self, cx: &mut Context<Self>) {
        self.saved_menu_open = !self.saved_menu_open;
        if self.saved_menu_open {
            self.picker = None;
            self.operator_menu = None;
            cx.emit(DocumentBuilderEvent::SavedQueriesRequested);
        }
        cx.notify();
    }

    pub fn set_saved_queries(&mut self, entries: Vec<SavedQueryEntry>, cx: &mut Context<Self>) {
        if self
            .loaded_id
            .as_ref()
            .is_some_and(|id| !entries.iter().any(|entry| &entry.id == id))
        {
            self.loaded_id = None;
        }

        self.saved_queries = entries;
        cx.notify();
    }

    pub fn request_open_saved(&mut self, id: &str, cx: &mut Context<Self>) {
        self.saved_menu_open = false;
        cx.emit(DocumentBuilderEvent::OpenSavedRequested { id: id.to_string() });
        cx.notify();
    }

    pub fn request_delete_saved(&mut self, id: &str, cx: &mut Context<Self>) {
        cx.emit(DocumentBuilderEvent::DeleteSavedRequested { id: id.to_string() });
    }

    /// Replaces the draft with the saved query `id`, in its saved mode. A
    /// find is written to the slots, replacing whatever they held; an
    /// aggregation leaves them alone. Returns `false`, changing nothing,
    /// when the query is an aggregation this connection cannot run.
    pub fn open_saved(
        &mut self,
        id: String,
        name: &str,
        spec: &DocumentQuerySpec,
        cx: &mut Context<Self>,
    ) -> bool {
        if spec.mode == DocumentQueryMode::Aggregate && !self.aggregate_available() {
            return false;
        }

        self.draft.load_all(spec);
        self.loaded_id = Some(id);
        self.pending_name = Some(name.to_string());
        self.saved_menu_open = false;

        self.picker = None;
        self.operator_menu = None;
        self.filter_expanded = false;
        self.chip_problems.clear();
        self.pending_paging_texts = true;
        self.limit_problem = false;
        self.skip_problem = false;
        self.sweep_inputs();

        self.recompute(false, cx);
        if self.draft.mode == DocumentQueryMode::Find {
            self.rewrite_from_builder(cx);
        }

        cx.emit(DocumentBuilderEvent::ModeChanged);
        true
    }

    // ---- mode and group stage --------------------------------------------

    /// Switches between Find and Aggregate. Aggregate is refused without the
    /// connection's aggregation support. Back in Find, the parts edited in
    /// the meantime are written to the slots.
    pub fn set_mode(&mut self, mode: DocumentQueryMode, cx: &mut Context<Self>) {
        if mode == DocumentQueryMode::Aggregate && !self.aggregate_available() {
            return;
        }

        if self.draft.set_mode(mode) {
            self.mode_changed(cx);
        }
    }

    /// "Add group stage": switches to Aggregate.
    pub fn add_group_stage(&mut self, cx: &mut Context<Self>) {
        self.set_mode(DocumentQueryMode::Aggregate, cx);
    }

    /// "Remove group stage": drops it and returns to Find.
    pub fn remove_group_stage(&mut self, cx: &mut Context<Self>) {
        if self.draft.remove_group_stage() {
            self.mode_changed(cx);
        }
    }

    fn mode_changed(&mut self, cx: &mut Context<Self>) {
        self.picker = None;
        self.operator_menu = None;
        self.filter_expanded = false;
        self.sweep_inputs();
        self.recompute(self.draft.mode == DocumentQueryMode::Find, cx);
        cx.emit(DocumentBuilderEvent::ModeChanged);
    }

    /// Opens or closes the Filter card behind the `$match` summary.
    pub fn toggle_filter_expanded(&mut self, cx: &mut Context<Self>) {
        self.filter_expanded = !self.filter_expanded;
        cx.notify();
    }

    pub fn remove_group_key(&mut self, index: usize, cx: &mut Context<Self>) {
        if self.draft.remove_group_key(index) {
            self.edited(cx);
        }
    }

    pub fn add_accumulator(&mut self, cx: &mut Context<Self>) {
        if self.draft.add_accumulator().is_some() {
            self.edited(cx);
        }
    }

    pub fn remove_accumulator(&mut self, id: NodeId, cx: &mut Context<Self>) {
        if self.draft.remove_accumulator(id) {
            self.sweep_inputs();
            self.edited(cx);
        }
    }

    pub fn set_accumulator_op(&mut self, id: NodeId, op: AccumulatorOp, cx: &mut Context<Self>) {
        if self.draft.set_accumulator_op(id, op) {
            self.edited(cx);
        }
    }

    fn set_accumulator_name(&mut self, id: NodeId, name: &str, cx: &mut Context<Self>) {
        if self.draft.set_accumulator_name(id, name) {
            self.edited(cx);
        }
    }

    /// Sort names offered after the group stage, typed like the fields they
    /// come from: a key as sampled, a count as a whole number, a sum as its
    /// field and an average as a decimal.
    fn group_output_catalog(&self) -> FieldCatalog {
        let Some(stage) = self.draft.group.as_ref() else {
            return FieldCatalog::default();
        };

        let numeric = |types: Vec<DocumentFieldType>| {
            let numeric: Vec<DocumentFieldType> = types
                .into_iter()
                .filter(|field_type| is_numeric(*field_type))
                .collect();
            if numeric.is_empty() {
                vec![DocumentFieldType::Decimal]
            } else {
                numeric
            }
        };

        let keys = stage
            .keys
            .iter()
            .map(|key| (key.clone(), self.catalog.types(key)));
        let accumulators = stage.accumulators.iter().map(|accumulator| {
            let types = match accumulator.op {
                AccumulatorOp::Count => vec![DocumentFieldType::Integer],
                AccumulatorOp::Sum => numeric(self.catalog.types(&accumulator.path)),
                AccumulatorOp::Avg => vec![DocumentFieldType::Decimal],
            };
            (accumulator.name.trim().to_string(), types)
        });

        let mut outputs: Vec<(String, Vec<DocumentFieldType>)> = Vec::new();
        for (name, types) in keys.chain(accumulators) {
            if !name.is_empty() && !outputs.iter().any(|(existing, _)| *existing == name) {
                outputs.push((name, types));
            }
        }

        FieldCatalog::from_outputs(outputs)
    }

    /// Types of a sort key: the sampled field in Find mode, the group output
    /// in Aggregate mode.
    pub(super) fn sort_types(&self, path: &str) -> Vec<DocumentFieldType> {
        match self.draft.mode {
            DocumentQueryMode::Find => self.catalog.types(path),
            DocumentQueryMode::Aggregate => self.group_output_catalog().types(path),
        }
    }

    // ---- filter edits ----------------------------------------------------

    /// Types sampled for `path` as the condition `id` would test it.
    pub(super) fn types_for(&self, id: NodeId, path: &str) -> Vec<DocumentFieldType> {
        let scope = self.draft.condition_scope(id).unwrap_or_default();
        self.catalog.types(&join_path(&scope, path))
    }

    pub(super) fn is_sampled(&self, id: NodeId, path: &str) -> bool {
        let scope = self.draft.condition_scope(id).unwrap_or_default();
        self.catalog.is_sampled(&join_path(&scope, path))
    }

    /// Catalog of the fields a pick for `target` can name.
    pub(super) fn picker_catalog(&self, target: PickTarget) -> FieldCatalog {
        match target {
            PickTarget::Condition(id) => {
                let scope = self.draft.condition_scope(id).unwrap_or_default();
                self.catalog.scoped(&scope)
            }
            PickTarget::Sort if self.draft.mode == DocumentQueryMode::Aggregate => {
                self.group_output_catalog()
            }
            PickTarget::Accumulator(_) => self.catalog.numeric(),
            PickTarget::Projection | PickTarget::Sort | PickTarget::GroupKey => {
                self.catalog.clone()
            }
        }
    }

    /// Whether a pick for `target` may name a path the list does not offer.
    /// After a group stage a sort names only a group output.
    pub(super) fn accepts_typed_path(&self, target: PickTarget) -> bool {
        !(target == PickTarget::Sort && self.draft.mode == DocumentQueryMode::Aggregate)
    }

    pub fn add_condition(&mut self, group: NodeId, cx: &mut Context<Self>) {
        if self.draft.add_condition(group).is_some() {
            self.edited(cx);
        }
    }

    pub fn add_group(&mut self, group: NodeId, cx: &mut Context<Self>) {
        if self.draft.add_group(group).is_some() {
            self.edited(cx);
        }
    }

    pub fn remove_node(&mut self, id: NodeId, cx: &mut Context<Self>) {
        if self.draft.remove_node(id) {
            self.sweep_inputs();
            self.edited(cx);
        }
    }

    pub fn set_combinator(
        &mut self,
        group: NodeId,
        combinator: DocumentCombinator,
        cx: &mut Context<Self>,
    ) {
        if self.draft.set_combinator(group, combinator) {
            self.edited(cx);
        }
    }

    pub fn set_path(&mut self, id: NodeId, path: &str, cx: &mut Context<Self>) {
        let types = self.types_for(id, path);
        if self.draft.set_path(id, path, &types) {
            self.after_operand_reshape(id);
            self.edited(cx);
        }
    }

    pub fn set_operator(&mut self, id: NodeId, operator: DocumentOperator, cx: &mut Context<Self>) {
        if self.draft.set_operator(id, operator) {
            self.after_operand_reshape(id);
            self.edited(cx);
        }
    }

    pub fn set_kind(&mut self, id: NodeId, kind: ScalarKind, cx: &mut Context<Self>) {
        if self.draft.set_kind(id, kind) {
            self.after_operand_reshape(id);
            self.edited(cx);
        }
    }

    pub fn set_text(&mut self, id: NodeId, text: &str, cx: &mut Context<Self>) {
        if self.draft.set_text(id, text) {
            self.edited(cx);
        }
    }

    pub fn set_toggle(&mut self, id: NodeId, flag: bool, cx: &mut Context<Self>) {
        if self.draft.set_toggle(id, flag) {
            self.edited(cx);
        }
    }

    /// Adds the chip typed into the condition's entry and clears the entry.
    pub fn add_chip(&mut self, id: NodeId, text: &str, cx: &mut Context<Self>) {
        match self.draft.add_chip(id, text) {
            Ok(()) => {
                self.chip_problems.remove(&id);
                self.pending_texts.insert(id, String::new());
                self.edited(cx);
            }
            Err(problem) => {
                self.chip_problems.insert(id, problem);
                cx.notify();
            }
        }
    }

    pub fn remove_chip(&mut self, id: NodeId, index: usize, cx: &mut Context<Self>) {
        if self.draft.remove_chip(id, index) {
            self.edited(cx);
        }
    }

    /// After an operand changed shape its input shows the new text.
    fn after_operand_reshape(&mut self, id: NodeId) {
        self.chip_problems.remove(&id);

        let text = match self.draft.condition(id).map(|condition| &condition.operand) {
            Some(Operand::Text { text, .. }) => text.clone(),
            _ => String::new(),
        };
        self.pending_texts.insert(id, text);
        self.sweep_inputs();
    }

    // ---- projection, sort, limit, skip -----------------------------------

    pub fn set_projection_mode(&mut self, mode: DocumentProjectionMode, cx: &mut Context<Self>) {
        if self.draft.projection.mode != mode {
            self.draft.set_projection_mode(mode);
            self.edited(cx);
        }
    }

    pub fn remove_projection_field(&mut self, index: usize, cx: &mut Context<Self>) {
        if self.draft.remove_projection_field(index) {
            self.edited(cx);
        }
    }

    pub fn remove_sort_key(&mut self, index: usize, cx: &mut Context<Self>) {
        if self.draft.remove_sort_key(index) {
            self.edited(cx);
        }
    }

    pub fn set_sort_direction(
        &mut self,
        index: usize,
        direction: DocumentSortDirection,
        cx: &mut Context<Self>,
    ) {
        if self.draft.set_sort_direction(index, direction) {
            self.edited(cx);
        }
    }

    pub fn move_sort_key(&mut self, from: usize, to: usize, cx: &mut Context<Self>) {
        if self.draft.move_sort_key(from, to) {
            self.edited(cx);
        }
    }

    fn set_limit_text(&mut self, text: &str, cx: &mut Context<Self>) {
        match parse_paging(text) {
            Ok(Some(0)) | Err(()) => {
                self.limit_problem = true;
                cx.notify();
            }
            Ok(limit) => {
                self.limit_problem = false;
                if self.draft.limit != limit {
                    self.draft.limit = limit;
                    self.edited(cx);
                } else {
                    cx.notify();
                }
            }
        }
    }

    fn set_skip_text(&mut self, text: &str, cx: &mut Context<Self>) {
        match parse_paging(text) {
            Err(()) => {
                self.skip_problem = true;
                cx.notify();
            }
            Ok(skip) => {
                self.skip_problem = false;
                self.set_skip(skip, cx);
            }
        }
    }

    /// Documents a Find passes over before its first page. The slots have
    /// no skip, so only the preview changes.
    pub fn set_skip(&mut self, skip: Option<u64>, cx: &mut Context<Self>) {
        let skip = skip.filter(|skip| *skip > 0);
        if self.draft.skip != skip {
            self.draft.skip = skip;
            self.edited(cx);
        } else {
            cx.notify();
        }
    }

    // ---- operator list ---------------------------------------------------

    /// Operators the condition `id` offers for its field.
    pub(super) fn operator_choices_for(&self, id: NodeId) -> Vec<DocumentOperator> {
        let path = self
            .draft
            .condition(id)
            .map(|condition| condition.path.clone())
            .unwrap_or_default();
        operator_choices(&self.types_for(id, &path))
    }

    /// Opens the operator list of condition `id`, or closes it when open.
    /// The list takes the keyboard focus so the arrows and Enter pick.
    pub fn toggle_operator_menu(
        &mut self,
        id: NodeId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self
            .operator_menu
            .as_ref()
            .is_some_and(|menu| menu.condition == id)
        {
            self.close_operator_menu(window, cx);
            return;
        }

        let current = self.draft.condition(id).map(|condition| condition.operator);
        let highlighted = self
            .operator_choices_for(id)
            .iter()
            .position(|choice| Some(*choice) == current)
            .unwrap_or(0);

        let focus = cx.focus_handle();
        focus.focus(window, cx);

        self.picker = None;
        self.operator_menu = Some(OperatorMenu {
            condition: id,
            highlighted,
            focus,
        });
        cx.notify();
    }

    pub fn close_operator_menu(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.operator_menu.take().is_some() {
            self.focus_handle.focus(window, cx);
            cx.notify();
        }
    }

    /// Moves the keyboard highlight of the open list by `step`, wrapping.
    pub fn move_operator_highlight(&mut self, step: isize, cx: &mut Context<Self>) {
        let Some(id) = self.operator_menu.as_ref().map(|menu| menu.condition) else {
            return;
        };
        let count = self.operator_choices_for(id).len();
        if count == 0 {
            return;
        }

        if let Some(menu) = self.operator_menu.as_mut() {
            let current = menu.highlighted as isize;
            menu.highlighted = (current + step).rem_euclid(count as isize) as usize;
            cx.notify();
        }
    }

    /// Picks the highlighted operator of the open list.
    pub fn choose_highlighted_operator(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some((id, highlighted)) = self
            .operator_menu
            .as_ref()
            .map(|menu| (menu.condition, menu.highlighted))
        else {
            return;
        };

        if let Some(operator) = self.operator_choices_for(id).get(highlighted).copied() {
            self.choose_operator(id, operator, window, cx);
        }
    }

    /// Sets the operator of condition `id` and closes its list.
    pub fn choose_operator(
        &mut self,
        id: NodeId,
        operator: DocumentOperator,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.close_operator_menu(window, cx);
        self.set_operator(id, operator, cx);
    }

    // ---- field picker ----------------------------------------------------

    pub fn open_picker(&mut self, target: PickTarget, window: &mut Window, cx: &mut Context<Self>) {
        if self
            .picker
            .as_ref()
            .is_some_and(|picker| picker.target == target)
        {
            self.picker = None;
            cx.notify();
            return;
        }

        let search = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("document.collection.builder.picker.search"))
        });

        let subscription = cx.subscribe_in(
            &search,
            window,
            |this, input, event: &InputEvent, _window, cx| match event {
                InputEvent::Change => cx.notify(),
                InputEvent::PressEnter { .. } => {
                    let query = input.read(cx).value().to_string();
                    this.pick_typed_path(&query, cx);
                }
                InputEvent::Focus | InputEvent::Blur => {}
            },
        );

        search.update(cx, |state, cx| state.focus(window, cx));

        self.picker = Some(FieldPicker {
            target,
            search,
            _subscription: subscription,
        });
        cx.notify();
    }

    pub fn close_picker(&mut self, cx: &mut Context<Self>) {
        if self.picker.take().is_some() {
            cx.notify();
        }
    }

    /// Search text of the open picker.
    pub(super) fn picker_query(&self, cx: &App) -> String {
        self.picker
            .as_ref()
            .map(|picker| picker.search.read(cx).value().to_string())
            .unwrap_or_default()
    }

    /// Fills the picker's target with `path`.
    pub fn pick(&mut self, path: &str, cx: &mut Context<Self>) {
        let Some(picker) = self.picker.take() else {
            return;
        };

        match picker.target {
            PickTarget::Condition(id) => self.set_path(id, path, cx),
            PickTarget::Projection => {
                if self.draft.add_projection_field(path) {
                    self.edited(cx);
                }
            }
            PickTarget::Sort => {
                let offered = self.accepts_typed_path(PickTarget::Sort)
                    || self
                        .draft
                        .sort_choices()
                        .iter()
                        .any(|choice| choice == path);
                if offered && self.draft.add_sort_key(path) {
                    self.edited(cx);
                }
            }
            PickTarget::GroupKey => {
                if self.draft.add_group_key(path) {
                    self.edited(cx);
                }
            }
            PickTarget::Accumulator(id) => {
                if self.draft.set_accumulator_path(id, path) {
                    self.edited(cx);
                }
            }
        }

        cx.notify();
    }

    /// Enter in the picker: the typed path, sampled or not.
    fn pick_typed_path(&mut self, query: &str, cx: &mut Context<Self>) {
        let Some(target) = self.picker.as_ref().map(|picker| picker.target) else {
            return;
        };

        let path = query.trim();
        let element_itself = path.is_empty() && self.allows_element_itself(target);
        let typed_allowed = self.accepts_typed_path(target) && is_valid_typed_path(path);
        let offered = self.picker_catalog(target).field(path).is_some();
        if element_itself || typed_allowed || offered {
            self.pick(path, cx);
        }
    }

    /// Whether a pick for `target` may name the array element itself.
    pub(super) fn allows_element_itself(&self, target: PickTarget) -> bool {
        match target {
            PickTarget::Condition(id) => self
                .draft
                .condition_scope(id)
                .is_some_and(|scope| !scope.is_empty()),
            PickTarget::Projection
            | PickTarget::Sort
            | PickTarget::GroupKey
            | PickTarget::Accumulator(_) => false,
        }
    }

    // ---- inputs ----------------------------------------------------------

    /// Creates the inputs the draft needs and flushes pending texts.
    pub(super) fn ensure_inputs(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let conditions: Vec<(NodeId, String)> = self
            .draft
            .conditions()
            .into_iter()
            .map(|condition| {
                let text = match &condition.operand {
                    Operand::Text { text, .. } => text.clone(),
                    _ => String::new(),
                };
                (condition.id, text)
            })
            .collect();

        for (id, current_text) in conditions {
            let wants_text = !matches!(
                self.draft.condition(id).map(|condition| condition.editor()),
                Some(ValueEditor::Toggle | ValueEditor::Nested) | None
            );

            if wants_text && !self.value_inputs.contains_key(&id) {
                let state = cx.new(|cx| {
                    let mut state = InputState::new(window, cx);
                    state.set_value(current_text.clone(), window, cx);
                    state
                });
                let subscription = cx.subscribe_in(
                    &state,
                    window,
                    move |this, input, event: &InputEvent, _window, cx| {
                        let text = input.read(cx).value().to_string();
                        match event {
                            InputEvent::Change => this.value_input_changed(id, &text, cx),
                            InputEvent::PressEnter { .. } => {
                                this.value_input_entered(id, &text, cx)
                            }
                            InputEvent::Focus | InputEvent::Blur => {}
                        }
                    },
                );
                self.value_inputs.insert(
                    id,
                    NodeInput {
                        state,
                        _subscription: subscription,
                    },
                );
                self.pending_texts.remove(&id);
            }
        }

        let pending: Vec<(NodeId, String)> = self.pending_texts.drain().collect();
        for (id, text) in pending {
            if let Some(input) = self.value_inputs.get(&id) {
                input
                    .state
                    .update(cx, |state, cx| state.set_value(text, window, cx));
            }
        }

        let accumulators: Vec<(NodeId, String)> = self
            .draft
            .group
            .as_ref()
            .map(|stage| {
                stage
                    .accumulators
                    .iter()
                    .map(|accumulator| (accumulator.id, accumulator.name.clone()))
                    .collect()
            })
            .unwrap_or_default();

        for (id, name) in accumulators {
            if self.accumulator_inputs.contains_key(&id) {
                continue;
            }

            let state = cx.new(|cx| {
                let mut state = InputState::new(window, cx);
                state.set_value(name, window, cx);
                state
            });
            let subscription = cx.subscribe_in(
                &state,
                window,
                move |this, input, event: &InputEvent, _window, cx| match event {
                    InputEvent::Change => {
                        let text = input.read(cx).value().to_string();
                        this.set_accumulator_name(id, &text, cx);
                    }
                    InputEvent::PressEnter { .. } => this.request_run(cx),
                    InputEvent::Focus | InputEvent::Blur => {}
                },
            );
            self.accumulator_inputs.insert(
                id,
                NodeInput {
                    state,
                    _subscription: subscription,
                },
            );
        }

        if let Some(name) = self.pending_name.take() {
            self.name_input
                .update(cx, |state, cx| state.set_value(name, window, cx));
        }

        if self.pending_paging_texts {
            self.pending_paging_texts = false;
            let limit = self
                .draft
                .limit
                .map(|limit| limit.to_string())
                .unwrap_or_default();
            let skip = self
                .draft
                .skip
                .map(|skip| skip.to_string())
                .unwrap_or_default();
            self.limit_input
                .update(cx, |state, cx| state.set_value(limit, window, cx));
            self.skip_input
                .update(cx, |state, cx| state.set_value(skip, window, cx));
        }
    }

    fn value_input_changed(&mut self, id: NodeId, text: &str, cx: &mut Context<Self>) {
        let is_chips = matches!(
            self.draft.condition(id).map(|condition| condition.editor()),
            Some(ValueEditor::Chips(_))
        );

        if is_chips {
            if self.chip_problems.remove(&id).is_some() {
                cx.notify();
            }
        } else {
            self.set_text(id, text, cx);
        }
    }

    fn value_input_entered(&mut self, id: NodeId, text: &str, cx: &mut Context<Self>) {
        let is_chips = matches!(
            self.draft.condition(id).map(|condition| condition.editor()),
            Some(ValueEditor::Chips(_))
        );

        if is_chips {
            self.add_chip(id, text, cx);
        } else {
            self.request_run(cx);
        }
    }

    /// Drops inputs whose condition left the draft or no longer takes text.
    fn sweep_inputs(&mut self) {
        let ids = self.draft.node_ids();

        let wants_text = |draft: &BuilderDraft, id: &NodeId| {
            !matches!(
                draft.condition(*id).map(|condition| condition.editor()),
                Some(ValueEditor::Toggle | ValueEditor::Nested) | None
            )
        };

        self.value_inputs
            .retain(|id, _| ids.contains(id) && wants_text(&self.draft, id));
        if self
            .operator_menu
            .as_ref()
            .is_some_and(|menu| !ids.contains(&menu.condition))
        {
            self.operator_menu = None;
        }
        self.chip_problems.retain(|id, _| ids.contains(id));
        self.pending_texts.retain(|id, _| ids.contains(id));

        let accumulator_ids: Vec<NodeId> = self
            .draft
            .group
            .as_ref()
            .map(|stage| {
                stage
                    .accumulators
                    .iter()
                    .map(|accumulator| accumulator.id)
                    .collect()
            })
            .unwrap_or_default();
        self.accumulator_inputs
            .retain(|id, _| accumulator_ids.contains(id));
    }
}

pub(super) fn is_numeric(field_type: DocumentFieldType) -> bool {
    matches!(
        field_type,
        DocumentFieldType::Integer | DocumentFieldType::Decimal
    )
}

/// `scope.path`, or either part alone when the other is empty.
pub(super) fn join_path(scope: &str, path: &str) -> String {
    match (scope.is_empty(), path.is_empty()) {
        (true, _) => path.to_string(),
        (false, true) => scope.to_string(),
        (false, false) => format!("{scope}.{path}"),
    }
}

/// A path typed into the picker: dotted segments, none empty or starting
/// with `$`.
pub(super) fn is_valid_typed_path(path: &str) -> bool {
    !path.is_empty()
        && path
            .split('.')
            .all(|segment| !segment.trim().is_empty() && !segment.starts_with('$'))
}

/// Limit or skip text: empty is `None`, anything else a whole number.
fn parse_paging(text: &str) -> Result<Option<u64>, ()> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    trimmed.parse::<u64>().map(Some).map_err(|_| ())
}
