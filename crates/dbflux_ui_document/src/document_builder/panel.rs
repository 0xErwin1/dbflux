//! The builder rail's entity: the draft, its sync with the slots and the
//! inputs keyed by draft node.

use std::collections::HashMap;
use std::sync::Arc;

use dbflux_components::controls::InputEvent;
use dbflux_core::{
    CollectionRef, CollectionSchemaSample, Connection, DocumentCombinator, DocumentFieldType,
    DocumentFindSlots, DocumentOperator, DocumentProjectionMode, DocumentQueryCodec,
    DocumentSortDirection,
};
use gpui::{
    App, AppContext, Context, Entity, EventEmitter, FocusHandle, Focusable, Subscription, Window,
};
use gpui_component::input::InputState;

use super::catalog::FieldCatalog;
use super::model::{BuilderDraft, DraftProblem, NodeId, Operand};
use super::sync::{SlotSync, SlotWrite};
use super::values::{ScalarKind, ValueEditor, ValueProblem, operator_choices};

/// What the rail asks of the grid that owns the slots.
#[derive(Debug, Clone)]
pub enum DocumentBuilderEvent {
    /// Write these texts into the query slots.
    WriteSlots(SlotWrite),
    /// Run the slots, which hold the builder's query.
    FindRequested,
    /// Open the query text in a new editor tab.
    OpenInEditorRequested(String),
    /// Hide the rail, keeping the draft.
    CloseRequested,
}

/// What a pick in the field picker fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickTarget {
    Condition(NodeId),
    Projection,
    Sort,
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

        let subscriptions = vec![
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
    pub fn read_slots(&mut self, slots: DocumentFindSlots, cx: &mut Context<Self>) {
        if self.sync.is_echo(&slots) {
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

    pub fn request_open_in_editor(&mut self, cx: &mut Context<Self>) {
        if self.problems.is_empty() && self.render_error.is_none() && !self.preview.is_empty() {
            cx.emit(DocumentBuilderEvent::OpenInEditorRequested(
                self.preview.clone(),
            ));
        }
    }

    pub fn request_close(&mut self, cx: &mut Context<Self>) {
        self.picker = None;
        cx.emit(DocumentBuilderEvent::CloseRequested);
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
            PickTarget::Projection | PickTarget::Sort => self.catalog.clone(),
        }
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
                if self.draft.add_sort_key(path) {
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
        if element_itself || is_valid_typed_path(path) {
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
            PickTarget::Projection | PickTarget::Sort => false,
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
            self.request_find(cx);
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
    }
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
