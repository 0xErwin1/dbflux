//! The builder's editable copy of a find query.
//!
//! [`BuilderDraft`] mirrors [`DocumentQuerySpec`] with a stable id on every
//! group and condition, so the panel can key its inputs by node, and keeps
//! each operand as the text the user typed next to the value it reads as.
//! A draft with an unfinished condition cannot become a spec; it reports
//! [`DraftProblem`]s instead, and the slots keep the last query that could.
//!
//! In Aggregate mode the draft also holds a group stage. Switching back to
//! Find keeps that stage aside, so switching again restores it; removing it
//! is what drops it.

use std::collections::HashMap;

use dbflux_core::{
    DocumentAccumulator, DocumentAccumulatorKind, DocumentCombinator, DocumentCondition,
    DocumentFieldType, DocumentFilterGroup, DocumentFilterNode, DocumentGroupStage,
    DocumentOperator, DocumentProjection, DocumentProjectionMode, DocumentQueryMode,
    DocumentQuerySpec, DocumentSortDirection, DocumentSortKey, DocumentSpecProblem, DocumentValue,
};

use super::values::{
    ScalarKind, ValueEditor, ValueProblem, ValueSyntax, format_value, operator_choices,
    parse_count, parse_pattern, parse_scalar, value_editor,
};

pub type NodeId = u64;

/// The field every document has and find results need to stay editable.
pub const ID_FIELD: &str = "_id";

#[derive(Debug, Clone, PartialEq)]
pub struct GroupDraft {
    pub id: NodeId,
    pub combinator: DocumentCombinator,
    pub children: Vec<NodeDraft>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodeDraft {
    Condition(ConditionDraft),
    Group(GroupDraft),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConditionDraft {
    pub id: NodeId,
    /// Field path; relative to the array inside an `ElemMatch`, where an
    /// empty path tests the element itself.
    pub path: String,
    pub operator: DocumentOperator,
    /// How typed values are read.
    pub kind: ScalarKind,
    pub operand: Operand,
}

impl ConditionDraft {
    pub fn editor(&self) -> ValueEditor {
        value_editor(self.operator, self.kind)
    }
}

/// The operand of a condition, shaped by its editor.
#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    /// Text typed into a single-value input and what it reads as.
    Text {
        text: String,
        value: Result<DocumentValue, ValueProblem>,
    },
    Toggle(bool),
    Chips(Vec<DocumentValue>),
    Nested(GroupDraft),
}

/// What an accumulator computes for each group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccumulatorOp {
    Count,
    Sum,
    Avg,
}

impl AccumulatorOp {
    pub const ALL: [AccumulatorOp; 3] =
        [AccumulatorOp::Count, AccumulatorOp::Sum, AccumulatorOp::Avg];

    /// Whether the accumulator reads a field.
    pub fn takes_field(self) -> bool {
        self != AccumulatorOp::Count
    }
}

/// One named output of the group stage. The field stays when the operator
/// switches to `Count`, so switching back finds it again.
#[derive(Debug, Clone, PartialEq)]
pub struct AccumulatorDraft {
    pub id: NodeId,
    pub name: String,
    pub op: AccumulatorOp,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupStageDraft {
    pub id: NodeId,
    pub keys: Vec<String>,
    pub accumulators: Vec<AccumulatorDraft>,
}

/// Name of the first accumulator of a new group stage, and the stem of the
/// names of the next ones.
const DEFAULT_ACCUMULATOR_NAME: &str = "count";

#[derive(Debug, Clone, PartialEq)]
pub enum ProblemKind {
    MissingField,
    Value(ValueProblem),
    EmptyList,
    EmptyGroup,
    /// The spec itself cannot run (a path naming an operator, say).
    Spec(DocumentSpecProblem),
}

/// Why a node keeps the draft from becoming a spec.
#[derive(Debug, Clone, PartialEq)]
pub struct DraftProblem {
    pub node: NodeId,
    pub kind: ProblemKind,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BuilderDraft {
    next_id: NodeId,
    pub mode: DocumentQueryMode,
    pub filter: GroupDraft,
    pub projection: DocumentProjection,
    pub sort: Vec<DocumentSortKey>,
    pub limit: Option<u64>,
    pub skip: Option<u64>,
    /// The group stage; kept while in Find mode so Aggregate restores it.
    pub group: Option<GroupStageDraft>,
    /// How typed values are written for the connection's driver.
    syntax: ValueSyntax,
}

impl Default for BuilderDraft {
    fn default() -> Self {
        Self {
            next_id: 1,
            mode: DocumentQueryMode::Find,
            filter: GroupDraft {
                id: 0,
                combinator: DocumentCombinator::And,
                children: Vec::new(),
            },
            projection: DocumentProjection::default(),
            sort: Vec::new(),
            limit: None,
            skip: None,
            group: None,
            syntax: ValueSyntax::default(),
        }
    }
}

impl BuilderDraft {
    /// An empty draft whose values are written as `syntax` says.
    pub fn with_value_syntax(syntax: ValueSyntax) -> Self {
        Self {
            syntax,
            ..Self::default()
        }
    }

    pub fn value_syntax(&self) -> ValueSyntax {
        self.syntax
    }

    #[cfg(test)]
    pub fn from_spec(spec: &DocumentQuerySpec) -> Self {
        let mut draft = Self::default();
        draft.load_all(spec);
        draft
    }

    /// Replaces the whole draft with `spec`, its mode and group stage
    /// included, as when a saved query is opened.
    pub fn load_all(&mut self, spec: &DocumentQuerySpec) {
        self.load(spec);
        self.mode = spec.mode;
        self.group = spec
            .group
            .as_ref()
            .map(|stage| self.group_stage_from_spec(stage));
    }

    /// Replaces the find parts of the draft with `spec`: filter, projection,
    /// sort, limit and skip. The mode and the group stage stay, since the
    /// slots never hold them. Every node gets an id never used before, so
    /// inputs keyed by the old ids are dropped rather than reused.
    pub fn load(&mut self, spec: &DocumentQuerySpec) {
        self.filter = self.group_from_spec(&spec.filter);
        self.projection = spec.projection.clone();
        self.sort = spec.sort.clone();
        self.limit = spec.limit;
        self.skip = spec.skip;
    }

    fn allocate(&mut self) -> NodeId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn group_stage_from_spec(&mut self, stage: &DocumentGroupStage) -> GroupStageDraft {
        let id = self.allocate();
        let accumulators = stage
            .accumulators
            .iter()
            .map(|accumulator| {
                let (op, path) = match &accumulator.kind {
                    DocumentAccumulatorKind::Count => (AccumulatorOp::Count, String::new()),
                    DocumentAccumulatorKind::Sum { path } => (AccumulatorOp::Sum, path.clone()),
                    DocumentAccumulatorKind::Avg { path } => (AccumulatorOp::Avg, path.clone()),
                };
                AccumulatorDraft {
                    id: self.allocate(),
                    name: accumulator.name.clone(),
                    op,
                    path,
                }
            })
            .collect();

        GroupStageDraft {
            id,
            keys: stage.keys.clone(),
            accumulators,
        }
    }

    fn group_from_spec(&mut self, group: &DocumentFilterGroup) -> GroupDraft {
        let id = self.allocate();
        let children = group
            .children
            .iter()
            .map(|child| match child {
                DocumentFilterNode::Condition(condition) => {
                    NodeDraft::Condition(self.condition_from_spec(condition))
                }
                DocumentFilterNode::Group(nested) => NodeDraft::Group(self.group_from_spec(nested)),
            })
            .collect();

        GroupDraft {
            id,
            combinator: group.combinator,
            children,
        }
    }

    fn condition_from_spec(&mut self, condition: &DocumentCondition) -> ConditionDraft {
        let id = self.allocate();
        let kind = ScalarKind::of_value(&condition.value);

        let operand = match (&condition.value, value_editor(condition.operator, kind)) {
            (DocumentValue::Nested(body), _) => Operand::Nested(self.group_from_spec(body)),
            (DocumentValue::Bool(flag), ValueEditor::Toggle) => Operand::Toggle(*flag),
            (DocumentValue::List(items), ValueEditor::Chips(_)) => Operand::Chips(items.clone()),
            (value, _) => Operand::Text {
                text: format_value(value, kind, self.syntax),
                value: Ok(value.clone()),
            },
        };

        ConditionDraft {
            id,
            path: condition.path.clone(),
            operator: condition.operator,
            kind,
            operand,
        }
    }

    // ---- reading ---------------------------------------------------------

    /// The spec the draft describes, or every reason it cannot be one yet.
    pub fn to_spec(&self) -> Result<DocumentQuerySpec, Vec<DraftProblem>> {
        let mut problems = Vec::new();
        let filter = group_to_spec(&self.filter, false, &mut problems);

        let group = match (self.mode, &self.group) {
            (DocumentQueryMode::Aggregate, Some(stage)) => {
                Some(group_stage_to_spec(stage, &mut problems))
            }
            _ => None,
        };

        if !problems.is_empty() {
            return Err(problems);
        }

        let projection = match self.mode {
            DocumentQueryMode::Find => self.projection.clone(),
            DocumentQueryMode::Aggregate => DocumentProjection::default(),
        };

        let spec = DocumentQuerySpec {
            mode: self.mode,
            filter,
            projection,
            sort: self.sort.clone(),
            limit: self.limit,
            skip: self.skip,
            group,
        };

        let mut duplicates_seen: HashMap<String, usize> = HashMap::new();
        let spec_problems: Vec<DraftProblem> = spec
            .validate()
            .into_iter()
            .map(|problem| DraftProblem {
                node: self.problem_node(&problem, &mut duplicates_seen),
                kind: ProblemKind::Spec(problem),
            })
            .collect();

        if spec_problems.is_empty() {
            Ok(spec)
        } else {
            Err(spec_problems)
        }
    }

    /// The node a core problem is shown on: the accumulator it names, the
    /// group stage for the rest of its problems, the filter otherwise.
    /// `duplicates_seen` counts the duplicate-name problems already placed,
    /// so each lands on the next accumulator reusing the name.
    fn problem_node(
        &self,
        problem: &DocumentSpecProblem,
        duplicates_seen: &mut HashMap<String, usize>,
    ) -> NodeId {
        let Some(stage) = self
            .group
            .as_ref()
            .filter(|_| self.mode == DocumentQueryMode::Aggregate)
        else {
            return self.filter.id;
        };

        match problem {
            DocumentSpecProblem::AccumulatorNameEmpty { index } => stage
                .accumulators
                .get(*index)
                .map_or(stage.id, |accumulator| accumulator.id),
            DocumentSpecProblem::AccumulatorNameDuplicate { name } => {
                let seen = duplicates_seen.entry(name.clone()).or_insert(0);
                *seen += 1;
                accumulators_named(stage, name)
                    .nth(*seen)
                    .map_or(stage.id, |accumulator| accumulator.id)
            }
            DocumentSpecProblem::AccumulatorPathMissing { name } => {
                accumulators_named(stage, name.trim())
                    .next()
                    .map_or(stage.id, |accumulator| accumulator.id)
            }
            DocumentSpecProblem::PathNamesOperator { path } => {
                if let Some(accumulator) = stage.accumulators.iter().find(|accumulator| {
                    accumulator.op.takes_field() && accumulator.path.trim() == path
                }) {
                    accumulator.id
                } else if stage.keys.iter().any(|key| key == path) {
                    stage.id
                } else {
                    self.filter.id
                }
            }
            DocumentSpecProblem::ProjectionInAggregate
            | DocumentSpecProblem::AggregateWithoutGroup
            | DocumentSpecProblem::AggregateSortPathUnknown { .. } => stage.id,
            _ => self.filter.id,
        }
    }

    pub fn condition(&self, id: NodeId) -> Option<&ConditionDraft> {
        find_condition(&self.filter, id)
    }

    /// Path of the array an `ElemMatch` condition tests, joined for nested
    /// matches; empty for a condition on the document itself.
    pub fn condition_scope(&self, id: NodeId) -> Option<String> {
        scope_in_group(&self.filter, id, "")
    }

    /// Conditions on the document itself, nested groups included and the
    /// sub-conditions of an `ElemMatch` left out.
    pub fn condition_count(&self) -> usize {
        count_conditions(&self.filter)
    }

    /// Every condition, those inside `ElemMatch` bodies included, in
    /// display order.
    pub fn conditions(&self) -> Vec<&ConditionDraft> {
        let mut conditions = Vec::new();
        collect_conditions(&self.filter, &mut conditions);
        conditions
    }

    /// Every node id, for dropping inputs of removed nodes.
    pub fn node_ids(&self) -> Vec<NodeId> {
        let mut ids = Vec::new();
        collect_ids(&self.filter, &mut ids);
        ids
    }

    // ---- filter edits ----------------------------------------------------

    /// Appends an empty condition to `group`.
    pub fn add_condition(&mut self, group: NodeId) -> Option<NodeId> {
        let id = self.allocate();
        let target = find_group_mut(&mut self.filter, group)?;
        target
            .children
            .push(NodeDraft::Condition(empty_condition(id)));
        Some(id)
    }

    /// Appends a nested group with one empty condition to `group`. It
    /// combines the other way from its parent: nesting an `And` in an `And`
    /// adds nothing.
    pub fn add_group(&mut self, group: NodeId) -> Option<NodeId> {
        let group_id = self.allocate();
        let condition_id = self.allocate();
        let target = find_group_mut(&mut self.filter, group)?;

        let combinator = match target.combinator {
            DocumentCombinator::And => DocumentCombinator::Or,
            DocumentCombinator::Or => DocumentCombinator::And,
        };
        target.children.push(NodeDraft::Group(GroupDraft {
            id: group_id,
            combinator,
            children: vec![NodeDraft::Condition(empty_condition(condition_id))],
        }));

        Some(group_id)
    }

    /// Removes a condition or a nested group. The root group stays.
    pub fn remove_node(&mut self, id: NodeId) -> bool {
        remove_from_group(&mut self.filter, id)
    }

    pub fn set_combinator(&mut self, group: NodeId, combinator: DocumentCombinator) -> bool {
        match find_group_mut(&mut self.filter, group) {
            Some(target) if target.combinator != combinator => {
                target.combinator = combinator;
                true
            }
            _ => false,
        }
    }

    /// Points a condition at `path`, sampled with `types`. The value kind
    /// follows the field; an operator the field does not offer falls back to
    /// its first one.
    pub fn set_path(&mut self, id: NodeId, path: &str, types: &[DocumentFieldType]) -> bool {
        let Some(condition) = self.condition_mut(id) else {
            return false;
        };

        condition.path = path.trim().to_string();
        condition.kind = ScalarKind::for_types(types);

        let choices = operator_choices(types);
        if !choices.contains(&condition.operator)
            && let Some(first) = choices.first()
        {
            condition.operator = *first;
        }

        self.reshape_operand(id);
        true
    }

    pub fn set_operator(&mut self, id: NodeId, operator: DocumentOperator) -> bool {
        match self.condition_mut(id) {
            Some(condition) if condition.operator != operator => {
                condition.operator = operator;
            }
            _ => return false,
        }

        self.reshape_operand(id);
        true
    }

    /// Reads typed text as the condition's value.
    pub fn set_text(&mut self, id: NodeId, text: &str) -> bool {
        let syntax = self.syntax;
        let Some(condition) = self.condition_mut(id) else {
            return false;
        };

        let editor = condition.editor();
        match &mut condition.operand {
            Operand::Text {
                text: current,
                value,
            } => {
                *current = text.to_string();
                *value = read_text(editor, text, syntax);
                true
            }
            _ => false,
        }
    }

    pub fn set_toggle(&mut self, id: NodeId, flag: bool) -> bool {
        match self.condition_mut(id) {
            Some(ConditionDraft {
                operand: Operand::Toggle(current),
                ..
            }) => {
                *current = flag;
                true
            }
            _ => false,
        }
    }

    /// Reads the condition's values as `kind` from now on (typing a
    /// hexadecimal text as an object id, for one).
    pub fn set_kind(&mut self, id: NodeId, kind: ScalarKind) -> bool {
        match self.condition_mut(id) {
            Some(condition) if condition.kind != kind => {
                condition.kind = kind;
            }
            _ => return false,
        }

        self.reshape_operand(id);
        true
    }

    /// Adds a typed chip to a list operand.
    pub fn add_chip(&mut self, id: NodeId, text: &str) -> Result<(), ValueProblem> {
        let syntax = self.syntax;
        let Some(condition) = self.condition_mut(id) else {
            return Err(ValueProblem::Empty);
        };

        let kind = condition.kind;
        let Operand::Chips(items) = &mut condition.operand else {
            return Err(ValueProblem::Empty);
        };

        let value = parse_scalar(kind, text, syntax)?;
        items.push(value);
        Ok(())
    }

    pub fn remove_chip(&mut self, id: NodeId, index: usize) -> bool {
        match self.condition_mut(id) {
            Some(ConditionDraft {
                operand: Operand::Chips(items),
                ..
            }) if index < items.len() => {
                items.remove(index);
                true
            }
            _ => false,
        }
    }

    fn condition_mut(&mut self, id: NodeId) -> Option<&mut ConditionDraft> {
        find_condition_mut(&mut self.filter, id)
    }

    /// Converts a condition's operand to the shape its editor needs, keeping
    /// what it can of the old one.
    fn reshape_operand(&mut self, id: NodeId) {
        let Some(condition) = self.condition(id) else {
            return;
        };

        let editor = condition.editor();
        let kind = condition.kind;
        let current = condition.operand.clone();
        let syntax = self.syntax;

        let reshaped = match (editor, current) {
            (ValueEditor::Nested, Operand::Nested(body)) => Operand::Nested(body),
            (ValueEditor::Nested, _) => {
                let group_id = self.allocate();
                let condition_id = self.allocate();
                Operand::Nested(GroupDraft {
                    id: group_id,
                    combinator: DocumentCombinator::And,
                    children: vec![NodeDraft::Condition(empty_condition(condition_id))],
                })
            }
            (ValueEditor::Toggle, Operand::Toggle(flag)) => Operand::Toggle(flag),
            (ValueEditor::Toggle, Operand::Text { text, .. }) => {
                Operand::Toggle(text.trim() != "false")
            }
            (ValueEditor::Toggle, _) => Operand::Toggle(true),
            (ValueEditor::Chips(_), Operand::Chips(items)) => {
                let text = items
                    .iter()
                    .map(|item| format_value(item, kind, syntax))
                    .collect::<Vec<_>>();
                Operand::Chips(
                    text.iter()
                        .filter_map(|text| parse_scalar(kind, text, syntax).ok())
                        .collect(),
                )
            }
            (ValueEditor::Chips(_), Operand::Text { text, .. }) => {
                Operand::Chips(parse_scalar(kind, &text, syntax).into_iter().collect())
            }
            (ValueEditor::Chips(_), _) => Operand::Chips(Vec::new()),
            (editor, Operand::Text { text, .. }) => Operand::Text {
                value: read_text(editor, &text, syntax),
                text,
            },
            (editor, Operand::Chips(items)) => {
                let text = items
                    .first()
                    .map(|item| format_value(item, kind, syntax))
                    .unwrap_or_default();
                Operand::Text {
                    value: read_text(editor, &text, syntax),
                    text,
                }
            }
            (editor, Operand::Toggle(flag)) => {
                let text = flag.to_string();
                Operand::Text {
                    value: read_text(editor, &text, syntax),
                    text,
                }
            }
            (editor, Operand::Nested(_)) => Operand::Text {
                value: read_text(editor, "", syntax),
                text: String::new(),
            },
        };

        if let Some(condition) = self.condition_mut(id) {
            condition.operand = reshaped;
        }
    }

    // ---- mode and group stage --------------------------------------------

    /// Switches between Find and Aggregate. Aggregate needs a group stage:
    /// the one kept from before, or a new one counting the documents.
    pub fn set_mode(&mut self, mode: DocumentQueryMode) -> bool {
        if self.mode == mode {
            return false;
        }

        if mode == DocumentQueryMode::Aggregate && self.group.is_none() {
            let stage_id = self.allocate();
            let accumulator_id = self.allocate();
            self.group = Some(GroupStageDraft {
                id: stage_id,
                keys: Vec::new(),
                accumulators: vec![AccumulatorDraft {
                    id: accumulator_id,
                    name: DEFAULT_ACCUMULATOR_NAME.to_string(),
                    op: AccumulatorOp::Count,
                    path: String::new(),
                }],
            });
        }

        self.mode = mode;
        true
    }

    /// Drops the group stage and returns to Find.
    pub fn remove_group_stage(&mut self) -> bool {
        let had_stage = self.group.take().is_some();
        let changed_mode = self.mode != DocumentQueryMode::Find;
        self.mode = DocumentQueryMode::Find;
        had_stage || changed_mode
    }

    pub fn add_group_key(&mut self, path: &str) -> bool {
        let path = path.trim();
        let Some(stage) = self.group.as_mut() else {
            return false;
        };

        if path.is_empty() || stage.keys.iter().any(|key| key == path) {
            return false;
        }

        stage.keys.push(path.to_string());
        true
    }

    pub fn remove_group_key(&mut self, index: usize) -> bool {
        match self.group.as_mut() {
            Some(stage) if index < stage.keys.len() => {
                stage.keys.remove(index);
                true
            }
            _ => false,
        }
    }

    /// Appends a `Count` accumulator with a name no other one uses.
    pub fn add_accumulator(&mut self) -> Option<NodeId> {
        let id = self.allocate();
        let stage = self.group.as_mut()?;

        let taken = |name: &str| {
            stage
                .accumulators
                .iter()
                .any(|accumulator| accumulator.name.trim() == name)
        };
        let name = if taken(DEFAULT_ACCUMULATOR_NAME) {
            (2..)
                .map(|suffix| format!("{DEFAULT_ACCUMULATOR_NAME}_{suffix}"))
                .find(|candidate| !taken(candidate))
                .unwrap_or_default()
        } else {
            DEFAULT_ACCUMULATOR_NAME.to_string()
        };

        stage.accumulators.push(AccumulatorDraft {
            id,
            name,
            op: AccumulatorOp::Count,
            path: String::new(),
        });
        Some(id)
    }

    pub fn remove_accumulator(&mut self, id: NodeId) -> bool {
        let Some(stage) = self.group.as_mut() else {
            return false;
        };

        let before = stage.accumulators.len();
        stage
            .accumulators
            .retain(|accumulator| accumulator.id != id);
        stage.accumulators.len() != before
    }

    #[cfg(test)]
    pub fn accumulator(&self, id: NodeId) -> Option<&AccumulatorDraft> {
        self.group
            .as_ref()?
            .accumulators
            .iter()
            .find(|accumulator| accumulator.id == id)
    }

    fn accumulator_mut(&mut self, id: NodeId) -> Option<&mut AccumulatorDraft> {
        self.group
            .as_mut()?
            .accumulators
            .iter_mut()
            .find(|accumulator| accumulator.id == id)
    }

    pub fn set_accumulator_name(&mut self, id: NodeId, name: &str) -> bool {
        match self.accumulator_mut(id) {
            Some(accumulator) if accumulator.name != name => {
                accumulator.name = name.to_string();
                true
            }
            _ => false,
        }
    }

    pub fn set_accumulator_op(&mut self, id: NodeId, op: AccumulatorOp) -> bool {
        match self.accumulator_mut(id) {
            Some(accumulator) if accumulator.op != op => {
                accumulator.op = op;
                true
            }
            _ => false,
        }
    }

    pub fn set_accumulator_path(&mut self, id: NodeId, path: &str) -> bool {
        let path = path.trim();
        match self.accumulator_mut(id) {
            Some(accumulator) if accumulator.path != path => {
                accumulator.path = path.to_string();
                true
            }
            _ => false,
        }
    }

    /// What a sort key may name after the group stage: the group keys, then
    /// the accumulator names, each once.
    pub fn sort_choices(&self) -> Vec<String> {
        let Some(stage) = self.group.as_ref() else {
            return Vec::new();
        };

        let mut choices: Vec<String> = Vec::new();
        let names = stage.keys.iter().map(|key| key.as_str()).chain(
            stage
                .accumulators
                .iter()
                .map(|accumulator| accumulator.name.trim()),
        );

        for name in names {
            if !name.is_empty() && !choices.iter().any(|choice| choice == name) {
                choices.push(name.to_string());
            }
        }

        choices
    }

    // ---- projection ------------------------------------------------------

    /// Switches between returning and hiding the listed fields. Hiding
    /// `_id` is never allowed, so it leaves the list when excluding.
    pub fn set_projection_mode(&mut self, mode: DocumentProjectionMode) {
        self.projection.mode = mode;
        if mode == DocumentProjectionMode::Exclude {
            self.projection.fields.retain(|field| field != ID_FIELD);
        }
    }

    pub fn add_projection_field(&mut self, path: &str) -> bool {
        let path = path.trim();
        let hides_id = self.projection.mode == DocumentProjectionMode::Exclude && path == ID_FIELD;

        if path.is_empty() || hides_id || self.projection.fields.iter().any(|field| field == path) {
            return false;
        }

        self.projection.fields.push(path.to_string());
        true
    }

    pub fn remove_projection_field(&mut self, index: usize) -> bool {
        if index < self.projection.fields.len() {
            self.projection.fields.remove(index);
            true
        } else {
            false
        }
    }

    // ---- sort ------------------------------------------------------------

    pub fn add_sort_key(&mut self, path: &str) -> bool {
        let path = path.trim();
        if path.is_empty() || self.sort.iter().any(|key| key.path == path) {
            return false;
        }

        self.sort
            .push(DocumentSortKey::new(path, DocumentSortDirection::Ascending));
        true
    }

    pub fn remove_sort_key(&mut self, index: usize) -> bool {
        if index < self.sort.len() {
            self.sort.remove(index);
            true
        } else {
            false
        }
    }

    pub fn set_sort_direction(&mut self, index: usize, direction: DocumentSortDirection) -> bool {
        match self.sort.get_mut(index) {
            Some(key) if key.direction != direction => {
                key.direction = direction;
                true
            }
            _ => false,
        }
    }

    /// Moves the key at `from` to position `to`, shifting the keys between.
    pub fn move_sort_key(&mut self, from: usize, to: usize) -> bool {
        if from == to || from >= self.sort.len() || to >= self.sort.len() {
            return false;
        }

        let key = self.sort.remove(from);
        self.sort.insert(to, key);
        true
    }
}

fn empty_condition(id: NodeId) -> ConditionDraft {
    ConditionDraft {
        id,
        path: String::new(),
        operator: DocumentOperator::Eq,
        kind: ScalarKind::Auto,
        operand: Operand::Text {
            text: String::new(),
            value: Err(ValueProblem::Empty),
        },
    }
}

fn read_text(
    editor: ValueEditor,
    text: &str,
    syntax: ValueSyntax,
) -> Result<DocumentValue, ValueProblem> {
    match editor {
        ValueEditor::Scalar(kind) => parse_scalar(kind, text, syntax),
        ValueEditor::Pattern => parse_pattern(text),
        ValueEditor::Count => parse_count(text),
        ValueEditor::Toggle => parse_scalar(ScalarKind::Bool, text, syntax),
        ValueEditor::Chips(kind) => parse_scalar(kind, text, syntax),
        ValueEditor::Nested => Err(ValueProblem::Empty),
    }
}

fn accumulators_named<'a>(
    stage: &'a GroupStageDraft,
    name: &'a str,
) -> impl Iterator<Item = &'a AccumulatorDraft> {
    stage
        .accumulators
        .iter()
        .filter(move |accumulator| accumulator.name.trim() == name)
}

/// The group stage the draft describes. A `Sum` or `Avg` without a field is
/// reported on its accumulator instead of reaching the core checks.
fn group_stage_to_spec(
    stage: &GroupStageDraft,
    problems: &mut Vec<DraftProblem>,
) -> DocumentGroupStage {
    let accumulators = stage
        .accumulators
        .iter()
        .map(|accumulator| {
            let path = accumulator.path.trim().to_string();
            if accumulator.op.takes_field() && path.is_empty() {
                problems.push(DraftProblem {
                    node: accumulator.id,
                    kind: ProblemKind::MissingField,
                });
            }

            let kind = match accumulator.op {
                AccumulatorOp::Count => DocumentAccumulatorKind::Count,
                AccumulatorOp::Sum => DocumentAccumulatorKind::Sum { path },
                AccumulatorOp::Avg => DocumentAccumulatorKind::Avg { path },
            };

            DocumentAccumulator {
                name: accumulator.name.trim().to_string(),
                kind,
            }
        })
        .collect();

    DocumentGroupStage {
        keys: stage.keys.clone(),
        accumulators,
    }
}

fn group_to_spec(
    group: &GroupDraft,
    in_elem_match: bool,
    problems: &mut Vec<DraftProblem>,
) -> DocumentFilterGroup {
    let children = group
        .children
        .iter()
        .filter_map(|child| match child {
            NodeDraft::Condition(condition) => {
                condition_to_spec(condition, in_elem_match, problems)
                    .map(DocumentFilterNode::Condition)
            }
            NodeDraft::Group(nested) => {
                if nested.children.is_empty() {
                    problems.push(DraftProblem {
                        node: nested.id,
                        kind: ProblemKind::EmptyGroup,
                    });
                }
                Some(DocumentFilterNode::Group(group_to_spec(
                    nested,
                    in_elem_match,
                    problems,
                )))
            }
        })
        .collect();

    DocumentFilterGroup::new(group.combinator, children)
}

fn condition_to_spec(
    condition: &ConditionDraft,
    in_elem_match: bool,
    problems: &mut Vec<DraftProblem>,
) -> Option<DocumentCondition> {
    let problem = |kind| DraftProblem {
        node: condition.id,
        kind,
    };

    if !in_elem_match && condition.path.is_empty() {
        problems.push(problem(ProblemKind::MissingField));
        return None;
    }

    let value = match &condition.operand {
        Operand::Text { value, .. } => match value {
            Ok(value) => value.clone(),
            Err(value_problem) => {
                problems.push(problem(ProblemKind::Value(*value_problem)));
                return None;
            }
        },
        Operand::Toggle(flag) => DocumentValue::Bool(*flag),
        Operand::Chips(items) => {
            if items.is_empty() {
                problems.push(problem(ProblemKind::EmptyList));
                return None;
            }
            DocumentValue::List(items.clone())
        }
        Operand::Nested(body) => {
            if body.children.is_empty() {
                problems.push(problem(ProblemKind::EmptyGroup));
                return None;
            }
            DocumentValue::Nested(group_to_spec(body, true, problems))
        }
    };

    Some(DocumentCondition::new(
        condition.path.clone(),
        condition.operator,
        value,
    ))
}

fn find_condition(group: &GroupDraft, id: NodeId) -> Option<&ConditionDraft> {
    group.children.iter().find_map(|child| match child {
        NodeDraft::Condition(condition) if condition.id == id => Some(condition),
        NodeDraft::Condition(ConditionDraft {
            operand: Operand::Nested(body),
            ..
        }) => find_condition(body, id),
        NodeDraft::Condition(_) => None,
        NodeDraft::Group(nested) => find_condition(nested, id),
    })
}

fn find_condition_mut(group: &mut GroupDraft, id: NodeId) -> Option<&mut ConditionDraft> {
    group.children.iter_mut().find_map(|child| match child {
        NodeDraft::Condition(condition) => {
            if condition.id == id {
                return Some(condition);
            }
            match &mut condition.operand {
                Operand::Nested(body) => find_condition_mut(body, id),
                _ => None,
            }
        }
        NodeDraft::Group(nested) => find_condition_mut(nested, id),
    })
}

fn find_group_mut(group: &mut GroupDraft, id: NodeId) -> Option<&mut GroupDraft> {
    if group.id == id {
        return Some(group);
    }

    group.children.iter_mut().find_map(|child| match child {
        NodeDraft::Group(nested) => find_group_mut(nested, id),
        NodeDraft::Condition(ConditionDraft {
            operand: Operand::Nested(body),
            ..
        }) => find_group_mut(body, id),
        NodeDraft::Condition(_) => None,
    })
}

fn remove_from_group(group: &mut GroupDraft, id: NodeId) -> bool {
    let before = group.children.len();
    group.children.retain(|child| match child {
        NodeDraft::Condition(condition) => condition.id != id,
        NodeDraft::Group(nested) => nested.id != id,
    });
    if group.children.len() != before {
        return true;
    }

    group.children.iter_mut().any(|child| match child {
        NodeDraft::Group(nested) => remove_from_group(nested, id),
        NodeDraft::Condition(ConditionDraft {
            operand: Operand::Nested(body),
            ..
        }) => remove_from_group(body, id),
        NodeDraft::Condition(_) => false,
    })
}

fn scope_in_group(group: &GroupDraft, id: NodeId, scope: &str) -> Option<String> {
    group.children.iter().find_map(|child| match child {
        NodeDraft::Condition(condition) if condition.id == id => Some(scope.to_string()),
        NodeDraft::Condition(ConditionDraft {
            path,
            operand: Operand::Nested(body),
            ..
        }) => {
            let inner = match (scope.is_empty(), path.is_empty()) {
                (true, _) => path.clone(),
                (false, true) => scope.to_string(),
                (false, false) => format!("{scope}.{path}"),
            };
            scope_in_group(body, id, &inner)
        }
        NodeDraft::Condition(_) => None,
        NodeDraft::Group(nested) => scope_in_group(nested, id, scope),
    })
}

fn count_conditions(group: &GroupDraft) -> usize {
    group
        .children
        .iter()
        .map(|child| match child {
            NodeDraft::Condition(_) => 1,
            NodeDraft::Group(nested) => count_conditions(nested),
        })
        .sum()
}

fn collect_ids(group: &GroupDraft, ids: &mut Vec<NodeId>) {
    ids.push(group.id);
    for child in &group.children {
        match child {
            NodeDraft::Condition(condition) => {
                ids.push(condition.id);
                if let Operand::Nested(body) = &condition.operand {
                    collect_ids(body, ids);
                }
            }
            NodeDraft::Group(nested) => collect_ids(nested, ids),
        }
    }
}

fn collect_conditions<'a>(group: &'a GroupDraft, conditions: &mut Vec<&'a ConditionDraft>) {
    for child in &group.children {
        match child {
            NodeDraft::Condition(condition) => {
                conditions.push(condition);
                if let Operand::Nested(body) = &condition.operand {
                    collect_conditions(body, conditions);
                }
            }
            NodeDraft::Group(nested) => collect_conditions(nested, conditions),
        }
    }
}
