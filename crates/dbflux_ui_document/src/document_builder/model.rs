//! The builder's editable copy of a find query.
//!
//! [`BuilderDraft`] mirrors [`DocumentQuerySpec`] with a stable id on every
//! group and condition, so the panel can key its inputs by node, and keeps
//! each operand as the text the user typed next to the value it reads as.
//! A draft with an unfinished condition cannot become a spec; it reports
//! [`DraftProblem`]s instead, and the slots keep the last query that could.

use dbflux_core::{
    DocumentCombinator, DocumentCondition, DocumentFieldType, DocumentFilterGroup,
    DocumentFilterNode, DocumentOperator, DocumentProjection, DocumentProjectionMode,
    DocumentQuerySpec, DocumentSortDirection, DocumentSortKey, DocumentSpecProblem, DocumentValue,
};

use super::values::{
    ScalarKind, ValueEditor, ValueProblem, format_value, operator_choices, parse_count,
    parse_pattern, parse_scalar, value_editor,
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
    pub filter: GroupDraft,
    pub projection: DocumentProjection,
    pub sort: Vec<DocumentSortKey>,
    pub limit: Option<u64>,
    pub skip: Option<u64>,
}

impl Default for BuilderDraft {
    fn default() -> Self {
        Self {
            next_id: 1,
            filter: GroupDraft {
                id: 0,
                combinator: DocumentCombinator::And,
                children: Vec::new(),
            },
            projection: DocumentProjection::default(),
            sort: Vec::new(),
            limit: None,
            skip: None,
        }
    }
}

impl BuilderDraft {
    #[cfg(test)]
    pub fn from_spec(spec: &DocumentQuerySpec) -> Self {
        let mut draft = Self::default();
        draft.load(spec);
        draft
    }

    /// Replaces the draft with `spec`. Every node gets an id never used
    /// before, so inputs keyed by the old ids are dropped rather than reused.
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
                text: format_value(value, kind),
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

        if !problems.is_empty() {
            return Err(problems);
        }

        let spec = DocumentQuerySpec {
            filter,
            projection: self.projection.clone(),
            sort: self.sort.clone(),
            limit: self.limit,
            skip: self.skip,
            ..DocumentQuerySpec::default()
        };

        let spec_problems: Vec<DraftProblem> = spec
            .validate()
            .into_iter()
            .map(|problem| DraftProblem {
                node: self.filter.id,
                kind: ProblemKind::Spec(problem),
            })
            .collect();

        if spec_problems.is_empty() {
            Ok(spec)
        } else {
            Err(spec_problems)
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
                *value = read_text(editor, text);
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
        let Some(condition) = self.condition_mut(id) else {
            return Err(ValueProblem::Empty);
        };

        let kind = condition.kind;
        let Operand::Chips(items) = &mut condition.operand else {
            return Err(ValueProblem::Empty);
        };

        let value = parse_scalar(kind, text)?;
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
                    .map(|item| format_value(item, kind))
                    .collect::<Vec<_>>();
                Operand::Chips(
                    text.iter()
                        .filter_map(|text| parse_scalar(kind, text).ok())
                        .collect(),
                )
            }
            (ValueEditor::Chips(_), Operand::Text { text, .. }) => {
                Operand::Chips(parse_scalar(kind, &text).into_iter().collect())
            }
            (ValueEditor::Chips(_), _) => Operand::Chips(Vec::new()),
            (editor, Operand::Text { text, .. }) => Operand::Text {
                value: read_text(editor, &text),
                text,
            },
            (editor, Operand::Chips(items)) => {
                let text = items
                    .first()
                    .map(|item| format_value(item, kind))
                    .unwrap_or_default();
                Operand::Text {
                    value: read_text(editor, &text),
                    text,
                }
            }
            (editor, Operand::Toggle(flag)) => {
                let text = flag.to_string();
                Operand::Text {
                    value: read_text(editor, &text),
                    text,
                }
            }
            (editor, Operand::Nested(_)) => Operand::Text {
                value: read_text(editor, ""),
                text: String::new(),
            },
        };

        if let Some(condition) = self.condition_mut(id) {
            condition.operand = reshaped;
        }
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

fn read_text(editor: ValueEditor, text: &str) -> Result<DocumentValue, ValueProblem> {
    match editor {
        ValueEditor::Scalar(kind) => parse_scalar(kind, text),
        ValueEditor::Pattern => parse_pattern(text),
        ValueEditor::Count => parse_count(text),
        ValueEditor::Toggle => parse_scalar(ScalarKind::Bool, text),
        ValueEditor::Chips(kind) => parse_scalar(kind, text),
        ValueEditor::Nested => Err(ValueProblem::Empty),
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
