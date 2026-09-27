//! Driver-agnostic model of a document-collection query built visually.
//!
//! [`DocumentQuerySpec`] describes a find (filter, projection, sort, limit,
//! skip) or a grouping aggregation without any native syntax. A driver that
//! supports the visual builder implements [`DocumentQueryCodec`] to turn the
//! spec into its native query slots, pipeline and preview text, and to read
//! slots typed by hand back into a spec.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::DbError;

/// Whether the builder composes a find or a grouping aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DocumentQueryMode {
    #[default]
    Find,
    Aggregate,
}

/// How the conditions of a group combine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DocumentCombinator {
    #[default]
    And,
    Or,
}

/// A combination of conditions and nested groups.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct DocumentFilterGroup {
    pub combinator: DocumentCombinator,
    pub children: Vec<DocumentFilterNode>,
}

impl DocumentFilterGroup {
    pub fn new(combinator: DocumentCombinator, children: Vec<DocumentFilterNode>) -> Self {
        Self {
            combinator,
            children,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.children.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DocumentFilterNode {
    Condition(DocumentCondition),
    Group(DocumentFilterGroup),
}

/// One test on a field path.
///
/// Inside the group of an `ElemMatch` value, an empty `path` tests the array
/// element itself (`{ scores: { $elemMatch: { $gte: 80 } } }`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocumentCondition {
    pub path: String,
    pub operator: DocumentOperator,
    pub value: DocumentValue,
}

impl DocumentCondition {
    pub fn new(path: impl Into<String>, operator: DocumentOperator, value: DocumentValue) -> Self {
        Self {
            path: path.into(),
            operator,
            value,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DocumentOperator {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    Nin,
    Regex,
    Exists,
    ElemMatch,
    Size,
    All,
}

impl DocumentOperator {
    /// Every operator, in the order the builder lists them.
    pub const ALL: [DocumentOperator; 13] = [
        DocumentOperator::Eq,
        DocumentOperator::Ne,
        DocumentOperator::Gt,
        DocumentOperator::Gte,
        DocumentOperator::Lt,
        DocumentOperator::Lte,
        DocumentOperator::In,
        DocumentOperator::Nin,
        DocumentOperator::Regex,
        DocumentOperator::Exists,
        DocumentOperator::ElemMatch,
        DocumentOperator::Size,
        DocumentOperator::All,
    ];

    /// Whether `value` has the shape this operator takes.
    pub fn accepts(&self, value: &DocumentValue) -> bool {
        match self {
            DocumentOperator::Eq | DocumentOperator::Ne => {
                value.is_scalar()
                    || matches!(value, DocumentValue::List(items) if items.iter().all(DocumentValue::is_scalar))
            }
            DocumentOperator::Gt
            | DocumentOperator::Gte
            | DocumentOperator::Lt
            | DocumentOperator::Lte => value.is_scalar() && *value != DocumentValue::Null,
            DocumentOperator::In | DocumentOperator::Nin | DocumentOperator::All => {
                matches!(value, DocumentValue::List(items) if items.iter().all(DocumentValue::is_scalar))
            }
            DocumentOperator::Regex => matches!(value, DocumentValue::Regex { .. }),
            DocumentOperator::Exists => matches!(value, DocumentValue::Bool(_)),
            DocumentOperator::ElemMatch => matches!(value, DocumentValue::Nested(_)),
            DocumentOperator::Size => matches!(value, DocumentValue::Integer(size) if *size >= 0),
        }
    }
}

/// A typed operand of a condition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DocumentValue {
    String(String),
    Integer(i64),
    Decimal(f64),
    Bool(bool),
    Date(DateTime<Utc>),
    /// Hexadecimal object identifier.
    ObjectId(String),
    Regex {
        pattern: String,
        options: String,
    },
    List(Vec<DocumentValue>),
    /// The conditions an array element must meet (`ElemMatch`).
    Nested(DocumentFilterGroup),
    Null,
}

impl DocumentValue {
    /// A single plain value: not a regex, list or nested group.
    pub fn is_scalar(&self) -> bool {
        matches!(
            self,
            DocumentValue::String(_)
                | DocumentValue::Integer(_)
                | DocumentValue::Decimal(_)
                | DocumentValue::Bool(_)
                | DocumentValue::Date(_)
                | DocumentValue::ObjectId(_)
                | DocumentValue::Null
        )
    }
}

/// Field type as the builder groups native schema-sample type names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DocumentFieldType {
    String,
    Integer,
    Decimal,
    Date,
    Bool,
    ObjectId,
    Array,
    Object,
}

impl DocumentFieldType {
    /// Operators the builder offers for a field of this type.
    pub fn operators(&self) -> &'static [DocumentOperator] {
        use DocumentOperator::*;

        match self {
            DocumentFieldType::String => &[Eq, Ne, In, Nin, Regex, Exists],
            DocumentFieldType::Integer | DocumentFieldType::Decimal => {
                &[Eq, Ne, Gt, Gte, Lt, Lte, In, Exists]
            }
            DocumentFieldType::Date => &[Eq, Gt, Gte, Lt, Lte, Exists],
            DocumentFieldType::Bool => &[Eq, Exists],
            DocumentFieldType::ObjectId => &[Eq, Ne, In, Exists],
            DocumentFieldType::Array => &[ElemMatch, Size, All, Exists],
            DocumentFieldType::Object => &[Exists],
        }
    }

    /// Operators offered for a field sampled with several types: every
    /// operator any of the types allows, once, in [`DocumentOperator::ALL`]
    /// order.
    pub fn operators_for(types: &[DocumentFieldType]) -> Vec<DocumentOperator> {
        DocumentOperator::ALL
            .into_iter()
            .filter(|operator| {
                types
                    .iter()
                    .any(|field_type| field_type.operators().contains(operator))
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DocumentProjectionMode {
    #[default]
    Include,
    Exclude,
}

/// Fields a find returns. No fields means whole documents.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct DocumentProjection {
    pub mode: DocumentProjectionMode,
    pub fields: Vec<String>,
}

impl DocumentProjection {
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DocumentSortDirection {
    #[default]
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentSortKey {
    pub path: String,
    pub direction: DocumentSortDirection,
}

impl DocumentSortKey {
    pub fn new(path: impl Into<String>, direction: DocumentSortDirection) -> Self {
        Self {
            path: path.into(),
            direction,
        }
    }
}

/// A grouping stage: the documents are grouped by `keys` (all documents
/// together when empty) and each group reports its accumulators.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct DocumentGroupStage {
    pub keys: Vec<String>,
    pub accumulators: Vec<DocumentAccumulator>,
}

impl DocumentGroupStage {
    /// Whether `path` names a field of the group output: one of the group
    /// keys or an accumulator.
    pub fn sorts_on(&self, path: &str) -> bool {
        self.keys.iter().any(|key| key == path)
            || self
                .accumulators
                .iter()
                .any(|accumulator| accumulator.name.trim() == path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentAccumulator {
    /// Output field name.
    pub name: String,
    pub kind: DocumentAccumulatorKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DocumentAccumulatorKind {
    Count,
    Sum { path: String },
    Avg { path: String },
}

/// A document query as the visual builder composes it.
///
/// Saved queries store this as JSON, so it reads tolerantly: a field missing
/// from an older payload takes its default and an unknown one is skipped.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct DocumentQuerySpec {
    pub mode: DocumentQueryMode,
    pub filter: DocumentFilterGroup,
    pub projection: DocumentProjection,
    pub sort: Vec<DocumentSortKey>,
    pub limit: Option<u64>,
    pub skip: Option<u64>,
    pub group: Option<DocumentGroupStage>,
}

/// Why a [`DocumentQuerySpec`] cannot run.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DocumentSpecProblem {
    /// The projection hides `_id`, which find results need to stay editable.
    #[error("the projection cannot exclude `_id`")]
    ProjectionExcludesId,
    #[error("an aggregation does not take a projection")]
    ProjectionInAggregate,
    #[error("an aggregation needs a group stage")]
    AggregateWithoutGroup,
    #[error("accumulator {index} has no name", index = .index + 1)]
    AccumulatorNameEmpty { index: usize },
    #[error("accumulator name `{name}` is used more than once")]
    AccumulatorNameDuplicate { name: String },
    #[error("accumulator `{name}` needs a field path")]
    AccumulatorPathMissing { name: String },
    #[error("a condition has no field path")]
    ConditionPathEmpty,
    #[error("a nested group has no conditions")]
    EmptyGroup,
    /// A path segment starting with `$` would be read as an operator or a
    /// variable instead of a field.
    #[error("`{path}` is not a field path: no segment may start with `$`")]
    PathNamesOperator { path: String },
    /// After the group stage only group keys and accumulator names exist.
    #[error("`{path}` is neither a group key nor an accumulator, so it cannot be sorted on")]
    AggregateSortPathUnknown { path: String },
    #[error("operator {operator:?} does not take that value on `{path}`")]
    OperatorValueMismatch {
        path: String,
        operator: DocumentOperator,
    },
}

impl DocumentQuerySpec {
    /// Every reason the spec cannot run; empty when it can.
    pub fn validate(&self) -> Vec<DocumentSpecProblem> {
        let mut problems = Vec::new();

        validate_group(&self.filter, GroupPosition::Root, &mut problems);

        match self.mode {
            DocumentQueryMode::Find => {
                let hides_id = self.projection.mode == DocumentProjectionMode::Exclude
                    && self.projection.fields.iter().any(|field| field == "_id");
                if hides_id {
                    problems.push(DocumentSpecProblem::ProjectionExcludesId);
                }

                for field in &self.projection.fields {
                    check_field_path(field, &mut problems);
                }
            }
            DocumentQueryMode::Aggregate => {
                if !self.projection.is_empty() {
                    problems.push(DocumentSpecProblem::ProjectionInAggregate);
                }

                match &self.group {
                    Some(group) => {
                        for key in &group.keys {
                            check_field_path(key, &mut problems);
                        }
                        validate_accumulators(&group.accumulators, &mut problems);
                    }
                    None => problems.push(DocumentSpecProblem::AggregateWithoutGroup),
                }
            }
        }

        for key in &self.sort {
            check_field_path(&key.path, &mut problems);
        }

        if self.mode == DocumentQueryMode::Aggregate
            && let Some(group) = &self.group
        {
            for key in &self.sort {
                if !group.sorts_on(&key.path) {
                    problems.push(DocumentSpecProblem::AggregateSortPathUnknown {
                        path: key.path.clone(),
                    });
                }
            }
        }

        problems
    }

    /// Whether the result rows map back to stored documents, so they can be
    /// edited in place.
    pub fn is_editable_result(&self) -> bool {
        self.mode == DocumentQueryMode::Find
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GroupPosition {
    Root,
    Nested,
    ElemMatchBody,
}

fn validate_group(
    group: &DocumentFilterGroup,
    position: GroupPosition,
    problems: &mut Vec<DocumentSpecProblem>,
) {
    if position != GroupPosition::Root && group.is_empty() {
        problems.push(DocumentSpecProblem::EmptyGroup);
    }

    for child in &group.children {
        match child {
            DocumentFilterNode::Condition(condition) => {
                validate_condition(condition, position, problems)
            }
            DocumentFilterNode::Group(nested) => {
                let nested_position = match position {
                    GroupPosition::ElemMatchBody => GroupPosition::ElemMatchBody,
                    GroupPosition::Root | GroupPosition::Nested => GroupPosition::Nested,
                };
                validate_group(nested, nested_position, problems);
            }
        }
    }
}

fn validate_condition(
    condition: &DocumentCondition,
    position: GroupPosition,
    problems: &mut Vec<DocumentSpecProblem>,
) {
    if position != GroupPosition::ElemMatchBody && condition.path.trim().is_empty() {
        problems.push(DocumentSpecProblem::ConditionPathEmpty);
    }

    check_field_path(&condition.path, problems);

    if !condition.operator.accepts(&condition.value) {
        problems.push(DocumentSpecProblem::OperatorValueMismatch {
            path: condition.path.clone(),
            operator: condition.operator,
        });
        return;
    }

    if let DocumentValue::Nested(body) = &condition.value {
        validate_group(body, GroupPosition::ElemMatchBody, problems);
    }
}

/// Reports `path` when one of its dotted segments starts with `$`, which the
/// driver would read as an operator (`$where`) or a variable (`$$ROOT`).
fn check_field_path(path: &str, problems: &mut Vec<DocumentSpecProblem>) {
    if path
        .split('.')
        .any(|segment| segment.trim_start().starts_with('$'))
    {
        problems.push(DocumentSpecProblem::PathNamesOperator {
            path: path.to_string(),
        });
    }
}

fn validate_accumulators(
    accumulators: &[DocumentAccumulator],
    problems: &mut Vec<DocumentSpecProblem>,
) {
    let mut seen_names = HashSet::new();

    for (index, accumulator) in accumulators.iter().enumerate() {
        let name = accumulator.name.trim();

        if name.is_empty() {
            problems.push(DocumentSpecProblem::AccumulatorNameEmpty { index });
        } else if !seen_names.insert(name) {
            problems.push(DocumentSpecProblem::AccumulatorNameDuplicate {
                name: name.to_string(),
            });
        }

        let path = match &accumulator.kind {
            DocumentAccumulatorKind::Count => None,
            DocumentAccumulatorKind::Sum { path } | DocumentAccumulatorKind::Avg { path } => {
                Some(path)
            }
        };
        if let Some(path) = path {
            if path.trim().is_empty() {
                problems.push(DocumentSpecProblem::AccumulatorPathMissing {
                    name: accumulator.name.clone(),
                });
            } else {
                check_field_path(path.trim(), problems);
            }
        }
    }
}

/// Native find slots, as the document query bar holds them. Each text is the
/// driver's native document syntax, `{}` when empty.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DocumentFindSlots {
    pub filter: String,
    pub projection: String,
    pub sort: String,
    pub limit: Option<u64>,
    pub skip: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DocumentSlot {
    Filter,
    Projection,
    Sort,
}

/// Part of a slot the builder cannot represent, kept as native text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnrepresentableClause {
    pub slot: DocumentSlot,
    pub text: String,
}

/// A spec read back from slots, with the clauses it could not hold.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DocumentSlotParse {
    pub spec: DocumentQuerySpec,
    pub unrepresentable: Vec<UnrepresentableClause>,
}

impl DocumentSlotParse {
    /// Whether every clause of the slots made it into the spec.
    pub fn is_complete(&self) -> bool {
        self.unrepresentable.is_empty()
    }
}

/// Converts [`DocumentQuerySpec`]s to and from a driver's native query syntax.
/// Connections that offer it report `DocumentFeatures::VISUAL_BUILDER`.
pub trait DocumentQueryCodec: Send + Sync {
    /// Find slots for a spec in `Find` mode.
    fn render_find(&self, spec: &DocumentQuerySpec) -> Result<DocumentFindSlots, DbError>;

    /// Native pipeline text for a spec in `Aggregate` mode.
    fn render_pipeline(&self, spec: &DocumentQuerySpec) -> Result<String, DbError>;

    /// Human-readable query text for the preview, in either mode.
    fn render_preview(&self, spec: &DocumentQuerySpec, collection: &str)
    -> Result<String, DbError>;

    /// Reads find slots into a spec, keeping what the spec cannot hold as
    /// native text.
    fn parse_find(&self, slots: &DocumentFindSlots) -> DocumentSlotParse;

    /// Builder type of a native schema-sample type name, `None` when the
    /// builder has no operators for it.
    fn field_type(&self, type_name: &str) -> Option<DocumentFieldType>;
}

#[cfg(test)]
mod tests {
    use super::*;

    use DocumentOperator::*;

    fn condition(
        path: &str,
        operator: DocumentOperator,
        value: DocumentValue,
    ) -> DocumentFilterNode {
        DocumentFilterNode::Condition(DocumentCondition::new(path, operator, value))
    }

    fn find_with(children: Vec<DocumentFilterNode>) -> DocumentQuerySpec {
        DocumentQuerySpec {
            filter: DocumentFilterGroup::new(DocumentCombinator::And, children),
            ..DocumentQuerySpec::default()
        }
    }

    fn aggregate_with(accumulators: Vec<DocumentAccumulator>) -> DocumentQuerySpec {
        DocumentQuerySpec {
            mode: DocumentQueryMode::Aggregate,
            group: Some(DocumentGroupStage {
                keys: vec!["status".to_string()],
                accumulators,
            }),
            ..DocumentQuerySpec::default()
        }
    }

    fn accumulator(name: &str, kind: DocumentAccumulatorKind) -> DocumentAccumulator {
        DocumentAccumulator {
            name: name.to_string(),
            kind,
        }
    }

    #[test]
    fn operator_tables_follow_the_design() {
        assert_eq!(
            DocumentFieldType::String.operators(),
            &[Eq, Ne, In, Nin, Regex, Exists]
        );
        assert_eq!(
            DocumentFieldType::Integer.operators(),
            &[Eq, Ne, Gt, Gte, Lt, Lte, In, Exists]
        );
        assert_eq!(
            DocumentFieldType::Decimal.operators(),
            &[Eq, Ne, Gt, Gte, Lt, Lte, In, Exists]
        );
        assert_eq!(
            DocumentFieldType::Date.operators(),
            &[Eq, Gt, Gte, Lt, Lte, Exists]
        );
        assert_eq!(DocumentFieldType::Bool.operators(), &[Eq, Exists]);
        assert_eq!(
            DocumentFieldType::ObjectId.operators(),
            &[Eq, Ne, In, Exists]
        );
        assert_eq!(
            DocumentFieldType::Array.operators(),
            &[ElemMatch, Size, All, Exists]
        );
        assert_eq!(DocumentFieldType::Object.operators(), &[Exists]);
    }

    #[test]
    fn mixed_types_offer_the_union_once_in_stable_order() {
        let operators =
            DocumentFieldType::operators_for(&[DocumentFieldType::Bool, DocumentFieldType::String]);
        assert_eq!(operators, vec![Eq, Ne, In, Nin, Regex, Exists]);

        let operators = DocumentFieldType::operators_for(&[
            DocumentFieldType::Array,
            DocumentFieldType::Date,
            DocumentFieldType::Array,
        ]);
        assert_eq!(
            operators,
            vec![Eq, Gt, Gte, Lt, Lte, Exists, ElemMatch, Size, All]
        );

        assert!(DocumentFieldType::operators_for(&[]).is_empty());
    }

    #[test]
    fn a_plain_find_is_valid_and_editable() {
        let spec = find_with(vec![condition("age", Gt, DocumentValue::Integer(30))]);

        assert!(spec.validate().is_empty());
        assert!(spec.is_editable_result());
    }

    #[test]
    fn projection_cannot_exclude_id() {
        let mut spec = DocumentQuerySpec {
            projection: DocumentProjection {
                mode: DocumentProjectionMode::Exclude,
                fields: vec!["secret".to_string(), "_id".to_string()],
            },
            ..DocumentQuerySpec::default()
        };
        assert_eq!(
            spec.validate(),
            vec![DocumentSpecProblem::ProjectionExcludesId]
        );

        spec.projection.mode = DocumentProjectionMode::Include;
        assert!(spec.validate().is_empty());
    }

    #[test]
    fn aggregate_takes_no_projection_and_needs_a_group() {
        let spec = DocumentQuerySpec {
            mode: DocumentQueryMode::Aggregate,
            projection: DocumentProjection {
                mode: DocumentProjectionMode::Include,
                fields: vec!["name".to_string()],
            },
            ..DocumentQuerySpec::default()
        };

        assert_eq!(
            spec.validate(),
            vec![
                DocumentSpecProblem::ProjectionInAggregate,
                DocumentSpecProblem::AggregateWithoutGroup,
            ]
        );
        assert!(!spec.is_editable_result());
    }

    #[test]
    fn a_complete_aggregate_is_valid_and_read_only() {
        let spec = aggregate_with(vec![
            accumulator("total", DocumentAccumulatorKind::Count),
            accumulator(
                "revenue",
                DocumentAccumulatorKind::Sum {
                    path: "amount".to_string(),
                },
            ),
        ]);

        assert!(spec.validate().is_empty());
        assert!(!spec.is_editable_result());
    }

    #[test]
    fn sum_and_avg_need_a_path() {
        let spec = aggregate_with(vec![
            accumulator(
                "revenue",
                DocumentAccumulatorKind::Sum {
                    path: String::new(),
                },
            ),
            accumulator(
                "mean",
                DocumentAccumulatorKind::Avg {
                    path: "  ".to_string(),
                },
            ),
        ]);

        assert_eq!(
            spec.validate(),
            vec![
                DocumentSpecProblem::AccumulatorPathMissing {
                    name: "revenue".to_string()
                },
                DocumentSpecProblem::AccumulatorPathMissing {
                    name: "mean".to_string()
                },
            ]
        );
    }

    #[test]
    fn accumulator_names_are_unique_and_not_empty() {
        let spec = aggregate_with(vec![
            accumulator("total", DocumentAccumulatorKind::Count),
            accumulator(" ", DocumentAccumulatorKind::Count),
            accumulator("total", DocumentAccumulatorKind::Count),
        ]);

        assert_eq!(
            spec.validate(),
            vec![
                DocumentSpecProblem::AccumulatorNameEmpty { index: 1 },
                DocumentSpecProblem::AccumulatorNameDuplicate {
                    name: "total".to_string()
                },
            ]
        );
    }

    #[test]
    fn operators_need_a_matching_value_shape() {
        let list = DocumentValue::List(vec![DocumentValue::String("a".to_string())]);
        let nested = DocumentValue::Nested(DocumentFilterGroup::new(
            DocumentCombinator::And,
            vec![condition("", Gte, DocumentValue::Integer(80))],
        ));
        let regex = DocumentValue::Regex {
            pattern: "^a".to_string(),
            options: "i".to_string(),
        };

        let accepted = [
            (Eq, DocumentValue::String("a".to_string())),
            (Eq, DocumentValue::Null),
            (
                Ne,
                DocumentValue::ObjectId("64b7f0000000000000000000".to_string()),
            ),
            (Gt, DocumentValue::Date(DateTime::<Utc>::UNIX_EPOCH)),
            (Lte, DocumentValue::Decimal(1.5)),
            (In, list.clone()),
            (Nin, list.clone()),
            (All, list.clone()),
            (Regex, regex.clone()),
            (Exists, DocumentValue::Bool(true)),
            (ElemMatch, nested.clone()),
            (Size, DocumentValue::Integer(3)),
        ];
        for (operator, value) in accepted {
            let spec = find_with(vec![condition("field", operator, value.clone())]);
            assert!(
                spec.validate().is_empty(),
                "{operator:?} should accept {value:?}"
            );
        }

        let rejected = [
            (Eq, regex.clone()),
            (Eq, nested.clone()),
            (Gt, DocumentValue::Null),
            (Gt, list.clone()),
            (In, DocumentValue::String("a".to_string())),
            (Nin, DocumentValue::Integer(1)),
            (All, DocumentValue::List(vec![nested.clone()])),
            (In, DocumentValue::List(vec![regex.clone()])),
            (Regex, DocumentValue::String("^a".to_string())),
            (Exists, DocumentValue::Integer(1)),
            (ElemMatch, list.clone()),
            (Size, DocumentValue::Decimal(2.0)),
            (Size, DocumentValue::Integer(-1)),
        ];
        for (operator, value) in rejected {
            let spec = find_with(vec![condition("field", operator, value.clone())]);
            assert_eq!(
                spec.validate(),
                vec![DocumentSpecProblem::OperatorValueMismatch {
                    path: "field".to_string(),
                    operator,
                }],
                "{operator:?} should reject {value:?}"
            );
        }
    }

    #[test]
    fn paths_cannot_name_operators() {
        let mut spec = find_with(vec![
            condition(
                "$where",
                Eq,
                DocumentValue::String("sleep(1000)".to_string()),
            ),
            condition("address.$city", Eq, DocumentValue::Null),
            condition(
                "items",
                ElemMatch,
                DocumentValue::Nested(DocumentFilterGroup::new(
                    DocumentCombinator::And,
                    vec![
                        condition("$expr", Exists, DocumentValue::Bool(true)),
                        condition("", Gt, DocumentValue::Integer(1)),
                    ],
                )),
            ),
        ]);
        spec.projection.fields = vec!["name".to_string(), "$secret".to_string()];
        spec.sort = vec![DocumentSortKey::new(
            "$natural",
            DocumentSortDirection::Ascending,
        )];

        let problem = |path: &str| DocumentSpecProblem::PathNamesOperator {
            path: path.to_string(),
        };
        assert_eq!(
            spec.validate(),
            vec![
                problem("$where"),
                problem("address.$city"),
                problem("$expr"),
                problem("$secret"),
                problem("$natural"),
            ]
        );

        let mut spec = aggregate_with(vec![
            accumulator(
                "revenue",
                DocumentAccumulatorKind::Sum {
                    path: "$total".to_string(),
                },
            ),
            accumulator(
                "mean",
                DocumentAccumulatorKind::Avg {
                    path: "a.$b".to_string(),
                },
            ),
        ]);
        spec.group.as_mut().unwrap().keys = vec!["$status".to_string()];
        assert_eq!(
            spec.validate(),
            vec![problem("$status"), problem("$total"), problem("a.$b")]
        );
    }

    #[test]
    fn equality_lists_hold_only_plain_values() {
        let regex = DocumentValue::Regex {
            pattern: "^a".to_string(),
            options: String::new(),
        };
        let nested_list = DocumentValue::List(vec![DocumentValue::Integer(1)]);

        for (operator, value) in [
            (Eq, DocumentValue::List(vec![regex])),
            (Ne, DocumentValue::List(vec![nested_list])),
        ] {
            let spec = find_with(vec![condition("tags", operator, value)]);
            assert_eq!(
                spec.validate(),
                vec![DocumentSpecProblem::OperatorValueMismatch {
                    path: "tags".to_string(),
                    operator,
                }]
            );
        }
    }

    #[test]
    fn aggregate_sorts_only_on_group_keys_and_accumulators() {
        let mut spec = aggregate_with(vec![accumulator("orders", DocumentAccumulatorKind::Count)]);
        spec.sort = vec![
            DocumentSortKey::new("status", DocumentSortDirection::Ascending),
            DocumentSortKey::new("orders", DocumentSortDirection::Descending),
        ];
        assert!(spec.validate().is_empty());

        spec.sort.push(DocumentSortKey::new(
            "amount",
            DocumentSortDirection::Ascending,
        ));
        assert_eq!(
            spec.validate(),
            vec![DocumentSpecProblem::AggregateSortPathUnknown {
                path: "amount".to_string()
            }]
        );

        let mut find = find_with(vec![]);
        find.sort = spec.sort.clone();
        assert!(find.validate().is_empty());
    }

    #[test]
    fn nested_groups_and_elem_match_bodies_are_checked() {
        let spec = find_with(vec![
            condition("", Eq, DocumentValue::Integer(1)),
            DocumentFilterNode::Group(DocumentFilterGroup::new(DocumentCombinator::Or, vec![])),
            DocumentFilterNode::Group(DocumentFilterGroup::new(
                DocumentCombinator::Or,
                vec![condition(
                    "items",
                    ElemMatch,
                    DocumentValue::Nested(DocumentFilterGroup::new(
                        DocumentCombinator::And,
                        vec![
                            condition("", Gte, DocumentValue::Integer(80)),
                            condition("qty", Size, DocumentValue::Bool(true)),
                        ],
                    )),
                )],
            )),
        ]);

        assert_eq!(
            spec.validate(),
            vec![
                DocumentSpecProblem::ConditionPathEmpty,
                DocumentSpecProblem::EmptyGroup,
                DocumentSpecProblem::OperatorValueMismatch {
                    path: "qty".to_string(),
                    operator: Size,
                },
            ]
        );
    }

    #[test]
    fn a_spec_survives_a_json_round_trip() {
        let spec = DocumentQuerySpec {
            mode: DocumentQueryMode::Aggregate,
            filter: DocumentFilterGroup::new(
                DocumentCombinator::Or,
                vec![
                    condition(
                        "status",
                        In,
                        DocumentValue::List(vec![
                            DocumentValue::String("failed".to_string()),
                            DocumentValue::Null,
                        ]),
                    ),
                    condition(
                        "_id",
                        Eq,
                        DocumentValue::ObjectId("66f0c2a1b2c3d4e5f6a7b8c9".to_string()),
                    ),
                ],
            ),
            projection: DocumentProjection {
                mode: DocumentProjectionMode::Exclude,
                fields: vec!["secret".to_string()],
            },
            sort: vec![DocumentSortKey::new(
                "total",
                DocumentSortDirection::Descending,
            )],
            limit: Some(50),
            skip: Some(10),
            group: Some(DocumentGroupStage {
                keys: vec!["status".to_string()],
                accumulators: vec![DocumentAccumulator {
                    name: "revenue".to_string(),
                    kind: DocumentAccumulatorKind::Sum {
                        path: "total".to_string(),
                    },
                }],
            }),
        };

        let text = serde_json::to_string(&spec).expect("serializes");
        let back: DocumentQuerySpec = serde_json::from_str(&text).expect("deserializes");

        assert_eq!(back, spec);
    }

    #[test]
    fn a_stored_spec_missing_newer_fields_or_holding_unknown_ones_still_reads() {
        let older = r#"{
            "mode": "Find",
            "filter": { "combinator": "And", "children": [
                { "Condition": { "path": "age", "operator": "Gt", "value": { "Integer": 30 } } }
            ] },
            "limit": 20
        }"#;

        let spec: DocumentQuerySpec = serde_json::from_str(older).expect("an older payload reads");
        assert_eq!(spec.limit, Some(20));
        assert_eq!(spec.projection, DocumentProjection::default());
        assert!(spec.sort.is_empty());
        assert_eq!(spec.skip, None);
        assert_eq!(spec.group, None);
        assert_eq!(spec.filter.children.len(), 1);

        let newer = r#"{
            "mode": "Aggregate",
            "filter": { "children": [], "future_flag": true },
            "group": { "keys": ["status"], "future_stage_option": 3 },
            "future_field": { "anything": [1, 2] }
        }"#;

        let spec: DocumentQuerySpec =
            serde_json::from_str(newer).expect("unknown fields are skipped");
        assert_eq!(spec.mode, DocumentQueryMode::Aggregate);
        assert_eq!(spec.filter.combinator, DocumentCombinator::And);
        assert_eq!(
            spec.group,
            Some(DocumentGroupStage {
                keys: vec!["status".to_string()],
                accumulators: vec![],
            })
        );
    }
}
