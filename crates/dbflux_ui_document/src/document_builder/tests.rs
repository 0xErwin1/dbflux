#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use chrono::{TimeZone, Utc};
use dbflux_core::{
    CollectionSchemaSample, DocumentAccumulator, DocumentAccumulatorKind, DocumentCombinator,
    DocumentCondition, DocumentFieldType, DocumentFilterGroup, DocumentFilterNode,
    DocumentFindSlots, DocumentGroupStage, DocumentOperator, DocumentProjection,
    DocumentProjectionMode, DocumentQueryMode, DocumentQuerySpec, DocumentSlot, DocumentSlotParse,
    DocumentSortDirection, DocumentSortKey, DocumentSpecProblem, DocumentValue, FieldSchemaStats,
    FieldTypeShare, FieldValueSummary, UnrepresentableClause,
};

use super::catalog::FieldCatalog;
use super::model::{AccumulatorOp, BuilderDraft, DraftProblem, NodeDraft, Operand, ProblemKind};
use super::sync::{SlotSync, SlotWrite};
use super::values::{
    ScalarKind, ValueEditor, ValueProblem, format_value, operator_choices, operator_ranges,
    parse_pattern, parse_scalar, value_editor,
};

use DocumentOperator::*;

const OBJECT_ID: &str = "66f0c2a1b2c3d4e5f6a7b8c9";

// ---- helpers ------------------------------------------------------------

fn stats(path: &str, presence: u64, types: &[(&str, u64)]) -> FieldSchemaStats {
    FieldSchemaStats {
        path: path.to_string(),
        presence,
        types: types
            .iter()
            .map(|(type_name, count)| FieldTypeShare {
                type_name: type_name.to_string(),
                count: *count,
            })
            .collect(),
        summary: FieldValueSummary::Empty,
    }
}

fn field_type(type_name: &str) -> Option<DocumentFieldType> {
    match type_name {
        "String" => Some(DocumentFieldType::String),
        "Int32" => Some(DocumentFieldType::Integer),
        "Double" => Some(DocumentFieldType::Decimal),
        "Date" => Some(DocumentFieldType::Date),
        "Boolean" => Some(DocumentFieldType::Bool),
        "ObjectId" => Some(DocumentFieldType::ObjectId),
        "Array" => Some(DocumentFieldType::Array),
        "Object" => Some(DocumentFieldType::Object),
        _ => None,
    }
}

fn sample() -> CollectionSchemaSample {
    CollectionSchemaSample {
        sampled_documents: 1_000,
        total_documents: Some(48_211),
        fields: vec![
            stats("_id", 1_000, &[("ObjectId", 1_000)]),
            stats("customer", 1_000, &[("Object", 1_000)]),
            stats("total", 1_000, &[("Double", 1_000)]),
            stats("customer.email", 1_000, &[("String", 1_000)]),
            stats("items", 1_000, &[("Array", 1_000)]),
            stats("customer.tier", 980, &[("String", 980)]),
            stats("items.sku", 1_000, &[("String", 3_000)]),
            stats(
                "code",
                340,
                &[("String", 200), ("Int32", 140), ("Null", 10)],
            ),
        ],
    }
}

fn catalog() -> FieldCatalog {
    FieldCatalog::new(&sample(), field_type)
}

fn condition(path: &str, operator: DocumentOperator, value: DocumentValue) -> DocumentFilterNode {
    DocumentFilterNode::Condition(DocumentCondition::new(path, operator, value))
}

fn group(combinator: DocumentCombinator, children: Vec<DocumentFilterNode>) -> DocumentFilterGroup {
    DocumentFilterGroup::new(combinator, children)
}

fn text(value: &str) -> DocumentValue {
    DocumentValue::String(value.to_string())
}

fn slots(filter: &str, projection: &str, sort: &str, limit: Option<u64>) -> DocumentFindSlots {
    DocumentFindSlots {
        filter: filter.to_string(),
        projection: projection.to_string(),
        sort: sort.to_string(),
        limit,
        skip: None,
    }
}

fn clause(slot: DocumentSlot, text: &str) -> UnrepresentableClause {
    UnrepresentableClause {
        slot,
        text: text.to_string(),
    }
}

/// A draft with one root condition on `path`, typed for `types`.
fn draft_with_condition(path: &str, types: &[DocumentFieldType]) -> (BuilderDraft, u64) {
    let mut draft = BuilderDraft::default();
    let root = draft.filter.id;
    let id = draft.add_condition(root).expect("root group exists");
    assert!(draft.set_path(id, path, types));
    (draft, id)
}

// ---- values -------------------------------------------------------------

#[test]
fn operators_follow_the_field_type_and_unions_mixed_types() {
    assert_eq!(
        operator_choices(&[DocumentFieldType::String]),
        vec![Eq, Ne, In, Nin, Regex, Exists]
    );
    assert_eq!(
        operator_choices(&[DocumentFieldType::String, DocumentFieldType::Integer]),
        vec![Eq, Ne, Gt, Gte, Lt, Lte, In, Nin, Regex, Exists]
    );
    assert_eq!(
        operator_choices(&[DocumentFieldType::Array]),
        vec![ElemMatch, Size, All, Exists]
    );
    assert_eq!(operator_choices(&[]), DocumentOperator::ALL.to_vec());
}

#[test]
fn the_value_editor_follows_the_operator_and_the_kind() {
    assert_eq!(value_editor(Exists, ScalarKind::Text), ValueEditor::Toggle);
    assert_eq!(value_editor(Eq, ScalarKind::Bool), ValueEditor::Toggle);
    assert_eq!(
        value_editor(In, ScalarKind::Number),
        ValueEditor::Chips(ScalarKind::Number)
    );
    assert_eq!(value_editor(Regex, ScalarKind::Text), ValueEditor::Pattern);
    assert_eq!(value_editor(Size, ScalarKind::Auto), ValueEditor::Count);
    assert_eq!(
        value_editor(ElemMatch, ScalarKind::Auto),
        ValueEditor::Nested
    );
    assert_eq!(
        value_editor(Gte, ScalarKind::Date),
        ValueEditor::Scalar(ScalarKind::Date)
    );
}

#[test]
fn typed_text_reads_as_the_value_of_its_kind() {
    assert_eq!(
        parse_scalar(ScalarKind::Number, "100"),
        Ok(DocumentValue::Integer(100))
    );
    assert_eq!(
        parse_scalar(ScalarKind::Number, " 2500.5 "),
        Ok(DocumentValue::Decimal(2500.5))
    );
    assert_eq!(
        parse_scalar(ScalarKind::Number, "ten"),
        Err(ValueProblem::NotANumber)
    );

    let midnight = Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap();
    assert_eq!(
        parse_scalar(ScalarKind::Date, "2026-09-01 00:00 UTC"),
        Ok(DocumentValue::Date(midnight))
    );
    assert_eq!(
        parse_scalar(ScalarKind::Date, "2026-09-01"),
        Ok(DocumentValue::Date(midnight))
    );
    assert_eq!(
        parse_scalar(ScalarKind::Date, "2026-09-01T10:20:30Z"),
        Ok(DocumentValue::Date(
            Utc.with_ymd_and_hms(2026, 9, 1, 10, 20, 30).unwrap()
        ))
    );
    assert_eq!(
        parse_scalar(ScalarKind::Date, "yesterday"),
        Err(ValueProblem::NotADate)
    );

    assert_eq!(
        parse_scalar(ScalarKind::ObjectId, &format!("ObjectId(\"{OBJECT_ID}\")")),
        Ok(DocumentValue::ObjectId(OBJECT_ID.to_string()))
    );
    assert_eq!(
        parse_scalar(ScalarKind::ObjectId, &OBJECT_ID.to_ascii_uppercase()),
        Ok(DocumentValue::ObjectId(OBJECT_ID.to_string()))
    );
    assert_eq!(
        parse_scalar(ScalarKind::ObjectId, "66f0"),
        Err(ValueProblem::NotAnObjectId)
    );

    assert_eq!(
        parse_scalar(ScalarKind::Text, OBJECT_ID),
        Err(ValueProblem::LooksLikeObjectId)
    );
    assert_eq!(parse_scalar(ScalarKind::Text, "\"\""), Ok(text("")));
    assert_eq!(parse_scalar(ScalarKind::Text, ""), Err(ValueProblem::Empty));
    assert_eq!(parse_scalar(ScalarKind::Text, "failed"), Ok(text("failed")));

    assert_eq!(
        parse_scalar(ScalarKind::Auto, "true"),
        Ok(DocumentValue::Bool(true))
    );
    assert_eq!(
        parse_scalar(ScalarKind::Auto, "42"),
        Ok(DocumentValue::Integer(42))
    );
    assert_eq!(parse_scalar(ScalarKind::Auto, "\"42\""), Ok(text("42")));
    assert_eq!(parse_scalar(ScalarKind::Auto, "Ada"), Ok(text("Ada")));
    assert_eq!(
        parse_scalar(ScalarKind::Auto, "null"),
        Ok(DocumentValue::Null)
    );
}

#[test]
fn formatted_values_read_back_unchanged() {
    let cases = [
        (text("failed"), ScalarKind::Text),
        (text(""), ScalarKind::Text),
        (text("\"quoted\""), ScalarKind::Text),
        (text("42"), ScalarKind::Auto),
        (text("true"), ScalarKind::Auto),
        (text("Ada"), ScalarKind::Auto),
        (DocumentValue::Integer(-7), ScalarKind::Number),
        (DocumentValue::Decimal(2500.25), ScalarKind::Number),
        (DocumentValue::Bool(false), ScalarKind::Bool),
        (DocumentValue::Null, ScalarKind::Auto),
        (
            DocumentValue::ObjectId(OBJECT_ID.to_string()),
            ScalarKind::ObjectId,
        ),
        (
            DocumentValue::ObjectId(OBJECT_ID.to_string()),
            ScalarKind::Auto,
        ),
        (
            DocumentValue::Date(Utc.with_ymd_and_hms(2026, 9, 1, 8, 30, 0).unwrap()),
            ScalarKind::Date,
        ),
        (
            DocumentValue::Date(Utc.with_ymd_and_hms(2026, 9, 1, 8, 30, 15).unwrap()),
            ScalarKind::Date,
        ),
    ];

    for (value, kind) in cases {
        assert_eq!(
            parse_scalar(kind, &format_value(&value, kind)),
            Ok(value.clone()),
            "{value:?} as {kind:?}"
        );
    }

    let pattern = DocumentValue::Regex {
        pattern: "^KB-".to_string(),
        options: "i".to_string(),
    };
    assert_eq!(parse_pattern("/^KB-/i"), Ok(pattern.clone()));
    assert_eq!(format_value(&pattern, ScalarKind::Text), "/^KB-/i");
    assert_eq!(
        parse_pattern("abc"),
        Ok(DocumentValue::Regex {
            pattern: "abc".to_string(),
            options: String::new(),
        })
    );
}

#[test]
fn preview_highlights_every_operator_token() {
    let preview = r#"db.orders.find({"total": {"$gt": 100}, "$or": [{"a": 1}]})"#;
    let tokens: Vec<&str> = operator_ranges(preview)
        .into_iter()
        .map(|range| &preview[range])
        .collect();

    assert_eq!(tokens, vec!["$gt", "$or"]);
}

// ---- catalog ------------------------------------------------------------

#[test]
fn the_catalog_lists_nested_paths_under_their_parent_in_sample_order() {
    let catalog = catalog();
    let paths: Vec<(&str, usize)> = catalog
        .fields()
        .iter()
        .map(|field| (field.path.as_str(), field.depth))
        .collect();

    assert_eq!(
        paths,
        vec![
            ("_id", 0),
            ("customer", 0),
            ("customer.email", 1),
            ("customer.tier", 1),
            ("total", 0),
            ("items", 0),
            ("items.sku", 1),
            ("code", 0),
        ]
    );

    let tier = catalog.field("customer.tier").unwrap();
    assert_eq!(tier.name, "tier");
    assert_eq!(tier.presence_percent, 98);
    assert_eq!(tier.types, vec![DocumentFieldType::String]);
}

#[test]
fn mixed_type_fields_keep_every_type_but_null() {
    let catalog = catalog();
    let code = catalog.field("code").unwrap();

    assert_eq!(
        code.types,
        vec![DocumentFieldType::String, DocumentFieldType::Integer]
    );
    assert_eq!(catalog.field("total").unwrap().types.len(), 1);
}

#[test]
fn search_matches_paths_case_insensitively_and_unsampled_paths_have_no_type() {
    let catalog = catalog();

    let found: Vec<&str> = catalog
        .search("TIER")
        .into_iter()
        .map(|field| field.path.as_str())
        .collect();
    assert_eq!(found, vec!["customer.tier"]);
    assert_eq!(catalog.search("").len(), catalog.fields().len());

    assert!(catalog.is_sampled("items.sku"));
    assert!(!catalog.is_sampled("failure.retry"));
    assert!(catalog.types("failure.retry").is_empty());
}

#[test]
fn a_scoped_catalog_lists_the_fields_of_an_array_element() {
    let items = catalog().scoped("items");
    let fields: Vec<(&str, usize)> = items
        .fields()
        .iter()
        .map(|field| (field.path.as_str(), field.depth))
        .collect();

    assert_eq!(fields, vec![("sku", 0)]);
    assert_eq!(items.types("sku"), vec![DocumentFieldType::String]);
}

// ---- draft --------------------------------------------------------------

#[test]
fn a_condition_takes_the_first_operator_of_its_field_and_a_typed_value() {
    let (mut draft, id) = draft_with_condition("status", &[DocumentFieldType::String]);

    let condition = draft.condition(id).unwrap();
    assert_eq!(condition.operator, Eq);
    assert_eq!(condition.kind, ScalarKind::Text);

    assert!(draft.set_text(id, "failed"));
    let spec = draft.to_spec().unwrap();
    assert_eq!(
        spec.filter,
        group(
            DocumentCombinator::And,
            vec![condition_node("status", Eq, text("failed"))]
        )
    );
}

fn condition_node(
    path: &str,
    operator: DocumentOperator,
    value: DocumentValue,
) -> DocumentFilterNode {
    condition(path, operator, value)
}

#[test]
fn changing_the_field_resets_an_operator_it_does_not_offer() {
    let (mut draft, id) = draft_with_condition("sku", &[DocumentFieldType::String]);
    assert!(draft.set_operator(id, Regex));

    assert!(draft.set_path(id, "total", &[DocumentFieldType::Decimal]));

    let condition = draft.condition(id).unwrap();
    assert_eq!(condition.operator, Eq);
    assert_eq!(condition.kind, ScalarKind::Number);
}

#[test]
fn switching_between_single_and_list_operators_keeps_the_value() {
    let (mut draft, id) = draft_with_condition("tier", &[DocumentFieldType::String]);
    draft.set_text(id, "team");

    assert!(draft.set_operator(id, In));
    assert_eq!(
        draft.condition(id).unwrap().operand,
        Operand::Chips(vec![text("team")])
    );

    assert_eq!(draft.add_chip(id, "enterprise"), Ok(()));
    assert_eq!(
        draft.to_spec().unwrap().filter.children,
        vec![condition(
            "tier",
            In,
            DocumentValue::List(vec![text("team"), text("enterprise")])
        )]
    );

    assert!(draft.set_operator(id, Eq));
    let condition = draft.condition(id).unwrap();
    assert_eq!(
        condition.operand,
        Operand::Text {
            text: "team".to_string(),
            value: Ok(text("team")),
        }
    );
}

#[test]
fn chips_are_typed_and_an_empty_list_cannot_run() {
    let (mut draft, id) = draft_with_condition("total", &[DocumentFieldType::Decimal]);
    draft.set_operator(id, In);

    assert_eq!(draft.add_chip(id, "ten"), Err(ValueProblem::NotANumber));
    assert_eq!(
        draft.to_spec().unwrap_err(),
        vec![DraftProblem {
            node: id,
            kind: ProblemKind::EmptyList,
        }]
    );

    assert_eq!(draft.add_chip(id, "10"), Ok(()));
    assert!(draft.remove_chip(id, 0));
    assert!(draft.to_spec().is_err());
}

#[test]
fn a_new_nested_group_needs_a_field_before_it_runs() {
    let mut draft = BuilderDraft::default();
    let root = draft.filter.id;
    let nested = draft.add_group(root).unwrap();

    let NodeDraft::Group(group_draft) = &draft.filter.children[0] else {
        panic!("expected a group");
    };
    assert_eq!(group_draft.id, nested);
    assert_eq!(group_draft.combinator, DocumentCombinator::Or);
    let NodeDraft::Condition(first) = &group_draft.children[0] else {
        panic!("expected a condition");
    };
    let first = first.id;

    let problems = draft.to_spec().unwrap_err();
    assert!(problems.contains(&DraftProblem {
        node: first,
        kind: ProblemKind::MissingField,
    }));

    draft.set_path(first, "failure.retry", &[DocumentFieldType::Bool]);
    assert_eq!(
        draft.condition(first).unwrap().operand,
        Operand::Toggle(true)
    );
    assert_eq!(
        draft.to_spec().unwrap().filter.children,
        vec![DocumentFilterNode::Group(group(
            DocumentCombinator::Or,
            vec![condition("failure.retry", Eq, DocumentValue::Bool(true))]
        ))]
    );

    assert!(draft.remove_node(nested));
    assert!(draft.filter.children.is_empty());
    assert!(draft.to_spec().unwrap().filter.is_empty());
}

#[test]
fn elem_match_holds_sub_conditions_on_the_array_element() {
    let (mut draft, id) = draft_with_condition("items", &[DocumentFieldType::Array]);
    assert_eq!(draft.condition(id).unwrap().operator, ElemMatch);

    let Operand::Nested(body) = &draft.condition(id).unwrap().operand else {
        panic!("expected a nested group");
    };
    let NodeDraft::Condition(sub) = &body.children[0] else {
        panic!("expected a sub-condition");
    };
    let sub = sub.id;

    assert_eq!(draft.condition_scope(sub).as_deref(), Some("items"));
    assert_eq!(draft.condition_scope(id).as_deref(), Some(""));
    assert_eq!(draft.condition_count(), 1);

    draft.set_path(sub, "sku", &[DocumentFieldType::String]);
    draft.set_operator(sub, Regex);
    draft.set_text(sub, "/^KB-/i");

    assert_eq!(
        draft.to_spec().unwrap().filter.children,
        vec![condition(
            "items",
            ElemMatch,
            DocumentValue::Nested(group(
                DocumentCombinator::And,
                vec![condition(
                    "sku",
                    Regex,
                    DocumentValue::Regex {
                        pattern: "^KB-".to_string(),
                        options: "i".to_string(),
                    }
                )]
            ))
        )]
    );
}

#[test]
fn a_hexadecimal_text_value_can_be_retyped_as_an_object_id() {
    let (mut draft, id) = draft_with_condition("ref", &[DocumentFieldType::String]);
    draft.set_text(id, OBJECT_ID);

    assert_eq!(
        draft.to_spec().unwrap_err(),
        vec![DraftProblem {
            node: id,
            kind: ProblemKind::Value(ValueProblem::LooksLikeObjectId),
        }]
    );

    assert!(draft.set_kind(id, ScalarKind::ObjectId));
    assert_eq!(
        draft.to_spec().unwrap().filter.children,
        vec![condition(
            "ref",
            Eq,
            DocumentValue::ObjectId(OBJECT_ID.to_string())
        )]
    );
}

fn full_spec() -> DocumentQuerySpec {
    DocumentQuerySpec {
        filter: group(
            DocumentCombinator::And,
            vec![
                condition("status", Eq, text("failed")),
                condition(
                    "customer.tier",
                    In,
                    DocumentValue::List(vec![text("team"), text("enterprise")]),
                ),
                condition("total", Gt, DocumentValue::Integer(100)),
                condition(
                    "created_at",
                    Gte,
                    DocumentValue::Date(Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap()),
                ),
                condition("_id", Ne, DocumentValue::ObjectId(OBJECT_ID.to_string())),
                condition("customer.company", Exists, DocumentValue::Bool(false)),
                DocumentFilterNode::Group(group(
                    DocumentCombinator::Or,
                    vec![
                        condition(
                            "items",
                            ElemMatch,
                            DocumentValue::Nested(group(
                                DocumentCombinator::And,
                                vec![condition("", Gte, DocumentValue::Integer(80))],
                            )),
                        ),
                        condition("failure.retry", Eq, DocumentValue::Bool(true)),
                        condition("items", Size, DocumentValue::Integer(3)),
                    ],
                )),
            ],
        ),
        projection: DocumentProjection {
            mode: DocumentProjectionMode::Include,
            fields: vec!["customer.email".to_string(), "total".to_string()],
        },
        sort: vec![
            DocumentSortKey::new("created_at", DocumentSortDirection::Descending),
            DocumentSortKey::new("total", DocumentSortDirection::Ascending),
        ],
        limit: Some(50),
        skip: Some(100),
        ..DocumentQuerySpec::default()
    }
}

#[test]
fn a_spec_loads_into_the_draft_and_comes_back_unchanged() {
    let spec = full_spec();
    let draft = BuilderDraft::from_spec(&spec);

    assert_eq!(draft.to_spec().unwrap(), spec);
    assert_eq!(draft.condition_count(), 9);

    let mut reloaded = BuilderDraft::default();
    let before = reloaded.filter.id;
    reloaded.load(&spec);
    assert_ne!(
        reloaded.filter.id, before,
        "a load gives every node a fresh id"
    );
    assert_eq!(reloaded.to_spec().unwrap(), spec);
}

#[test]
fn the_projection_never_excludes_id() {
    let mut draft = BuilderDraft::default();
    assert!(draft.add_projection_field("_id"));
    assert!(draft.add_projection_field("name"));
    assert!(!draft.add_projection_field("name"));

    draft.set_projection_mode(DocumentProjectionMode::Exclude);
    assert_eq!(draft.projection.fields, vec!["name".to_string()]);
    assert!(!draft.add_projection_field("_id"));

    assert!(draft.remove_projection_field(0));
    assert!(draft.projection.fields.is_empty());
}

#[test]
fn sort_keys_are_unique_and_can_be_reordered() {
    let mut draft = BuilderDraft::default();
    assert!(draft.add_sort_key("created_at"));
    assert!(draft.add_sort_key("total"));
    assert!(!draft.add_sort_key("total"));

    assert!(draft.set_sort_direction(0, DocumentSortDirection::Descending));
    assert!(draft.move_sort_key(1, 0));

    assert_eq!(
        draft.sort,
        vec![
            DocumentSortKey::new("total", DocumentSortDirection::Ascending),
            DocumentSortKey::new("created_at", DocumentSortDirection::Descending),
        ]
    );

    assert!(draft.remove_sort_key(1));
    assert!(!draft.remove_sort_key(5));
    assert_eq!(draft.sort.len(), 1);
}

// ---- sync ---------------------------------------------------------------

fn parse(
    spec: DocumentQuerySpec,
    unrepresentable: Vec<UnrepresentableClause>,
) -> DocumentSlotParse {
    DocumentSlotParse {
        spec,
        unrepresentable,
    }
}

fn filtered(children: Vec<DocumentFilterNode>) -> DocumentQuerySpec {
    DocumentQuerySpec {
        filter: group(DocumentCombinator::And, children),
        ..DocumentQuerySpec::default()
    }
}

fn rendered(filter: &str, projection: &str, sort: &str, limit: Option<u64>) -> DocumentFindSlots {
    slots(filter, projection, sort, limit)
}

#[test]
fn an_edit_writes_only_the_slots_whose_part_changed() {
    let mut sync = SlotSync::default();
    let read = slots(r#"{ a: 1 }"#, r#"{ name: 1 }"#, "", None);
    let mut spec = filtered(vec![condition("a", Eq, DocumentValue::Integer(1))]);
    spec.projection.fields = vec!["name".to_string()];
    sync.read(&read, &parse(spec.clone(), Vec::new()));
    assert!(sync.is_echo(&read));

    spec.filter = group(
        DocumentCombinator::And,
        vec![condition("a", Eq, DocumentValue::Integer(2))],
    );
    let write = sync.plan(
        &spec,
        &rendered(r#"{"a": 2}"#, r#"{"name": 1}"#, "{}", None),
    );

    assert_eq!(
        write,
        SlotWrite {
            filter: Some(r#"{"a": 2}"#.to_string()),
            ..SlotWrite::default()
        }
    );
    assert!(sync.is_echo(&slots(r#"{"a": 2}"#, r#"{ name: 1 }"#, "", None)));
    assert!(!sync.is_conflicted());
}

#[test]
fn an_emptied_part_clears_its_slot_and_the_limit_is_written_as_text() {
    let mut sync = SlotSync::default();
    let spec = filtered(vec![condition("a", Eq, DocumentValue::Integer(1))]);
    sync.read(
        &slots(r#"{"a": 1}"#, "", "", None),
        &parse(spec, Vec::new()),
    );

    let mut emptied = DocumentQuerySpec::default();
    emptied.limit = Some(50);
    let write = sync.plan(&emptied, &rendered("{}", "{}", "{}", Some(50)));

    assert_eq!(
        write,
        SlotWrite {
            filter: Some(String::new()),
            limit: Some("50".to_string()),
            ..SlotWrite::default()
        }
    );
}

#[test]
fn a_slot_the_builder_could_not_read_is_never_overwritten_by_an_edit() {
    let mut sync = SlotSync::default();
    let read = slots(r#"{"$expr": {"$gt": ["$a", "$b"]}, "c": 1}"#, "", "", None);
    let parsed = parse(
        filtered(vec![condition("c", Eq, DocumentValue::Integer(1))]),
        vec![clause(
            DocumentSlot::Filter,
            r#"{"$expr": {"$gt": ["$a", "$b"]}}"#,
        )],
    );
    sync.read(&read, &parsed);

    assert!(sync.is_conflicted());
    assert!(sync.locks(DocumentSlot::Filter));
    assert!(!sync.locks(DocumentSlot::Sort));

    let mut edited = filtered(vec![condition("c", Eq, DocumentValue::Integer(2))]);
    edited.sort = vec![DocumentSortKey::new("c", DocumentSortDirection::Descending)];
    let write = sync.plan(
        &edited,
        &rendered(r#"{"c": 2}"#, "{}", r#"{"c": -1}"#, None),
    );

    assert_eq!(
        write,
        SlotWrite {
            sort: Some(r#"{"c": -1}"#.to_string()),
            ..SlotWrite::default()
        }
    );
    assert_eq!(sync.held(), &[DocumentSlot::Filter]);

    let mut reverted = filtered(vec![condition("c", Eq, DocumentValue::Integer(1))]);
    reverted.sort = edited.sort.clone();
    let write = sync.plan(
        &reverted,
        &rendered(r#"{"c": 1}"#, "{}", r#"{"c": -1}"#, None),
    );
    assert!(write.is_empty());
    assert!(sync.held().is_empty());
    assert!(sync.is_conflicted());
}

#[test]
fn rewriting_from_the_builder_replaces_the_slot_and_ends_the_conflict() {
    let mut sync = SlotSync::default();
    let read = slots(r#"{"$where": "true"}"#, "", "", None);
    sync.read(
        &read,
        &parse(
            DocumentQuerySpec::default(),
            vec![clause(DocumentSlot::Filter, r#"{"$where": "true"}"#)],
        ),
    );

    let edited = filtered(vec![condition("a", Eq, DocumentValue::Integer(1))]);
    let held = sync.plan(&edited, &rendered(r#"{"a": 1}"#, "{}", "{}", None));
    assert!(held.is_empty());

    let write = sync.rewrite(&edited, &rendered(r#"{"a": 1}"#, "{}", "{}", None));
    assert_eq!(write.filter.as_deref(), Some(r#"{"a": 1}"#));
    assert!(!sync.is_conflicted());
    assert!(sync.held().is_empty());
    assert!(sync.is_echo(&slots(r#"{"a": 1}"#, "", "", None)));
}

#[test]
fn rewriting_writes_every_part_even_when_its_text_looks_unchanged() {
    let mut sync = SlotSync::default();
    let read = slots(r#"{"a": 1}"#, r#"{"name": 1}"#, r#"{"a": -1}"#, Some(20));
    let mut spec = filtered(vec![condition("a", Eq, DocumentValue::Integer(1))]);
    spec.projection.fields = vec!["name".to_string()];
    spec.sort = vec![DocumentSortKey::new("a", DocumentSortDirection::Descending)];
    spec.limit = Some(20);
    sync.read(&read, &parse(spec.clone(), Vec::new()));

    let write = sync.rewrite(&spec, &read);

    assert_eq!(
        write,
        SlotWrite {
            filter: Some(r#"{"a": 1}"#.to_string()),
            projection: Some(r#"{"name": 1}"#.to_string()),
            sort: Some(r#"{"a": -1}"#.to_string()),
            limit: Some("20".to_string()),
        },
        "the slot may no longer hold what was last read"
    );
}

// ---- aggregate mode -----------------------------------------------------

fn accumulator(name: &str, kind: DocumentAccumulatorKind) -> DocumentAccumulator {
    DocumentAccumulator {
        name: name.to_string(),
        kind,
    }
}

fn grouped_spec() -> DocumentQuerySpec {
    DocumentQuerySpec {
        mode: DocumentQueryMode::Aggregate,
        filter: group(
            DocumentCombinator::And,
            vec![condition("status", Eq, text("failed"))],
        ),
        sort: vec![DocumentSortKey::new(
            "revenue",
            DocumentSortDirection::Descending,
        )],
        limit: Some(20),
        group: Some(DocumentGroupStage {
            keys: vec!["customer.tier".to_string()],
            accumulators: vec![
                accumulator("orders", DocumentAccumulatorKind::Count),
                accumulator(
                    "revenue",
                    DocumentAccumulatorKind::Sum {
                        path: "total".to_string(),
                    },
                ),
                accumulator(
                    "avg_total",
                    DocumentAccumulatorKind::Avg {
                        path: "total".to_string(),
                    },
                ),
            ],
        }),
        ..DocumentQuerySpec::default()
    }
}

#[test]
fn a_grouped_spec_loads_into_the_draft_and_comes_back_unchanged() {
    let spec = grouped_spec();
    let draft = BuilderDraft::from_spec(&spec);

    assert_eq!(draft.mode, DocumentQueryMode::Aggregate);
    assert_eq!(draft.to_spec().unwrap(), spec);
}

#[test]
fn adding_a_group_stage_switches_to_aggregate_and_removing_it_returns_to_find() {
    let (mut draft, id) = draft_with_condition("status", &[DocumentFieldType::String]);
    draft.set_text(id, "failed");
    draft.add_projection_field("total");

    assert!(draft.set_mode(DocumentQueryMode::Aggregate));
    assert_eq!(draft.mode, DocumentQueryMode::Aggregate);

    let stage = draft.group.as_ref().expect("a group stage");
    assert!(stage.keys.is_empty());
    assert_eq!(stage.accumulators.len(), 1);
    assert_eq!(stage.accumulators[0].op, AccumulatorOp::Count);

    let spec = draft.to_spec().unwrap();
    assert_eq!(spec.mode, DocumentQueryMode::Aggregate);
    assert!(
        spec.projection.is_empty(),
        "an aggregation takes no projection"
    );
    assert_eq!(
        spec.group.unwrap().accumulators,
        vec![accumulator("count", DocumentAccumulatorKind::Count)]
    );

    assert!(draft.remove_group_stage());
    assert_eq!(draft.mode, DocumentQueryMode::Find);
    assert!(draft.group.is_none());

    let spec = draft.to_spec().unwrap();
    assert_eq!(spec.mode, DocumentQueryMode::Find);
    assert!(spec.group.is_none());
    assert_eq!(
        spec.projection.fields,
        vec!["total".to_string()],
        "the find projection survives the round trip"
    );
}

#[test]
fn switching_to_find_keeps_the_group_stage_for_the_way_back() {
    let mut draft = BuilderDraft::default();
    draft.set_mode(DocumentQueryMode::Aggregate);
    assert!(draft.add_group_key("customer.tier"));

    assert!(draft.set_mode(DocumentQueryMode::Find));
    assert!(draft.to_spec().unwrap().group.is_none());

    assert!(draft.set_mode(DocumentQueryMode::Aggregate));
    assert_eq!(
        draft.to_spec().unwrap().group.unwrap().keys,
        vec!["customer.tier".to_string()]
    );
    assert!(!draft.set_mode(DocumentQueryMode::Aggregate));
}

#[test]
fn accumulators_take_unique_names_and_sum_or_avg_need_a_field() {
    let mut draft = BuilderDraft::default();
    draft.set_mode(DocumentQueryMode::Aggregate);

    let second = draft.add_accumulator().unwrap();
    let names: Vec<String> = draft
        .group
        .as_ref()
        .unwrap()
        .accumulators
        .iter()
        .map(|accumulator| accumulator.name.clone())
        .collect();
    assert_eq!(names, vec!["count".to_string(), "count_2".to_string()]);

    assert!(draft.set_accumulator_op(second, AccumulatorOp::Sum));
    assert!(draft.set_accumulator_name(second, "revenue"));
    assert_eq!(
        draft.to_spec().unwrap_err(),
        vec![DraftProblem {
            node: second,
            kind: ProblemKind::MissingField,
        }]
    );

    assert!(draft.set_accumulator_path(second, "total"));
    assert_eq!(
        draft.to_spec().unwrap().group.unwrap().accumulators[1],
        accumulator(
            "revenue",
            DocumentAccumulatorKind::Sum {
                path: "total".to_string(),
            }
        )
    );

    assert!(draft.set_accumulator_op(second, AccumulatorOp::Count));
    assert!(draft.set_accumulator_op(second, AccumulatorOp::Avg));
    assert_eq!(
        draft.accumulator(second).unwrap().path,
        "total",
        "the field is kept while the operator changes"
    );

    assert!(draft.remove_accumulator(second));
    assert!(draft.accumulator(second).is_none());
}

#[test]
fn core_problems_of_the_group_stage_point_at_their_accumulator() {
    let mut draft = BuilderDraft::default();
    draft.set_mode(DocumentQueryMode::Aggregate);
    let first = draft.group.as_ref().unwrap().accumulators[0].id;
    let second = draft.add_accumulator().unwrap();
    let third = draft.add_accumulator().unwrap();

    draft.set_accumulator_name(second, "count");
    draft.set_accumulator_name(third, " ");

    let problems = draft.to_spec().unwrap_err();
    assert!(problems.contains(&DraftProblem {
        node: second,
        kind: ProblemKind::Spec(DocumentSpecProblem::AccumulatorNameDuplicate {
            name: "count".to_string(),
        }),
    }));
    assert!(problems.contains(&DraftProblem {
        node: third,
        kind: ProblemKind::Spec(DocumentSpecProblem::AccumulatorNameEmpty { index: 2 }),
    }));
    assert!(problems.iter().all(|problem| problem.node != first));
}

#[test]
fn after_a_group_stage_sort_offers_only_the_group_keys_and_accumulators() {
    let mut draft = BuilderDraft::from_spec(&grouped_spec());

    assert_eq!(
        draft.sort_choices(),
        vec![
            "customer.tier".to_string(),
            "orders".to_string(),
            "revenue".to_string(),
            "avg_total".to_string(),
        ]
    );

    assert!(draft.add_sort_key("created_at"));
    let problems = draft.to_spec().unwrap_err();
    assert_eq!(
        problems
            .iter()
            .map(|problem| problem.kind.clone())
            .collect::<Vec<_>>(),
        vec![ProblemKind::Spec(
            DocumentSpecProblem::AggregateSortPathUnknown {
                path: "created_at".to_string(),
            }
        )]
    );

    draft.set_mode(DocumentQueryMode::Find);
    assert!(
        draft.to_spec().is_ok(),
        "a find sorts on any field of the documents"
    );
}

#[test]
fn group_keys_are_unique_and_removable() {
    let mut draft = BuilderDraft::default();
    draft.set_mode(DocumentQueryMode::Aggregate);

    assert!(draft.add_group_key("customer.tier"));
    assert!(!draft.add_group_key("customer.tier"));
    assert!(!draft.add_group_key(" "));
    assert!(draft.add_group_key("status"));

    assert!(draft.remove_group_key(0));
    assert_eq!(
        draft.group.as_ref().unwrap().keys,
        vec!["status".to_string()]
    );
    assert!(!draft.remove_group_key(3));
}
