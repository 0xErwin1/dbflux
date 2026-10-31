use super::*;

fn action(classification: ExecutionClassification) -> RootSectionAction {
    RootSectionAction {
        id: "action".to_string(),
        label: LocalizedText::plain("Action"),
        description: None,
        classification,
    }
}

#[test]
fn destructive_and_admin_actions_require_confirmation() {
    let confirmed = [
        ExecutionClassification::Destructive,
        ExecutionClassification::Admin,
        ExecutionClassification::AdminDestructive,
    ];
    let unconfirmed = [
        ExecutionClassification::Metadata,
        ExecutionClassification::Read,
        ExecutionClassification::Write,
        ExecutionClassification::AdminSafe,
    ];

    for classification in confirmed {
        assert!(
            action(classification).requires_confirmation(),
            "{classification:?} must ask for confirmation"
        );
    }
    for classification in unconfirmed {
        assert!(
            !action(classification).requires_confirmation(),
            "{classification:?} must run without confirmation"
        );
    }
}

#[test]
fn section_definition_round_trips_through_json() {
    let section = RootSectionDef {
        id: "buckets".to_string(),
        label: LocalizedText::keyed("sidebar.tree.folder.buckets", "Buckets"),
        counted_label_key: Some("sidebar.tree.container.object_storage".to_string()),
        empty_label: LocalizedText::plain("No buckets"),
        presentation: Some(RootSectionPresentation::ObjectContainerTable),
        item_presentation: RootItemPresentation::ObjectBrowser,
        opens_on_profile_activation: true,
        item_actions: vec![action(ExecutionClassification::AdminDestructive)],
    };

    let json = serde_json::to_string(&section).expect("serialize section");
    let decoded: RootSectionDef = serde_json::from_str(&json).expect("deserialize section");

    assert_eq!(decoded, section);
}

#[test]
fn minimal_section_json_fills_optional_fields_with_defaults() {
    let json = r#"{
        "id": "topics",
        "label": {"fallback": "Topics"},
        "empty_label": {"fallback": "No topics"},
        "item_presentation": "object_browser"
    }"#;

    let decoded: RootSectionDef = serde_json::from_str(json).expect("deserialize section");

    assert_eq!(decoded.label, LocalizedText::plain("Topics"));
    assert_eq!(decoded.counted_label_key, None);
    assert_eq!(decoded.presentation, None);
    assert!(!decoded.opens_on_profile_activation);
    assert!(decoded.item_actions.is_empty());
}

#[test]
fn item_action_finds_declared_actions_only() {
    let section = RootSectionDef {
        id: "buckets".to_string(),
        label: LocalizedText::plain("Buckets"),
        counted_label_key: None,
        empty_label: LocalizedText::plain("No buckets"),
        presentation: None,
        item_presentation: RootItemPresentation::ObjectBrowser,
        opens_on_profile_activation: false,
        item_actions: vec![action(ExecutionClassification::Write)],
    };

    assert_eq!(
        section
            .item_action("action")
            .map(|found| found.classification),
        Some(ExecutionClassification::Write)
    );
    assert!(section.item_action("missing").is_none());
}

struct ListingOnly;

impl RootSectionSource for ListingOnly {
    fn sections(&self) -> Result<Vec<RootSectionDef>, DbError> {
        Ok(Vec::new())
    }

    fn list_items(&self, _: &str, _: Option<&str>) -> Result<RootSectionPage, DbError> {
        Ok(RootSectionPage::default())
    }
}

#[test]
fn execute_action_defaults_to_not_supported() {
    let result = ListingOnly.execute_action("buckets", "logs", "delete");

    assert!(
        matches!(&result, Err(DbError::NotSupported(message)) if message.contains("delete")),
        "unexpected result: {result:?}"
    );
}

fn section(id: &str) -> RootSectionDef {
    RootSectionDef {
        id: id.to_string(),
        label: LocalizedText::plain("Section"),
        counted_label_key: None,
        empty_label: LocalizedText::plain("Empty"),
        presentation: None,
        item_presentation: RootItemPresentation::ObjectBrowser,
        opens_on_profile_activation: false,
        item_actions: Vec::new(),
    }
}

#[test]
fn section_ids_with_the_node_id_separator_are_invalid() {
    assert!(section("buckets").has_valid_id());
    assert!(section("consumer-groups.v2").has_valid_id());
    assert!(!section("reports|daily").has_valid_id());
    assert!(!section("|").has_valid_id());
}

#[test]
fn object_store_presentations_require_an_object_store() {
    assert!(RootSectionPresentation::ObjectContainerTable.requires_object_store());
    assert!(RootItemPresentation::ObjectBrowser.requires_object_store());
    assert!(section("buckets").requires_object_store());
    assert!(
        RootSectionDef {
            presentation: Some(RootSectionPresentation::ObjectContainerTable),
            ..section("buckets")
        }
        .requires_object_store()
    );
}

#[test]
fn only_metadata_and_read_actions_are_read_only() {
    let read_only = [
        ExecutionClassification::Metadata,
        ExecutionClassification::Read,
    ];
    let mutating = [
        ExecutionClassification::Write,
        ExecutionClassification::Destructive,
        ExecutionClassification::AdminSafe,
        ExecutionClassification::Admin,
        ExecutionClassification::AdminDestructive,
    ];

    for classification in read_only {
        assert!(action(classification).is_read_only(), "{classification:?}");
    }
    for classification in mutating {
        assert!(!action(classification).is_read_only(), "{classification:?}");
    }
}
