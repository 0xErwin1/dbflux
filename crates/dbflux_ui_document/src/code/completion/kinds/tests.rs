use super::super::{SqlCompletionMetadata, sql_items_with_kind_words};
use super::KindWords;
use dbflux_core::{CollectionPresentation, TableInfo, ViewInfo};
use lsp_types::CompletionItem;
use std::cell::Cell;

fn words() -> KindWords {
    KindWords::resolve(|key| format!("<{key}>"))
}

fn word(key: &str) -> String {
    format!("<document.code.completion_kind.{key}>")
}

fn table(name: &str) -> TableInfo {
    TableInfo {
        name: name.to_string(),
        schema: None,
        columns: None,
        indexes: None,
        foreign_keys: None,
        constraints: None,
        sample_fields: None,
        presentation: CollectionPresentation::default(),
        child_items: None,
        storage_hints: None,
        pseudo_columns: Box::default(),
    }
}

fn metadata() -> SqlCompletionMetadata {
    let mut metadata = SqlCompletionMetadata::default();
    metadata.add_table(&table("access_log"));
    metadata.add_view(&ViewInfo {
        name: "active_customers".to_string(),
        schema: None,
    });
    metadata
}

fn items(source: &str) -> Vec<CompletionItem> {
    let engine = dbflux_core::SqlContextEngine::new().expect("grammar loads");
    let analysis = engine.analyze(source, source.len()).expect("analysis");
    sql_items_with_kind_words(&metadata(), source, source.len(), Some(&analysis), &words())
}

fn description_of(items: &[CompletionItem], label: &str) -> Option<String> {
    items
        .iter()
        .find(|item| item.label == label)?
        .label_details
        .as_ref()?
        .description
        .clone()
}

#[test]
fn a_cte_a_table_and_a_view_with_the_same_prefix_each_get_their_own_word() {
    let items = items("WITH active_users AS (SELECT 1) SELECT * FROM a");

    assert_eq!(description_of(&items, "active_users"), Some(word("cte")));
    assert_eq!(description_of(&items, "access_log"), Some(word("table")));
    assert_eq!(
        description_of(&items, "active_customers"),
        Some(word("view"))
    );
}

#[test]
fn a_cte_shadows_a_table_of_the_same_name() {
    let items = items("WITH access_log AS (SELECT 1) SELECT * FROM a");

    assert_eq!(description_of(&items, "access_log"), Some(word("cte")));
}

#[test]
fn keywords_columns_and_aliases_get_their_words() {
    let mut with_columns = SqlCompletionMetadata::default();
    let mut orders = table("orders");
    orders.columns = Some(vec![dbflux_core::ColumnInfo {
        name: "amount".to_string(),
        type_name: "integer".to_string(),
        nullable: true,
        is_primary_key: false,
        default_value: None,
        enum_values: None,
    }]);
    with_columns.add_table(&orders);

    let source = "SELECT a FROM orders o WHERE ";
    let engine = dbflux_core::SqlContextEngine::new().expect("grammar loads");
    let analysis = engine.analyze(source, source.len()).expect("analysis");
    let items = sql_items_with_kind_words(
        &with_columns,
        source,
        source.len(),
        Some(&analysis),
        &words(),
    );

    assert_eq!(description_of(&items, "amount"), Some(word("column")));
    assert_eq!(description_of(&items, "o"), Some(word("alias")));
    assert_eq!(description_of(&items, "AND"), Some(word("keyword")));
}

#[test]
fn a_schema_gets_its_word() {
    let mut schema_metadata = SqlCompletionMetadata::default();
    let mut orders = table("orders");
    orders.schema = Some("analytics".to_string());
    schema_metadata.add_table(&orders);
    schema_metadata.schema_first = Some(super::super::SchemaFirst {
        selected_schema: None,
    });

    let items = sql_items_with_kind_words(&schema_metadata, "SELECT * FROM an", 16, None, &words());

    assert_eq!(description_of(&items, "analytics"), Some(word("schema")));
}

#[test]
fn the_kind_words_are_resolved_once_per_request_whatever_the_item_count() {
    let lookups = Cell::new(0);
    let words = KindWords::resolve(|key| {
        lookups.set(lookups.get() + 1);
        key.to_string()
    });
    assert_eq!(lookups.get(), 8, "one lookup per kind word");

    let source = "SELECT * FROM a";
    let before = lookups.get();
    let _ = sql_items_with_kind_words(&metadata(), source, source.len(), None, &words);
    let _ = sql_items_with_kind_words(&metadata(), source, source.len(), None, &words);
    assert_eq!(
        lookups.get(),
        before,
        "describing items never translates again"
    );
}

fn qualified_metadata() -> SqlCompletionMetadata {
    let mut metadata = SqlCompletionMetadata::default();
    let mut public_table = table("access_log");
    public_table.schema = Some("public".to_string());
    metadata.add_table(&public_table);
    metadata.add_view(&ViewInfo {
        name: "access_log".to_string(),
        schema: Some("analytics".to_string()),
    });
    metadata
}

fn qualified_items(source: &str) -> Vec<CompletionItem> {
    let engine = dbflux_core::SqlContextEngine::new().expect("grammar loads");
    let analysis = engine.analyze(source, source.len()).expect("analysis");
    sql_items_with_kind_words(
        &qualified_metadata(),
        source,
        source.len(),
        Some(&analysis),
        &words(),
    )
}

#[test]
fn a_view_is_told_from_a_table_of_the_same_name_in_another_schema() {
    let items = qualified_items("SELECT * FROM analytics.a");
    assert_eq!(description_of(&items, "access_log"), Some(word("view")));

    let items = qualified_items("SELECT * FROM public.a");
    assert_eq!(description_of(&items, "access_log"), Some(word("table")));
}

#[test]
fn a_schema_qualified_relation_is_not_a_cte_of_the_same_name() {
    let items = qualified_items("WITH access_log AS (SELECT 1) SELECT * FROM analytics.a");
    assert_eq!(description_of(&items, "access_log"), Some(word("view")));
}

#[test]
fn an_unqualified_cte_still_shadows_a_catalog_relation() {
    let items = qualified_items("WITH access_log AS (SELECT 1) SELECT * FROM a");
    assert_eq!(description_of(&items, "access_log"), Some(word("cte")));
}

#[test]
fn a_snippet_gets_its_word() {
    let mut items = vec![CompletionItem {
        label: "sel".to_string(),
        kind: Some(lsp_types::CompletionItemKind::SNIPPET),
        ..CompletionItem::default()
    }];

    super::describe_items(&mut items, &words(), &[], |_| false);

    assert_eq!(description_of(&items, "sel"), Some(word("snippet")));
}
