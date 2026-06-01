use crate::completion_support::{
    completion_replace_range, extract_identifier_prefix, normalize_identifier,
    push_completion_item, scan_identifier_start,
};
use dbflux_components::controls::{CompletionProvider, InputState, Rope};
use dbflux_core::ColumnInfo;
use gpui::{Context, Task, WeakEntity, Window};
use lsp_types::{
    CompletionContext, CompletionItem, CompletionItemKind, CompletionResponse, CompletionTextEdit,
    InsertTextFormat, Range as LspRange, TextEdit,
};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use uuid::Uuid;

use dbflux_ui_base::AppStateEntity;

/// A single alias-to-table binding in the visual query builder.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct AliasBinding {
    /// The alias as it appears in the query (e.g. `u`, `orders`).
    pub alias: String,
    /// Schema qualifier, if any.
    pub schema: Option<String>,
    /// The underlying table name.
    pub table: String,
    /// True for the source table; false for join targets.
    pub is_source: bool,
}

/// In-memory cache of column metadata for the current builder panel.
///
/// Populated at panel construction (source table) and incrementally via
/// background fetches (joined tables). Shared between the panel and all
/// completion providers via `Rc<RefCell<SchemaCache>>`.
#[derive(Default)]
#[allow(dead_code)]
pub(crate) struct SchemaCache {
    /// Columns for the source (primary) table, keyed by the table name.
    pub source_table: String,
    pub source_columns: Vec<ColumnInfo>,

    /// Lazily fetched columns for joined tables.
    /// Key: (schema, table) where both are normalized (lowercase, unquoted).
    pub joined_columns: HashMap<(Option<String>, String), Vec<ColumnInfo>>,

    /// Keys for which a background fetch is in flight (dedup guard).
    pub fetching: HashSet<(Option<String>, String)>,

    /// Keys that failed to fetch; the provider will not retry these.
    pub failed: HashSet<(Option<String>, String)>,
}

/// Which input site the provider is attached to.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) enum CompletionMode {
    /// Join `to_table` input: suggest table names.
    Tables {
        table_names: Vec<String>,
        default_schema: Option<String>,
    },

    /// The `alias.column` inputs in Columns, Sort, and Filter predicate sections.
    ///
    /// Before a dot: suggest aliases and unqualified source-table columns.
    /// After `<alias>.`: suggest only the columns of that alias's table.
    AliasOrColumn { aliases: Vec<AliasBinding> },

    /// Join ON-clause right-hand side.
    ///
    /// Same logic as `AliasOrColumn`. The distinct variant exists so future
    /// bias toward the most-recently-added join can be added without touching
    /// attach-site code.
    JoinConditionRight { aliases: Vec<AliasBinding> },

    /// DataView WHERE filter bar: multi-token free-form input.
    ///
    /// Uses the same alias/column logic but seeded with only the source table.
    FilterExpression { aliases: Vec<AliasBinding> },
}

/// Schema-aware completion provider for single-line builder inputs.
#[allow(dead_code)]
pub(crate) struct SchemaCompletionProvider {
    #[allow(dead_code)]
    app_state: WeakEntity<AppStateEntity>,
    #[allow(dead_code)]
    profile_id: Uuid,
    mode: CompletionMode,
    schema_cache: Rc<RefCell<SchemaCache>>,
}

#[allow(dead_code)]
impl SchemaCompletionProvider {
    pub(crate) fn new(
        app_state: WeakEntity<AppStateEntity>,
        profile_id: Uuid,
        mode: CompletionMode,
        schema_cache: Rc<RefCell<SchemaCache>>,
    ) -> Self {
        Self {
            app_state,
            profile_id,
            mode,
            schema_cache,
        }
    }
}

/// Extract the qualifier just before a dot that precedes `prefix_start`.
///
/// For input like `users.em` with `prefix_start` pointing at `em`, this
/// returns `Some("users")`. Returns `None` when no dot-qualifier is present.
#[allow(dead_code)]
pub(crate) fn compute_qualifier(source: &str, prefix_start: usize) -> Option<String> {
    if prefix_start == 0 {
        return None;
    }

    let has_dot = source.as_bytes().get(prefix_start - 1) == Some(&b'.');
    if !has_dot {
        return None;
    }

    let qualifier_end = prefix_start - 1;
    let qualifier_start = scan_identifier_start(source, qualifier_end);
    if qualifier_start == qualifier_end {
        return None;
    }

    Some(source[qualifier_start..qualifier_end].to_string())
}

/// Compute completion suggestions as a pure function with no GPUI dependencies.
///
/// `mode` determines which data sources to draw from. `cache` is borrowed
/// read-only at the call site. `prefix` and `qualifier` are derived from the
/// cursor position in the current input text.
#[allow(dead_code)]
pub(crate) fn compute_suggestions(
    mode: &CompletionMode,
    cache: &SchemaCache,
    prefix: &str,
    qualifier: Option<&str>,
    replace_range: LspRange,
) -> Vec<CompletionItem> {
    let prefix_upper = prefix.to_uppercase();

    match mode {
        CompletionMode::Tables {
            table_names,
            default_schema,
        } => {
            let mut seen = HashSet::new();
            let mut items = Vec::new();

            for name in table_names {
                let display_name = match default_schema {
                    Some(schema) if name.starts_with(&format!("{}.", schema)) => {
                        name[schema.len() + 1..].to_string()
                    }
                    _ => name.clone(),
                };

                if !prefix_upper.is_empty()
                    && !display_name.to_uppercase().starts_with(&prefix_upper)
                {
                    continue;
                }

                push_completion_item(
                    &mut items,
                    &mut seen,
                    &display_name,
                    CompletionItemKind::STRUCT,
                    prefix,
                    replace_range,
                );
            }

            items
        }

        CompletionMode::AliasOrColumn { aliases }
        | CompletionMode::JoinConditionRight { aliases }
        | CompletionMode::FilterExpression { aliases } => {
            let mut seen = HashSet::new();
            let mut items = Vec::new();

            if let Some(qualifier_str) = qualifier {
                let qualifier_norm = normalize_identifier(qualifier_str);

                let matched_binding = aliases
                    .iter()
                    .find(|b| normalize_identifier(&b.alias) == qualifier_norm);

                let Some(binding) = matched_binding else {
                    return items;
                };

                let columns: &[ColumnInfo] = if binding.is_source {
                    &cache.source_columns
                } else {
                    let key = (
                        binding.schema.as_ref().map(|s| normalize_identifier(s)),
                        normalize_identifier(&binding.table),
                    );

                    match cache.joined_columns.get(&key) {
                        Some(cols) => cols,
                        None => return items,
                    }
                };

                for col in columns {
                    if !prefix_upper.is_empty()
                        && !col.name.to_uppercase().starts_with(&prefix_upper)
                    {
                        continue;
                    }

                    push_completion_item(
                        &mut items,
                        &mut seen,
                        &col.name,
                        CompletionItemKind::FIELD,
                        prefix,
                        replace_range,
                    );
                }
            } else {
                for binding in aliases {
                    if !prefix_upper.is_empty()
                        && !binding.alias.to_uppercase().starts_with(&prefix_upper)
                    {
                        continue;
                    }

                    let detail = binding.table.clone();
                    let key = binding.alias.to_uppercase();
                    if seen.insert(key) {
                        items.push(CompletionItem {
                            label: binding.alias.clone(),
                            kind: Some(CompletionItemKind::REFERENCE),
                            detail: Some(detail),
                            insert_text_format: Some(InsertTextFormat::PLAIN_TEXT),
                            filter_text: Some(prefix.to_string()),
                            text_edit: Some(CompletionTextEdit::Edit(TextEdit {
                                range: replace_range,
                                new_text: binding.alias.clone(),
                            })),
                            ..CompletionItem::default()
                        });
                    }
                }

                for col in &cache.source_columns {
                    if !prefix_upper.is_empty()
                        && !col.name.to_uppercase().starts_with(&prefix_upper)
                    {
                        continue;
                    }

                    push_completion_item(
                        &mut items,
                        &mut seen,
                        &col.name,
                        CompletionItemKind::FIELD,
                        prefix,
                        replace_range,
                    );
                }
            }

            items
        }
    }
}

impl CompletionProvider for SchemaCompletionProvider {
    fn completions(
        &self,
        text: &Rope,
        offset: usize,
        _trigger: CompletionContext,
        _window: &mut Window,
        _cx: &mut Context<InputState>,
    ) -> Task<anyhow::Result<CompletionResponse>> {
        let source = text.to_string();
        let cursor = offset.min(source.len());
        let (prefix_start, prefix) = extract_identifier_prefix(&source, cursor);
        let replace_range = completion_replace_range(&source, prefix_start, cursor);
        let qualifier = compute_qualifier(&source, prefix_start);

        let cache = self.schema_cache.borrow();
        let items = compute_suggestions(
            &self.mode,
            &cache,
            &prefix,
            qualifier.as_deref(),
            replace_range,
        );

        Task::ready(Ok(CompletionResponse::Array(items)))
    }

    fn is_completion_trigger(
        &self,
        _offset: usize,
        new_text: &str,
        _cx: &mut Context<InputState>,
    ) -> bool {
        if new_text.len() != 1 {
            return false;
        }

        let ch = new_text.as_bytes()[0] as char;
        ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_column(name: &str, type_name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            type_name: type_name.to_string(),
            nullable: true,
            is_primary_key: false,
            default_value: None,
            enum_values: None,
        }
    }

    fn empty_replace_range() -> LspRange {
        use lsp_types::{Position, Range};
        Range {
            start: Position {
                line: 0,
                character: 0,
            },
            end: Position {
                line: 0,
                character: 0,
            },
        }
    }

    fn label_set(items: &[CompletionItem]) -> Vec<String> {
        let mut labels: Vec<String> = items.iter().map(|i| i.label.clone()).collect();
        labels.sort();
        labels
    }

    fn make_source_cache(columns: Vec<ColumnInfo>) -> SchemaCache {
        SchemaCache {
            source_table: "users".to_string(),
            source_columns: columns,
            joined_columns: HashMap::new(),
            fetching: HashSet::new(),
            failed: HashSet::new(),
        }
    }

    // --- Tables mode ---

    #[test]
    fn tables_mode_lists_tables_by_prefix() {
        let mode = CompletionMode::Tables {
            table_names: vec![
                "public.users".to_string(),
                "public.user_logs".to_string(),
                "public.orders".to_string(),
            ],
            default_schema: Some("public".to_string()),
        };
        let cache = SchemaCache::default();

        let items = compute_suggestions(&mode, &cache, "us", None, empty_replace_range());
        let labels = label_set(&items);

        assert_eq!(labels, vec!["user_logs", "users"]);
        assert!(
            items
                .iter()
                .all(|i| i.kind == Some(CompletionItemKind::STRUCT))
        );
    }

    #[test]
    fn tables_mode_qualifies_cross_schema_tables() {
        let mode = CompletionMode::Tables {
            table_names: vec!["public.users".to_string(), "analytics.events".to_string()],
            default_schema: Some("public".to_string()),
        };
        let cache = SchemaCache::default();

        let items = compute_suggestions(&mode, &cache, "a", None, empty_replace_range());
        let labels = label_set(&items);

        assert_eq!(labels, vec!["analytics.events"]);
    }

    // --- AliasOrColumn mode — no qualifier ---

    #[test]
    fn alias_or_column_no_qualifier_suggests_aliases_and_source_columns() {
        let mode = CompletionMode::AliasOrColumn {
            aliases: vec![
                AliasBinding {
                    alias: "u".to_string(),
                    schema: None,
                    table: "users".to_string(),
                    is_source: true,
                },
                AliasBinding {
                    alias: "o".to_string(),
                    schema: None,
                    table: "orders".to_string(),
                    is_source: false,
                },
            ],
        };
        let cache = make_source_cache(vec![
            make_column("name", "text"),
            make_column("user_id", "int"),
            make_column("updated_at", "timestamp"),
        ]);

        let items = compute_suggestions(&mode, &cache, "u", None, empty_replace_range());
        let labels = label_set(&items);

        assert!(
            labels.contains(&"u".to_string()),
            "alias u must be included"
        );
        assert!(
            labels.contains(&"user_id".to_string()),
            "user_id must be included"
        );
        assert!(
            labels.contains(&"updated_at".to_string()),
            "updated_at must be included"
        );
        assert!(
            !labels.contains(&"o".to_string()),
            "alias o must be filtered by prefix"
        );
        assert!(
            !labels.contains(&"name".to_string()),
            "name must be filtered by prefix"
        );
    }

    // --- AliasOrColumn mode — with source alias qualifier ---

    #[test]
    fn alias_or_column_with_source_alias_qualifier_suggests_source_columns() {
        let mode = CompletionMode::AliasOrColumn {
            aliases: vec![AliasBinding {
                alias: "u".to_string(),
                schema: None,
                table: "users".to_string(),
                is_source: true,
            }],
        };
        let cache = make_source_cache(vec![
            make_column("email", "varchar"),
            make_column("id", "int"),
            make_column("name", "text"),
        ]);

        let items = compute_suggestions(&mode, &cache, "em", Some("u"), empty_replace_range());
        let labels = label_set(&items);

        assert_eq!(labels, vec!["email"]);
        assert_eq!(items[0].kind, Some(CompletionItemKind::FIELD));
    }

    // --- AliasOrColumn mode — with joined alias qualifier ---

    #[test]
    fn alias_or_column_with_joined_alias_qualifier_suggests_joined_columns() {
        let mode = CompletionMode::AliasOrColumn {
            aliases: vec![AliasBinding {
                alias: "o".to_string(),
                schema: None,
                table: "orders".to_string(),
                is_source: false,
            }],
        };

        let mut cache = SchemaCache::default();
        cache.joined_columns.insert(
            (None, "orders".to_string()),
            vec![
                make_column("status", "text"),
                make_column("total_cents", "int"),
            ],
        );

        let items = compute_suggestions(&mode, &cache, "sta", Some("o"), empty_replace_range());
        let labels = label_set(&items);

        assert_eq!(labels, vec!["status"]);
    }

    // --- AliasOrColumn mode — unfetched join ---

    #[test]
    fn alias_or_column_with_unfetched_join_returns_empty() {
        let mode = CompletionMode::AliasOrColumn {
            aliases: vec![AliasBinding {
                alias: "o".to_string(),
                schema: None,
                table: "orders".to_string(),
                is_source: false,
            }],
        };
        let cache = SchemaCache::default();

        let items = compute_suggestions(&mode, &cache, "sta", Some("o"), empty_replace_range());
        assert!(items.is_empty(), "unfetched join must yield empty list");
    }

    // --- Case insensitive prefix ---

    #[test]
    fn prefix_filter_is_case_insensitive() {
        let mode = CompletionMode::AliasOrColumn {
            aliases: vec![AliasBinding {
                alias: "u".to_string(),
                schema: None,
                table: "users".to_string(),
                is_source: true,
            }],
        };
        let cache = make_source_cache(vec![make_column("email", "varchar")]);

        let items = compute_suggestions(&mode, &cache, "EM", Some("U"), empty_replace_range());
        let labels = label_set(&items);

        assert_eq!(labels, vec!["email"]);
    }

    // --- Starts-with, not contains ---

    #[test]
    fn prefix_filter_is_starts_with_not_contains() {
        let mode = CompletionMode::AliasOrColumn {
            aliases: vec![AliasBinding {
                alias: "u".to_string(),
                schema: None,
                table: "users".to_string(),
                is_source: true,
            }],
        };
        let cache = make_source_cache(vec![make_column("email", "varchar")]);

        let items = compute_suggestions(&mode, &cache, "mail", Some("u"), empty_replace_range());
        assert!(items.is_empty(), "substring match must not produce results");
    }

    // --- Deduplication ---

    #[test]
    fn dedup_by_uppercased_label() {
        let mode = CompletionMode::AliasOrColumn {
            aliases: vec![
                AliasBinding {
                    alias: "u".to_string(),
                    schema: None,
                    table: "users".to_string(),
                    is_source: true,
                },
                AliasBinding {
                    alias: "u".to_string(),
                    schema: None,
                    table: "USERS".to_string(),
                    is_source: true,
                },
            ],
        };
        let cache = SchemaCache::default();

        let items = compute_suggestions(&mode, &cache, "", None, empty_replace_range());
        let u_count = items.iter().filter(|i| i.label == "u").count();
        assert_eq!(u_count, 1, "duplicate alias u must appear only once");
    }

    // --- FilterExpression mode ---

    #[test]
    fn filter_expression_mode_uses_source_alias_only() {
        let mode = CompletionMode::FilterExpression {
            aliases: vec![AliasBinding {
                alias: "users".to_string(),
                schema: None,
                table: "users".to_string(),
                is_source: true,
            }],
        };
        let cache = make_source_cache(vec![make_column("email", "varchar")]);

        let items = compute_suggestions(&mode, &cache, "em", Some("users"), empty_replace_range());
        let labels = label_set(&items);

        assert_eq!(labels, vec!["email"]);
    }

    // --- JoinConditionRight mode ---

    #[test]
    fn join_condition_right_mode_matches_alias_or_column_behavior() {
        let mode = CompletionMode::JoinConditionRight {
            aliases: vec![
                AliasBinding {
                    alias: "u".to_string(),
                    schema: None,
                    table: "users".to_string(),
                    is_source: true,
                },
                AliasBinding {
                    alias: "o".to_string(),
                    schema: None,
                    table: "orders".to_string(),
                    is_source: false,
                },
            ],
        };

        let mut cache = SchemaCache::default();
        cache.joined_columns.insert(
            (None, "orders".to_string()),
            vec![make_column("status", "text")],
        );

        let items = compute_suggestions(&mode, &cache, "st", Some("o"), empty_replace_range());
        let labels = label_set(&items);

        assert_eq!(labels, vec!["status"]);
    }
}
