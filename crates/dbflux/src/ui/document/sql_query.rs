use super::data_grid_panel::{DataGridEvent, DataGridPanel};
use super::handle::DocumentEvent;
use super::types::{DocumentId, DocumentState};
use crate::app::AppState;
use crate::keymap::{Command, ContextId};
use crate::ui::history_modal::{HistoryModal, HistoryQuerySelected};
use crate::ui::icons::AppIcon;
use crate::ui::toast::ToastExt;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{
    CancelToken, DangerousQueryKind, DbError, DiagnosticSeverity as CoreDiagnosticSeverity,
    EditorDiagnostic as CoreEditorDiagnostic, HistoryEntry, QueryRequest, QueryResult,
    ValidationResult, detect_dangerous_query,
};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::highlighter::{
    Diagnostic as InputDiagnostic, DiagnosticSeverity as InputDiagnosticSeverity,
};
use gpui_component::input::{
    CompletionProvider, Input, InputEvent, InputState, Position as InputPosition, Rope,
};
use gpui_component::resizable::{resizable_panel, v_resizable};
use lsp_types::{
    CompletionContext, CompletionItem, CompletionItemKind, CompletionResponse, InsertTextFormat,
};
use std::cmp::min;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

struct QueryCompletionProvider {
    query_language: dbflux_core::QueryLanguage,
    app_state: Entity<AppState>,
    connection_id: Option<Uuid>,
}

impl QueryCompletionProvider {
    fn new(
        query_language: dbflux_core::QueryLanguage,
        app_state: Entity<AppState>,
        connection_id: Option<Uuid>,
    ) -> Self {
        Self {
            query_language,
            app_state,
            connection_id,
        }
    }

    fn keyword_candidates(&self) -> &'static [&'static str] {
        match self.query_language {
            dbflux_core::QueryLanguage::Sql
            | dbflux_core::QueryLanguage::Cql
            | dbflux_core::QueryLanguage::InfluxQuery => &[
                "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON",
                "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET", "INSERT", "INTO", "VALUES",
                "UPDATE", "SET", "DELETE", "CREATE", "ALTER", "DROP", "TRUNCATE", "BEGIN",
                "COMMIT", "ROLLBACK", "COUNT", "SUM", "AVG", "MIN", "MAX", "DISTINCT", "AND", "OR",
                "NOT", "NULL", "IS", "LIKE", "IN", "BETWEEN", "EXISTS",
            ],
            dbflux_core::QueryLanguage::MongoQuery => &[
                "db",
                "find",
                "findOne",
                "aggregate",
                "insertOne",
                "insertMany",
                "updateOne",
                "updateMany",
                "replaceOne",
                "deleteOne",
                "deleteMany",
                "count",
                "countDocuments",
                "$match",
                "$project",
                "$group",
                "$sort",
                "$limit",
                "$skip",
                "$lookup",
                "$unwind",
                "$set",
                "$eq",
                "$ne",
                "$gt",
                "$gte",
                "$lt",
                "$lte",
                "$in",
                "$nin",
                "$and",
                "$or",
                "$not",
                "$exists",
                "$regex",
            ],
            dbflux_core::QueryLanguage::RedisCommands => &[
                "GET", "SET", "MGET", "MSET", "DEL", "EXISTS", "EXPIRE", "TTL", "INCR", "DECR",
                "HGET", "HSET", "HDEL", "HGETALL", "LPUSH", "RPUSH", "LPOP", "RPOP", "LRANGE",
                "SADD", "SREM", "SMEMBERS", "ZADD", "ZREM", "ZRANGE", "KEYS", "SCAN", "INFO",
                "PING",
            ],
            dbflux_core::QueryLanguage::Cypher => &[
                "MATCH", "WHERE", "RETURN", "CREATE", "MERGE", "SET", "DELETE", "DETACH", "LIMIT",
            ],
            dbflux_core::QueryLanguage::Custom(_) => &[],
        }
    }

    fn sql_completion_metadata(&self, cx: &App) -> SqlCompletionMetadata {
        let Some(connection_id) = self.connection_id else {
            return SqlCompletionMetadata::default();
        };

        let state = self.app_state.read(cx);
        let Some(connected) = state.connections().get(&connection_id) else {
            return SqlCompletionMetadata::default();
        };

        let mut metadata = SqlCompletionMetadata::default();

        if let Some(snapshot) = &connected.schema {
            if let Some(relational) = snapshot.as_relational() {
                for table in &relational.tables {
                    metadata.add_table(table);
                }

                for view in &relational.views {
                    metadata.add_view(view);
                }

                for schema in &relational.schemas {
                    for table in &schema.tables {
                        metadata.add_table(table);
                    }

                    for view in &schema.views {
                        metadata.add_view(view);
                    }
                }
            }
        }

        for schema in connected.database_schemas.values() {
            for table in &schema.tables {
                metadata.add_table(table);
            }

            for view in &schema.views {
                metadata.add_view(view);
            }
        }

        for table in connected.table_details.values() {
            metadata.add_table(table);
        }

        metadata
    }

    fn mongo_completion_metadata(&self, cx: &App) -> MongoCompletionMetadata {
        let Some(connection_id) = self.connection_id else {
            return MongoCompletionMetadata::default();
        };

        let state = self.app_state.read(cx);
        let Some(connected) = state.connections().get(&connection_id) else {
            return MongoCompletionMetadata::default();
        };

        let mut metadata = MongoCompletionMetadata::default();

        if let Some(snapshot) = &connected.schema
            && let Some(document) = snapshot.as_document()
        {
            for collection in &document.collections {
                metadata.add_collection(collection);
            }
        }

        for schema in connected.database_schemas.values() {
            for table in &schema.tables {
                metadata.add_collection_name(&table.name);

                if let Some(columns) = &table.columns {
                    for column in columns {
                        metadata.add_field_for_collection(&table.name, &column.name);
                    }
                }
            }
        }

        for table in connected.table_details.values() {
            metadata.add_collection_name(&table.name);

            if let Some(columns) = &table.columns {
                for column in columns {
                    metadata.add_field_for_collection(&table.name, &column.name);
                }
            }
        }

        metadata
    }

    fn redis_completion_metadata(&self, cx: &App) -> RedisCompletionMetadata {
        let Some(connection_id) = self.connection_id else {
            return RedisCompletionMetadata::default();
        };

        let state = self.app_state.read(cx);
        let Some(connected) = state.connections().get(&connection_id) else {
            return RedisCompletionMetadata::default();
        };

        let mut metadata = RedisCompletionMetadata::default();

        if let Some(snapshot) = &connected.schema
            && let Some(key_value) = snapshot.as_key_value()
        {
            for keyspace in &key_value.keyspaces {
                metadata.keyspaces.push(keyspace.db_index);
            }
        }

        metadata.keyspaces.sort_unstable();
        metadata.keyspaces.dedup();
        metadata
    }

    fn completion_items_for_sql(
        &self,
        source: &str,
        cursor: usize,
        cx: &App,
    ) -> Vec<CompletionItem> {
        let metadata = self.sql_completion_metadata(cx);
        let (prefix_start, prefix) = extract_identifier_prefix(source, cursor);
        let prefix_upper = prefix.to_uppercase();
        let before_cursor = &source[..cursor];

        let mut seen = HashSet::new();
        let mut items = Vec::new();

        let has_dot_before_prefix =
            prefix_start > 0 && source.as_bytes().get(prefix_start - 1) == Some(&b'.');

        if has_dot_before_prefix {
            let qualifier_end = prefix_start - 1;
            let qualifier_start = scan_identifier_start(source, qualifier_end);
            let qualifier = &source[qualifier_start..qualifier_end];

            let aliases = extract_sql_aliases(before_cursor);
            let resolved_qualifier = aliases
                .get(&normalize_identifier(qualifier))
                .cloned()
                .unwrap_or_else(|| normalize_identifier(qualifier));

            for column_name in metadata.columns_for_table(&resolved_qualifier) {
                if !prefix_upper.is_empty()
                    && !column_name.to_uppercase().starts_with(&prefix_upper)
                {
                    continue;
                }

                push_completion_item(
                    &mut items,
                    &mut seen,
                    column_name,
                    CompletionItemKind::FIELD,
                );
            }

            return items;
        }

        for keyword in self.keyword_candidates() {
            if !prefix_upper.is_empty() && !keyword.to_uppercase().starts_with(&prefix_upper) {
                continue;
            }

            push_completion_item(&mut items, &mut seen, keyword, CompletionItemKind::KEYWORD);
        }

        let in_table_context = is_sql_table_context(before_cursor);

        for table_name in metadata.table_names_iter() {
            if !prefix_upper.is_empty() && !table_name.to_uppercase().starts_with(&prefix_upper) {
                continue;
            }

            if !in_table_context && prefix_upper.is_empty() {
                continue;
            }

            push_completion_item(
                &mut items,
                &mut seen,
                table_name,
                CompletionItemKind::STRUCT,
            );
        }

        for view_name in metadata.view_names_iter() {
            if !prefix_upper.is_empty() && !view_name.to_uppercase().starts_with(&prefix_upper) {
                continue;
            }

            if !in_table_context && prefix_upper.is_empty() {
                continue;
            }

            push_completion_item(&mut items, &mut seen, view_name, CompletionItemKind::STRUCT);
        }

        if !in_table_context {
            for column_name in metadata.all_columns_iter() {
                if !prefix_upper.is_empty()
                    && !column_name.to_uppercase().starts_with(&prefix_upper)
                {
                    continue;
                }

                if prefix_upper.is_empty() {
                    continue;
                }

                push_completion_item(
                    &mut items,
                    &mut seen,
                    column_name,
                    CompletionItemKind::FIELD,
                );
            }
        }

        items
    }

    fn completion_items_for_mongo(
        &self,
        source: &str,
        cursor: usize,
        cx: &App,
    ) -> Vec<CompletionItem> {
        let metadata = self.mongo_completion_metadata(cx);
        let (prefix_start, prefix) = extract_identifier_prefix(source, cursor);
        let prefix_upper = prefix.to_uppercase();

        let mut seen = HashSet::new();
        let mut items = Vec::new();

        let context = mongo_completion_context(source, prefix_start);

        match context {
            MongoCompletionContext::Collection => {
                for collection in metadata.collection_names_iter() {
                    if !prefix_upper.is_empty()
                        && !collection.to_uppercase().starts_with(&prefix_upper)
                    {
                        continue;
                    }

                    push_completion_item(
                        &mut items,
                        &mut seen,
                        collection,
                        CompletionItemKind::CLASS,
                    );
                }
            }
            MongoCompletionContext::Method => {
                for method in MONGO_METHODS {
                    if !prefix_upper.is_empty() && !method.to_uppercase().starts_with(&prefix_upper)
                    {
                        continue;
                    }

                    push_completion_item(&mut items, &mut seen, method, CompletionItemKind::METHOD);
                }
            }
            MongoCompletionContext::Field { collection } => {
                let fields = metadata.fields_for_collection(&collection);

                for field in fields {
                    if !prefix_upper.is_empty() && !field.to_uppercase().starts_with(&prefix_upper)
                    {
                        continue;
                    }

                    push_completion_item(&mut items, &mut seen, field, CompletionItemKind::FIELD);
                }

                if items.is_empty() {
                    for field in metadata.all_fields_iter() {
                        if !prefix_upper.is_empty()
                            && !field.to_uppercase().starts_with(&prefix_upper)
                        {
                            continue;
                        }

                        push_completion_item(
                            &mut items,
                            &mut seen,
                            field,
                            CompletionItemKind::FIELD,
                        );
                    }
                }
            }
            MongoCompletionContext::Operator => {
                for operator in MONGO_OPERATORS {
                    if !prefix_upper.is_empty()
                        && !operator.to_uppercase().starts_with(&prefix_upper)
                    {
                        continue;
                    }

                    push_completion_item(
                        &mut items,
                        &mut seen,
                        operator,
                        CompletionItemKind::OPERATOR,
                    );
                }
            }
            MongoCompletionContext::General => {}
        }

        for keyword in self.keyword_candidates() {
            if !prefix_upper.is_empty() && !keyword.to_uppercase().starts_with(&prefix_upper) {
                continue;
            }

            push_completion_item(&mut items, &mut seen, keyword, CompletionItemKind::KEYWORD);
        }

        items
    }

    fn completion_items_for_redis(
        &self,
        source: &str,
        cursor: usize,
        cx: &App,
    ) -> Vec<CompletionItem> {
        let metadata = self.redis_completion_metadata(cx);
        let before_cursor = &source[..cursor];
        let tokens = tokenize_redis_command(before_cursor);
        let ends_with_space = before_cursor
            .chars()
            .last()
            .is_some_and(|ch| ch.is_whitespace());

        let mut seen = HashSet::new();
        let mut items = Vec::new();

        let command_mode = tokens.is_empty() || (tokens.len() == 1 && !ends_with_space);
        if command_mode {
            let prefix = tokens.first().cloned().unwrap_or_default().to_uppercase();

            for command in REDIS_COMMANDS {
                if !prefix.is_empty() && !command.starts_with(&prefix) {
                    continue;
                }

                push_completion_item(&mut items, &mut seen, command, CompletionItemKind::FUNCTION);
            }

            return items;
        }

        let command = tokens[0].to_uppercase();
        let argument_index = if ends_with_space {
            tokens.len().saturating_sub(1)
        } else {
            tokens.len().saturating_sub(2)
        };

        if command == "SELECT" && argument_index == 0 {
            for keyspace in &metadata.keyspaces {
                let label = keyspace.to_string();
                push_completion_item(&mut items, &mut seen, &label, CompletionItemKind::VALUE);
            }
        }

        if let Some(options) = redis_argument_options(&command, argument_index) {
            let prefix = if ends_with_space {
                String::new()
            } else {
                tokens.last().cloned().unwrap_or_default().to_uppercase()
            };

            for option in options {
                if !prefix.is_empty() && !option.to_uppercase().starts_with(&prefix) {
                    continue;
                }

                push_completion_item(&mut items, &mut seen, option, CompletionItemKind::KEYWORD);
            }
        }

        items
    }
}

impl CompletionProvider for QueryCompletionProvider {
    fn completions(
        &self,
        text: &Rope,
        offset: usize,
        _trigger: CompletionContext,
        _window: &mut Window,
        _cx: &mut Context<InputState>,
    ) -> Task<anyhow::Result<CompletionResponse>> {
        let source = text.to_string();
        let cursor = min(offset, source.len());
        let items = match self.query_language {
            dbflux_core::QueryLanguage::Sql
            | dbflux_core::QueryLanguage::Cql
            | dbflux_core::QueryLanguage::InfluxQuery => {
                self.completion_items_for_sql(&source, cursor, _cx)
            }
            dbflux_core::QueryLanguage::MongoQuery => {
                self.completion_items_for_mongo(&source, cursor, _cx)
            }
            dbflux_core::QueryLanguage::RedisCommands => {
                self.completion_items_for_redis(&source, cursor, _cx)
            }
            _ => {
                let (_, prefix) = extract_identifier_prefix(&source, cursor);
                let prefix_upper = prefix.to_uppercase();
                let mut items = Vec::new();
                let mut seen = HashSet::new();

                for candidate in self.keyword_candidates() {
                    if !prefix_upper.is_empty()
                        && !candidate.to_uppercase().starts_with(&prefix_upper)
                    {
                        continue;
                    }

                    push_completion_item(
                        &mut items,
                        &mut seen,
                        candidate,
                        CompletionItemKind::KEYWORD,
                    );
                }

                items
            }
        };

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
        ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' || ch == '$'
    }
}

#[derive(Default)]
struct SqlCompletionMetadata {
    table_names: BTreeSet<String>,
    view_names: BTreeSet<String>,
    all_columns: BTreeSet<String>,
    columns_by_table: HashMap<String, BTreeSet<String>>,
}

impl SqlCompletionMetadata {
    fn add_table(&mut self, table: &dbflux_core::TableInfo) {
        self.table_names.insert(table.name.clone());

        if let Some(schema) = &table.schema {
            self.table_names
                .insert(format!("{}.{}", schema, table.name));
        }

        let Some(columns) = &table.columns else {
            return;
        };

        let mut keys = vec![normalize_identifier(&table.name)];
        if let Some(schema) = &table.schema {
            keys.push(normalize_identifier(&format!("{}.{}", schema, table.name)));
        }

        for column in columns {
            self.all_columns.insert(column.name.clone());

            for key in &keys {
                self.columns_by_table
                    .entry(key.clone())
                    .or_default()
                    .insert(column.name.clone());
            }
        }
    }

    fn add_view(&mut self, view: &dbflux_core::ViewInfo) {
        self.view_names.insert(view.name.clone());

        if let Some(schema) = &view.schema {
            self.view_names.insert(format!("{}.{}", schema, view.name));
        }
    }

    fn columns_for_table(&self, table_name: &str) -> Vec<&str> {
        self.columns_by_table
            .get(table_name)
            .map(|columns| columns.iter().map(|c| c.as_str()).collect())
            .unwrap_or_default()
    }

    fn table_names_iter(&self) -> impl Iterator<Item = &str> {
        self.table_names.iter().map(|name| name.as_str())
    }

    fn view_names_iter(&self) -> impl Iterator<Item = &str> {
        self.view_names.iter().map(|name| name.as_str())
    }

    fn all_columns_iter(&self) -> impl Iterator<Item = &str> {
        self.all_columns.iter().map(|name| name.as_str())
    }
}

#[derive(Default)]
struct MongoCompletionMetadata {
    collection_names: BTreeSet<String>,
    all_fields: BTreeSet<String>,
    fields_by_collection: HashMap<String, BTreeSet<String>>,
}

impl MongoCompletionMetadata {
    fn add_collection(&mut self, collection: &dbflux_core::CollectionInfo) {
        self.add_collection_name(&collection.name);

        if let Some(fields) = &collection.sample_fields {
            for field in fields {
                self.add_field_for_collection(&collection.name, &field.name);
                self.add_nested_fields_for_collection(&collection.name, field);
            }
        }
    }

    fn add_nested_fields_for_collection(
        &mut self,
        collection_name: &str,
        field: &dbflux_core::FieldInfo,
    ) {
        let Some(nested_fields) = &field.nested_fields else {
            return;
        };

        for nested in nested_fields {
            self.add_field_for_collection(collection_name, &nested.name);
            self.add_nested_fields_for_collection(collection_name, nested);
        }
    }

    fn add_collection_name(&mut self, collection_name: &str) {
        self.collection_names.insert(collection_name.to_string());
    }

    fn add_field_for_collection(&mut self, collection_name: &str, field_name: &str) {
        self.all_fields.insert(field_name.to_string());

        self.fields_by_collection
            .entry(normalize_identifier(collection_name))
            .or_default()
            .insert(field_name.to_string());
    }

    fn collection_names_iter(&self) -> impl Iterator<Item = &str> {
        self.collection_names.iter().map(|name| name.as_str())
    }

    fn all_fields_iter(&self) -> impl Iterator<Item = &str> {
        self.all_fields.iter().map(|name| name.as_str())
    }

    fn fields_for_collection(&self, collection_name: &str) -> Vec<&str> {
        self.fields_by_collection
            .get(&normalize_identifier(collection_name))
            .map(|fields| fields.iter().map(|f| f.as_str()).collect())
            .unwrap_or_default()
    }
}

#[derive(Default)]
struct RedisCompletionMetadata {
    keyspaces: Vec<u32>,
}

enum MongoCompletionContext {
    Collection,
    Method,
    Field { collection: String },
    Operator,
    General,
}

const MONGO_METHODS: &[&str] = &[
    "find",
    "findOne",
    "aggregate",
    "count",
    "countDocuments",
    "insertOne",
    "insertMany",
    "updateOne",
    "updateMany",
    "replaceOne",
    "deleteOne",
    "deleteMany",
    "drop",
];

const MONGO_OPERATORS: &[&str] = &[
    "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin", "$and", "$or", "$not", "$exists",
    "$regex", "$match", "$project", "$group", "$sort", "$limit", "$skip", "$lookup", "$unwind",
    "$set",
];

const REDIS_COMMANDS: &[&str] = &[
    "GET", "SET", "MGET", "MSET", "DEL", "EXISTS", "EXPIRE", "TTL", "TYPE", "INCR", "DECR", "HGET",
    "HSET", "HDEL", "HGETALL", "LPUSH", "RPUSH", "LPOP", "RPOP", "LRANGE", "SADD", "SREM",
    "SMEMBERS", "ZADD", "ZREM", "ZRANGE", "KEYS", "SCAN", "INFO", "PING", "SELECT",
];

fn mongo_completion_context(source: &str, prefix_start: usize) -> MongoCompletionContext {
    let before_prefix = &source[..prefix_start];

    if before_prefix.ends_with("db.") {
        return MongoCompletionContext::Collection;
    }

    if let Some((collection, method_context)) = extract_mongo_collection_context(before_prefix) {
        if method_context {
            return MongoCompletionContext::Method;
        }

        return MongoCompletionContext::Field { collection };
    }

    if is_mongo_operator_context(before_prefix) {
        return MongoCompletionContext::Operator;
    }

    MongoCompletionContext::General
}

fn extract_mongo_collection_context(before_prefix: &str) -> Option<(String, bool)> {
    let mut chars = before_prefix.chars().rev().peekable();

    while let Some(ch) = chars.peek() {
        if ch.is_whitespace() {
            chars.next();
        } else {
            break;
        }
    }

    if chars.peek().is_some_and(|ch| *ch == '.') {
        chars.next();

        let mut collection_rev = String::new();
        while let Some(ch) = chars.peek() {
            if ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '$' {
                collection_rev.push(*ch);
                chars.next();
                continue;
            }

            break;
        }

        if collection_rev.is_empty() {
            return None;
        }

        if chars.next() != Some('.') || chars.next() != Some('b') || chars.next() != Some('d') {
            return None;
        }

        let collection = collection_rev.chars().rev().collect::<String>();
        return Some((collection, true));
    }

    let mut recent = before_prefix.trim_end();
    if let Some(dot_idx) = recent.rfind('.') {
        recent = &recent[..dot_idx];
    }

    if let Some(db_dot) = recent.rfind("db.") {
        let tail = &recent[db_dot + 3..];
        let collection = tail
            .chars()
            .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '$')
            .collect::<String>();

        if !collection.is_empty() {
            return Some((collection, false));
        }
    }

    None
}

fn is_mongo_operator_context(before_prefix: &str) -> bool {
    let trimmed = before_prefix.trim_end();
    trimmed.ends_with('{')
        || trimmed.ends_with(',')
        || trimmed.ends_with(':')
        || trimmed.ends_with("[")
}

fn tokenize_redis_command(before_cursor: &str) -> Vec<String> {
    before_cursor
        .split_whitespace()
        .map(|part| part.trim_matches(';').to_string())
        .filter(|part| !part.is_empty())
        .collect()
}

fn redis_argument_options(command: &str, argument_index: usize) -> Option<&'static [&'static str]> {
    match command {
        "SET" => {
            if argument_index >= 2 {
                Some(&["NX", "XX", "EX", "PX", "EXAT", "PXAT", "KEEPTTL", "GET"])
            } else {
                None
            }
        }
        "EXPIRE" => {
            if argument_index >= 2 {
                Some(&["NX", "XX", "GT", "LT"])
            } else {
                None
            }
        }
        "ZADD" => {
            if argument_index >= 1 {
                Some(&["NX", "XX", "GT", "LT", "CH", "INCR"])
            } else {
                None
            }
        }
        _ => None,
    }
}

fn push_completion_item(
    items: &mut Vec<CompletionItem>,
    seen: &mut HashSet<String>,
    label: &str,
    kind: CompletionItemKind,
) {
    let key = label.to_uppercase();
    if !seen.insert(key) {
        return;
    }

    items.push(CompletionItem {
        label: label.to_string(),
        kind: Some(kind),
        insert_text_format: Some(InsertTextFormat::PLAIN_TEXT),
        ..CompletionItem::default()
    });
}

fn normalize_identifier(value: &str) -> String {
    value.trim_matches('"').to_lowercase()
}

fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$'
}

fn scan_identifier_start(source: &str, end: usize) -> usize {
    let bytes = source.as_bytes();
    let mut start = end;

    while start > 0 {
        let idx = start - 1;
        if !is_identifier_byte(bytes[idx]) {
            break;
        }

        start -= 1;
    }

    start
}

fn extract_identifier_prefix(source: &str, cursor: usize) -> (usize, String) {
    let cursor = min(cursor, source.len());
    let prefix_start = scan_identifier_start(source, cursor);
    (prefix_start, source[prefix_start..cursor].to_string())
}

fn tokenize_sql_identifiers(sql: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for ch in sql.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '$' || ch == '.' {
            current.push(ch);
            continue;
        }

        if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

fn extract_sql_aliases(sql_before_cursor: &str) -> HashMap<String, String> {
    let tokens = tokenize_sql_identifiers(sql_before_cursor);
    let mut aliases = HashMap::new();
    let keywords = ["FROM", "JOIN", "UPDATE", "INTO"];

    let mut idx = 0;
    while idx < tokens.len() {
        let token_upper = tokens[idx].to_uppercase();
        if !keywords.contains(&token_upper.as_str()) {
            idx += 1;
            continue;
        }

        let Some(table_token) = tokens.get(idx + 1) else {
            break;
        };

        let table_name = normalize_identifier(table_token);

        if let Some(next_token) = tokens.get(idx + 2) {
            let next_upper = next_token.to_uppercase();

            if next_upper == "AS" {
                if let Some(alias_token) = tokens.get(idx + 3) {
                    aliases.insert(normalize_identifier(alias_token), table_name.clone());
                }
            } else if ![
                "ON", "WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "JOIN", "INNER", "LEFT",
                "RIGHT", "FULL",
            ]
            .contains(&next_upper.as_str())
            {
                aliases.insert(normalize_identifier(next_token), table_name.clone());
            }
        }

        idx += 1;
    }

    aliases
}

fn is_sql_table_context(sql_before_cursor: &str) -> bool {
    let tokens = tokenize_sql_identifiers(sql_before_cursor);
    let Some(last) = tokens.last() else {
        return false;
    };

    matches!(
        last.to_uppercase().as_str(),
        "FROM" | "JOIN" | "UPDATE" | "INTO" | "TABLE"
    )
}

/// A single result tab within the SqlQueryDocument.
struct ResultTab {
    id: Uuid,
    title: String,
    grid: Entity<DataGridPanel>,
    _subscription: Subscription,
}

/// Internal layout of the document.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum SqlQueryLayout {
    #[default]
    Split,
    EditorOnly,
    ResultsOnly,
}

/// Where focus is within the document.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SqlQueryFocus {
    #[default]
    Editor,
    Results,
}

pub struct SqlQueryDocument {
    // Identity
    id: DocumentId,
    title: String,
    state: DocumentState,
    connection_id: Option<Uuid>,

    // Dependencies
    app_state: Entity<AppState>,

    // Editor
    input_state: Entity<InputState>,
    _input_subscriptions: Vec<Subscription>,
    original_content: String,
    saved_query_id: Option<Uuid>,

    // Execution
    execution_history: Vec<ExecutionRecord>,
    active_execution_index: Option<usize>,
    pending_result: Option<PendingQueryResult>,

    // Result tabs
    result_tabs: Vec<ResultTab>,
    active_result_index: Option<usize>,
    result_tab_counter: usize,
    run_in_new_tab: bool,

    // History modal
    history_modal: Entity<HistoryModal>,
    _history_subscriptions: Vec<Subscription>,
    pending_set_query: Option<HistoryQuerySelected>,

    // Layout/focus
    layout: SqlQueryLayout,
    focus_handle: FocusHandle,
    focus_mode: SqlQueryFocus,
    active_cancel_token: Option<CancelToken>,
    results_maximized: bool,

    // Dangerous query confirmation
    pending_dangerous_query: Option<PendingDangerousQuery>,
}

struct PendingQueryResult {
    exec_id: Uuid,
    query: String,
    result: Result<QueryResult, DbError>,
}

/// Pending dangerous query confirmation.
struct PendingDangerousQuery {
    query: String,
    kind: DangerousQueryKind,
    in_new_tab: bool,
}

/// Record of a query execution.
#[derive(Clone)]
pub struct ExecutionRecord {
    pub id: Uuid,
    pub started_at: Instant,
    pub finished_at: Option<Instant>,
    pub result: Option<Arc<QueryResult>>,
    pub error: Option<String>,
    pub rows_affected: Option<u64>,
}

impl SqlQueryDocument {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let connection_id = app_state.read(cx).active_connection_id();

        // Get query language from the active connection, default to SQL
        let query_language = connection_id
            .and_then(|id| app_state.read(cx).connections().get(&id))
            .map(|conn| conn.connection.metadata().query_language)
            .unwrap_or(dbflux_core::QueryLanguage::Sql);

        let editor_mode = query_language.editor_mode();
        let placeholder = query_language.placeholder();

        let input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor(editor_mode)
                .line_number(true)
                .soft_wrap(false)
                .placeholder(placeholder)
        });

        let completion_provider: Rc<dyn CompletionProvider> = Rc::new(
            QueryCompletionProvider::new(query_language, app_state.clone(), connection_id),
        );
        input_state.update(cx, |state, _cx| {
            state.lsp.completion_provider = Some(completion_provider);
        });

        let input_change_sub = cx.subscribe_in(
            &input_state,
            window,
            |this, _input, event: &InputEvent, window, cx| match event {
                InputEvent::Change => {
                    this.refresh_editor_diagnostics(window, cx);
                }
                InputEvent::Focus => {
                    this.enter_editor_mode(cx);
                }
                InputEvent::Blur | InputEvent::PressEnter { .. } => {}
            },
        );

        // Create history modal
        let history_modal = cx.new(|cx| HistoryModal::new(app_state.clone(), window, cx));

        // Subscribe to history modal events
        let query_selected_sub = cx.subscribe(
            &history_modal,
            |this, _, event: &HistoryQuerySelected, cx| {
                this.pending_set_query = Some(event.clone());
                cx.notify();
            },
        );

        Self {
            id: DocumentId::new(),
            title: "Query 1".to_string(),
            state: DocumentState::Clean,
            connection_id,
            app_state,
            input_state,
            _input_subscriptions: vec![input_change_sub],
            original_content: String::new(),
            saved_query_id: None,
            execution_history: Vec::new(),
            active_execution_index: None,
            pending_result: None,
            result_tabs: Vec::new(),
            active_result_index: None,
            result_tab_counter: 0,
            run_in_new_tab: false,
            history_modal,
            _history_subscriptions: vec![query_selected_sub],
            pending_set_query: None,
            layout: SqlQueryLayout::EditorOnly,
            focus_handle: cx.focus_handle(),
            focus_mode: SqlQueryFocus::Editor,
            active_cancel_token: None,
            results_maximized: false,
            pending_dangerous_query: None,
        }
    }

    /// Sets the document content.
    pub fn set_content(&mut self, sql: &str, window: &mut Window, cx: &mut Context<Self>) {
        let sql_owned = sql.to_string();
        self.input_state
            .update(cx, |state, cx| state.set_value(&sql_owned, window, cx));
        self.original_content = sql_owned;
        self.refresh_editor_diagnostics(window, cx);
    }

    fn refresh_editor_diagnostics(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let query_text = self.input_state.read(cx).value().to_string();

        let diagnostics = if let Some(conn_id) = self.connection_id {
            if let Some(connected) = self.app_state.read(cx).connections().get(&conn_id) {
                connected
                    .connection
                    .language_service()
                    .editor_diagnostics(&query_text)
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        self.input_state.update(cx, |state, cx| {
            let text = state.text().clone();

            let Some(diagnostic_set) = state.diagnostics_mut() else {
                return;
            };

            diagnostic_set.reset(&text);

            for diagnostic in diagnostics {
                diagnostic_set.push(Self::to_input_diagnostic(diagnostic));
            }

            cx.notify();
        });
    }

    fn to_input_diagnostic(diagnostic: CoreEditorDiagnostic) -> InputDiagnostic {
        let severity = match diagnostic.severity {
            CoreDiagnosticSeverity::Error => InputDiagnosticSeverity::Error,
            CoreDiagnosticSeverity::Warning => InputDiagnosticSeverity::Warning,
            CoreDiagnosticSeverity::Info => InputDiagnosticSeverity::Info,
            CoreDiagnosticSeverity::Hint => InputDiagnosticSeverity::Hint,
        };

        let start = InputPosition::new(diagnostic.range.start.line, diagnostic.range.start.column);
        let mut end = InputPosition::new(diagnostic.range.end.line, diagnostic.range.end.column);

        if end.line == start.line && end.character <= start.character {
            end.character = start.character.saturating_add(1);
        }

        InputDiagnostic::new(start..end, diagnostic.message).with_severity(severity)
    }

    /// Creates document with specific title.
    pub fn with_title(mut self, title: String) -> Self {
        self.title = title;
        self
    }

    // === Accessors for DocumentHandle ===

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        self.title.clone()
    }

    pub fn state(&self) -> DocumentState {
        self.state
    }

    pub fn connection_id(&self) -> Option<Uuid> {
        self.connection_id
    }

    pub fn can_close(&self, cx: &App) -> bool {
        !self.has_unsaved_changes(cx)
    }

    /// Returns true if the editor content differs from the original content.
    pub fn has_unsaved_changes(&self, cx: &App) -> bool {
        let current = self.input_state.read(cx).value();
        current != self.original_content
    }

    fn enter_editor_mode(&mut self, cx: &mut Context<Self>) {
        if self.focus_mode != SqlQueryFocus::Editor {
            self.focus_mode = SqlQueryFocus::Editor;
            cx.notify();
        }
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_handle.focus(window);

        if self.focus_mode == SqlQueryFocus::Editor {
            self.input_state
                .update(cx, |state, cx| state.focus(window, cx));
        }
    }

    /// Returns the active context for keyboard handling based on internal focus.
    pub fn active_context(&self, cx: &App) -> ContextId {
        if self.pending_dangerous_query.is_some() {
            return ContextId::ConfirmModal;
        }

        if self.history_modal.read(cx).is_visible() {
            return ContextId::HistoryModal;
        }

        // Check if the active result tab's grid has a modal, context menu, or inline edit open
        if self.focus_mode == SqlQueryFocus::Results
            && let Some(index) = self.active_result_index
            && let Some(tab) = self.result_tabs.get(index)
        {
            let grid_context = tab.grid.read(cx).active_context(cx);

            if grid_context != ContextId::Results {
                return grid_context;
            }
        }

        match self.focus_mode {
            SqlQueryFocus::Editor => ContextId::Editor,
            SqlQueryFocus::Results => ContextId::Results,
        }
    }

    // === Query Execution ===

    /// Returns selected text when a non-empty selection exists.
    fn selected_query(&self, window: &mut Window, cx: &mut Context<Self>) -> Option<String> {
        self.input_state.update(cx, |state, cx| {
            let sel = state.selected_text_range(false, window, cx)?;

            if sel.range.is_empty() {
                return None;
            }

            let mut adjusted = None;
            state
                .text_for_range(sel.range, &mut adjusted, window, cx)
                .map(|text| text.trim().to_string())
                .filter(|text| !text.is_empty())
        })
    }

    /// Returns the selected text if a selection exists, otherwise the full editor content.
    fn selected_or_full_query(&self, window: &mut Window, cx: &mut Context<Self>) -> String {
        self.selected_query(window, cx)
            .unwrap_or_else(|| self.input_state.read(cx).value().to_string())
    }

    pub fn run_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.run_query_impl(false, window, cx);
    }

    pub fn run_selected_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(query) = self.selected_query(window, cx) else {
            cx.toast_warning("Select query text to run", window);
            return;
        };

        self.run_query_text(query, false, window, cx);
    }

    fn run_query_impl(&mut self, in_new_tab: bool, window: &mut Window, cx: &mut Context<Self>) {
        let query = self.selected_or_full_query(window, cx);
        self.run_query_text(query, in_new_tab, window, cx);
    }

    fn run_query_text(
        &mut self,
        query: String,
        in_new_tab: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if query.trim().is_empty() {
            cx.toast_warning("Enter a query to run", window);
            return;
        }

        // Check for dangerous queries
        if let Some(kind) = detect_dangerous_query(&query) {
            let is_suppressed = self
                .app_state
                .read(cx)
                .dangerous_query_suppressions()
                .is_suppressed(kind);

            if !is_suppressed {
                self.pending_dangerous_query = Some(PendingDangerousQuery {
                    query,
                    kind,
                    in_new_tab,
                });
                cx.notify();
                return;
            }
        }

        // Validate query using the connection's language service
        if let Some(conn_id) = self.connection_id
            && let Some(connected) = self.app_state.read(cx).connections().get(&conn_id)
        {
            let lang = connected.connection.language_service();
            match lang.validate(&query) {
                ValidationResult::Valid => {}
                ValidationResult::SyntaxError(diag) => {
                    let msg = match diag.hint {
                        Some(ref hint) => format!("{}\nHint: {}", diag.message, hint),
                        None => diag.message,
                    };
                    cx.toast_error(msg, window);
                    return;
                }
                ValidationResult::WrongLanguage { message, .. } => {
                    cx.toast_error(message, window);
                    return;
                }
            }
        }

        self.execute_query_internal(query, in_new_tab, window, cx);
    }

    fn execute_query_internal(
        &mut self,
        query: String,
        in_new_tab: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(conn_id) = self.connection_id else {
            cx.toast_error("No active connection", window);
            return;
        };

        let connection = self
            .app_state
            .read(cx)
            .connections()
            .get(&conn_id)
            .map(|c| c.connection.clone());

        let Some(connection) = connection else {
            cx.toast_error("Connection not found", window);
            return;
        };

        // Set the new tab flag before execution
        self.run_in_new_tab = in_new_tab;

        // Create cancel token for this execution
        let cancel_token = CancelToken::new();
        self.active_cancel_token = Some(cancel_token.clone());

        // Create execution record
        let exec_id = Uuid::new_v4();
        let record = ExecutionRecord {
            id: exec_id,
            started_at: Instant::now(),
            finished_at: None,
            result: None,
            error: None,
            rows_affected: None,
        };
        self.execution_history.push(record);
        self.active_execution_index = Some(self.execution_history.len() - 1);

        // Change state
        self.state = DocumentState::Executing;
        cx.emit(DocumentEvent::ExecutionStarted);
        cx.notify();

        // Get active database for MySQL/MariaDB
        let active_database = self
            .app_state
            .read(cx)
            .connections()
            .get(&conn_id)
            .and_then(|c| c.active_database.clone());

        // Execute in background
        let request = QueryRequest::new(query.clone()).with_database(active_database);

        let task = cx.background_executor().spawn({
            let connection = connection.clone();
            async move { connection.execute(&request) }
        });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            cx.update(|cx| {
                this.update(cx, |doc, cx| {
                    // Store pending result to be processed in render (where we have window)
                    doc.pending_result = Some(PendingQueryResult {
                        exec_id,
                        query,
                        result,
                    });
                    cx.notify();
                })
                .ok();
            })
            .ok();
        })
        .detach();
    }

    fn confirm_dangerous_query(
        &mut self,
        suppress: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(pending) = self.pending_dangerous_query.take() else {
            return;
        };

        if suppress {
            self.app_state.update(cx, |state, _| {
                state
                    .dangerous_query_suppressions_mut()
                    .set_suppressed(pending.kind);
            });
        }

        self.execute_query_internal(pending.query, pending.in_new_tab, window, cx);
    }

    fn cancel_dangerous_query(&mut self, cx: &mut Context<Self>) {
        self.pending_dangerous_query = None;
        cx.notify();
    }

    /// Process pending query selected from history modal (called from render).
    fn process_pending_set_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(selected) = self.pending_set_query.take() else {
            return;
        };

        // Set the query content in the editor
        self.input_state
            .update(cx, |state, cx| state.set_value(&selected.sql, window, cx));

        // Update title if a name was provided
        if let Some(name) = selected.name {
            self.title = name;
        }

        // Track the saved query ID if this came from saved queries
        self.saved_query_id = selected.saved_query_id;

        // Focus back on editor
        self.focus_mode = SqlQueryFocus::Editor;

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    /// Process pending query result (called from render where we have window access).
    fn process_pending_result(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(pending) = self.pending_result.take() else {
            return;
        };

        self.active_cancel_token = None;
        self.state = DocumentState::Clean;

        let Some(record) = self
            .execution_history
            .iter_mut()
            .find(|r| r.id == pending.exec_id)
        else {
            return;
        };

        record.finished_at = Some(Instant::now());

        match pending.result {
            Ok(qr) => {
                let row_count = qr.rows.len();
                let execution_time = qr.execution_time;
                record.rows_affected = Some(row_count as u64);
                let arc_result = Arc::new(qr);
                record.result = Some(arc_result.clone());

                // Add to global history
                let (database, connection_name) = self
                    .connection_id
                    .and_then(|id| self.app_state.read(cx).connections().get(&id))
                    .map(|c| (c.active_database.clone(), Some(c.profile.name.clone())))
                    .unwrap_or((None, None));

                let history_entry = HistoryEntry::new(
                    pending.query.clone(),
                    database,
                    connection_name,
                    execution_time,
                    Some(row_count),
                );
                self.app_state.update(cx, |state, _| {
                    state.add_history_entry(history_entry);
                });

                self.setup_data_grid(arc_result, pending.query, window, cx);

                if self.layout == SqlQueryLayout::EditorOnly {
                    self.layout = SqlQueryLayout::Split;
                }

                self.focus_mode = SqlQueryFocus::Results;
            }
            Err(e) => {
                let error_msg = e.to_string();
                record.error = Some(error_msg.clone());
                self.state = DocumentState::Error;
                cx.toast_error(format!("Query failed: {}", error_msg), window);
            }
        }

        cx.emit(DocumentEvent::ExecutionFinished);
        cx.emit(DocumentEvent::MetaChanged);
    }

    fn setup_data_grid(
        &mut self,
        result: Arc<QueryResult>,
        query: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let should_create_new_tab = self.run_in_new_tab
            || self.result_tabs.is_empty()
            || self.active_result_index.is_none();

        self.run_in_new_tab = false;

        if should_create_new_tab {
            self.create_result_tab(result, query, window, cx);
        } else if let Some(index) = self.active_result_index
            && let Some(tab) = self.result_tabs.get_mut(index)
        {
            tab.grid
                .update(cx, |g, cx| g.set_query_result(result, query.clone(), cx));
        }
    }

    fn create_result_tab(
        &mut self,
        result: Arc<QueryResult>,
        query: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.result_tab_counter += 1;
        let tab_id = Uuid::new_v4();
        let title = format!("Result {}", self.result_tab_counter);

        let app_state = self.app_state.clone();
        let grid = cx
            .new(|cx| DataGridPanel::new_for_result(result, query.clone(), app_state, window, cx));

        let subscription = cx.subscribe(
            &grid,
            |this, _grid, event: &DataGridEvent, cx| match event {
                DataGridEvent::RequestHide => {
                    this.hide_results(cx);
                }
                DataGridEvent::RequestToggleMaximize => {
                    this.toggle_maximize_results(cx);
                }
                DataGridEvent::Focused => {
                    this.focus_mode = SqlQueryFocus::Results;
                    cx.emit(DocumentEvent::RequestFocus);
                    cx.notify();
                }
                DataGridEvent::RequestSqlPreview {
                    profile_id,
                    schema_name,
                    table_name,
                    column_names,
                    row_values,
                    pk_indices,
                    generation_type,
                } => {
                    cx.emit(DocumentEvent::RequestSqlPreview {
                        profile_id: *profile_id,
                        schema_name: schema_name.clone(),
                        table_name: table_name.clone(),
                        column_names: column_names.clone(),
                        row_values: row_values.clone(),
                        pk_indices: pk_indices.clone(),
                        generation_type: *generation_type,
                    });
                }
            },
        );

        let tab = ResultTab {
            id: tab_id,
            title,
            grid,
            _subscription: subscription,
        };

        self.result_tabs.push(tab);
        self.active_result_index = Some(self.result_tabs.len() - 1);
    }

    pub fn cancel_query(&mut self, cx: &mut Context<Self>) {
        if let Some(token) = self.active_cancel_token.take() {
            token.cancel();
            self.state = DocumentState::Clean;
            cx.emit(DocumentEvent::MetaChanged);
            cx.notify();
        }
    }

    pub fn hide_results(&mut self, cx: &mut Context<Self>) {
        self.layout = SqlQueryLayout::EditorOnly;
        self.focus_mode = SqlQueryFocus::Editor;
        self.results_maximized = false;
        cx.notify();
    }

    pub fn toggle_maximize_results(&mut self, cx: &mut Context<Self>) {
        if self.results_maximized {
            self.layout = SqlQueryLayout::Split;
            self.results_maximized = false;
        } else {
            self.layout = SqlQueryLayout::ResultsOnly;
            self.results_maximized = true;
        }

        // Update the active grid's maximized state
        if let Some(grid) = self.active_result_grid() {
            grid.update(cx, |g, cx| g.set_maximized(self.results_maximized, cx));
        }

        cx.notify();
    }

    pub fn run_query_in_new_tab(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.run_query_impl(true, window, cx);
    }

    pub fn close_result_tab(&mut self, tab_id: Uuid, cx: &mut Context<Self>) {
        let Some(index) = self.result_tabs.iter().position(|t| t.id == tab_id) else {
            return;
        };

        self.result_tabs.remove(index);

        if self.result_tabs.is_empty() {
            self.active_result_index = None;
            self.layout = SqlQueryLayout::EditorOnly;
            self.focus_mode = SqlQueryFocus::Editor;
        } else if let Some(active) = self.active_result_index {
            if active >= self.result_tabs.len() {
                self.active_result_index = Some(self.result_tabs.len() - 1);
            } else if active > index {
                self.active_result_index = Some(active - 1);
            }
        }

        cx.notify();
    }

    pub fn activate_result_tab(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.result_tabs.len() {
            self.active_result_index = Some(index);
            cx.notify();
        }
    }

    fn active_result_grid(&self) -> Option<Entity<DataGridPanel>> {
        self.active_result_index
            .and_then(|i| self.result_tabs.get(i))
            .map(|tab| tab.grid.clone())
    }

    // === Command Dispatch ===

    /// Route commands to the history modal when it's visible.
    fn dispatch_to_history_modal(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match cmd {
            Command::Cancel => {
                self.history_modal.update(cx, |modal, cx| modal.close(cx));
                true
            }
            Command::SelectNext => {
                self.history_modal
                    .update(cx, |modal, cx| modal.select_next(cx));
                true
            }
            Command::SelectPrev => {
                self.history_modal
                    .update(cx, |modal, cx| modal.select_prev(cx));
                true
            }
            Command::Execute => {
                self.history_modal
                    .update(cx, |modal, cx| modal.execute_selected(window, cx));
                true
            }
            Command::Delete => {
                self.history_modal
                    .update(cx, |modal, cx| modal.delete_selected(cx));
                true
            }
            Command::ToggleFavorite => {
                self.history_modal
                    .update(cx, |modal, cx| modal.toggle_favorite_selected(cx));
                true
            }
            Command::Rename => {
                self.history_modal
                    .update(cx, |modal, cx| modal.start_rename_selected(window, cx));
                true
            }
            Command::FocusSearch => {
                self.history_modal
                    .update(cx, |modal, cx| modal.focus_search(window, cx));
                true
            }
            Command::SaveQuery => {
                self.history_modal
                    .update(cx, |modal, cx| modal.save_selected_history(window, cx));
                true
            }
            // Other commands are not handled by the modal
            _ => false,
        }
    }

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        // When dangerous query confirmation is showing, handle only modal commands
        if self.pending_dangerous_query.is_some() {
            match cmd {
                Command::Cancel => {
                    self.cancel_dangerous_query(cx);
                    return true;
                }
                Command::Execute => {
                    self.confirm_dangerous_query(false, window, cx);
                    return true;
                }
                _ => return false,
            }
        }

        // When history modal is open, route commands to it first
        if self.history_modal.read(cx).is_visible()
            && self.dispatch_to_history_modal(cmd, window, cx)
        {
            return true;
        }

        // When focused on results, delegate to active DataGridPanel
        if self.focus_mode == SqlQueryFocus::Results
            && let Some(grid) = self.active_result_grid()
        {
            // Special handling for FocusUp to exit results
            if cmd == Command::FocusUp {
                self.focus_mode = SqlQueryFocus::Editor;
                cx.notify();
                return true;
            }

            // Delegate to grid
            let handled = grid.update(cx, |g, cx| g.dispatch_command(cmd, window, cx));
            if handled {
                return true;
            }
        }

        match cmd {
            Command::RunQuery => {
                self.run_query(window, cx);
                true
            }
            Command::RunQueryInNewTab => {
                self.run_query_in_new_tab(window, cx);
                true
            }
            Command::Cancel | Command::CancelQuery => {
                if self.active_cancel_token.is_some() {
                    self.cancel_query(cx);
                    true
                } else {
                    false
                }
            }

            // Focus navigation from editor to results
            Command::FocusDown => {
                if self.focus_mode == SqlQueryFocus::Editor && !self.result_tabs.is_empty() {
                    self.focus_mode = SqlQueryFocus::Results;
                    cx.notify();
                    true
                } else {
                    false
                }
            }

            // Layout toggles
            Command::ToggleEditor => {
                self.layout = match self.layout {
                    SqlQueryLayout::EditorOnly => SqlQueryLayout::Split,
                    _ => SqlQueryLayout::EditorOnly,
                };
                cx.notify();
                true
            }
            Command::ToggleResults | Command::TogglePanel => {
                self.layout = match self.layout {
                    SqlQueryLayout::ResultsOnly => SqlQueryLayout::Split,
                    _ => SqlQueryLayout::ResultsOnly,
                };
                cx.notify();
                true
            }

            // History modal commands
            Command::ToggleHistoryDropdown => {
                let is_open = self.history_modal.read(cx).is_visible();
                if is_open {
                    self.history_modal.update(cx, |modal, cx| modal.close(cx));
                } else {
                    self.history_modal
                        .update(cx, |modal, cx| modal.open(window, cx));
                }
                true
            }
            Command::OpenSavedQueries => {
                self.history_modal
                    .update(cx, |modal, cx| modal.open_saved_tab(window, cx));
                true
            }
            Command::SaveQuery => {
                let sql = self.input_state.read(cx).value().to_string();
                if sql.trim().is_empty() {
                    cx.toast_warning("Enter a query to save", window);
                } else {
                    self.history_modal
                        .update(cx, |modal, cx| modal.open_save(sql, window, cx));
                }
                true
            }

            _ => false,
        }
    }

    // === Render ===

    fn render_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_executing = self.state == DocumentState::Executing;

        let (run_icon, run_label, run_enabled) = if is_executing {
            (AppIcon::X, "Cancel", true)
        } else {
            (AppIcon::Play, "Run", true)
        };

        let btn_bg = theme.secondary;
        let primary = theme.primary;

        let execution_time = self
            .active_execution_index
            .and_then(|i| self.execution_history.get(i))
            .and_then(|r| {
                r.finished_at
                    .map(|finished| finished.duration_since(r.started_at))
            });

        div()
            .id("sql-toolbar")
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(
                div()
                    .id("run-query-btn")
                    .flex()
                    .items_center()
                    .gap_1()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .text_xs()
                    .when(run_enabled, |el| {
                        el.bg(if is_executing { theme.danger } else { primary })
                            .text_color(theme.background)
                            .hover(|d| d.opacity(0.9))
                    })
                    .when(!run_enabled, |el| {
                        el.bg(btn_bg)
                            .text_color(theme.muted_foreground)
                            .cursor_not_allowed()
                    })
                    .on_click(cx.listener(move |this, _, window, cx| {
                        if this.state == DocumentState::Executing {
                            this.cancel_query(cx);
                        } else {
                            this.run_query(window, cx);
                        }
                    }))
                    .child(
                        svg()
                            .path(run_icon.path())
                            .size_3()
                            .text_color(if run_enabled {
                                theme.background
                            } else {
                                theme.muted_foreground
                            }),
                    )
                    .child(run_label),
            )
            .when(!is_executing, |el| {
                el.child(
                    div()
                        .id("run-in-new-tab-btn")
                        .flex()
                        .items_center()
                        .gap_1()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .rounded(Radii::SM)
                        .cursor_pointer()
                        .text_xs()
                        .bg(btn_bg)
                        .text_color(theme.foreground)
                        .hover(|d| d.bg(theme.secondary_hover))
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.run_query_in_new_tab(window, cx);
                        }))
                        .child(
                            svg()
                                .path(AppIcon::SquarePlay.path())
                                .size_3()
                                .text_color(theme.foreground),
                        )
                        .child("New tab"),
                )
                .child(
                    div()
                        .id("run-selection-btn")
                        .flex()
                        .items_center()
                        .gap_1()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .rounded(Radii::SM)
                        .cursor_pointer()
                        .text_xs()
                        .bg(btn_bg)
                        .text_color(theme.foreground)
                        .hover(|d| d.bg(theme.secondary_hover))
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.run_selected_query(window, cx);
                        }))
                        .child(
                            svg()
                                .path(AppIcon::ScrollText.path())
                                .size_3()
                                .text_color(theme.foreground),
                        )
                        .child("Selection"),
                )
            })
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child("Ctrl+Enter (selection/full)"),
            )
            .child(div().flex_1())
            .when_some(execution_time, |el, duration| {
                el.child(
                    div()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child(format!("{:.2}s", duration.as_secs_f64())),
                )
            })
    }

    fn render_editor(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_focused = self.focus_mode == SqlQueryFocus::Editor;
        let bg = cx.theme().background;
        let accent = cx.theme().accent;

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(bg)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, window, cx| {
                    this.enter_editor_mode(cx);
                    this.input_state
                        .update(cx, |state, cx| state.focus(window, cx));
                    cx.emit(DocumentEvent::RequestFocus);
                }),
            )
            .when(is_focused, |el| {
                el.border_2().border_color(accent.opacity(0.3))
            })
            .child(
                div().flex_1().overflow_hidden().child(
                    Input::new(&self.input_state)
                        .appearance(false)
                        .w_full()
                        .h_full(),
                ),
            )
    }

    fn render_results(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_focused = self.focus_mode == SqlQueryFocus::Results;
        let bg = cx.theme().background;
        let accent = cx.theme().accent;

        let error = self
            .active_execution_index
            .and_then(|i| self.execution_history.get(i))
            .and_then(|r| r.error.clone());

        let has_error = error.is_some();
        let active_grid = self.active_result_grid();
        let has_grid = active_grid.is_some();
        let has_tabs = !self.result_tabs.is_empty();

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(bg)
            .when(is_focused, |el| {
                el.border_2().border_color(accent.opacity(0.3))
            })
            .when(has_tabs, |el| el.child(self.render_results_header(cx)))
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .when_some(error, |el, err| el.child(self.render_error_state(&err, cx)))
                    .when_some(active_grid, |el, grid| el.child(grid))
                    .when(!has_grid && !has_error, |el| {
                        el.child(self.render_empty_results(cx))
                    }),
            )
    }

    fn render_results_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let active_index = self.active_result_index;

        div()
            .id("results-header")
            .flex()
            .items_center()
            .h(Heights::TAB)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .overflow_x_hidden()
                    .flex_1()
                    .children(self.result_tabs.iter().enumerate().map(|(i, tab)| {
                        let is_active = active_index == Some(i);
                        let tab_id = tab.id;

                        div()
                            .id(ElementId::Name(format!("result-tab-{}", tab.id).into()))
                            .flex()
                            .items_center()
                            .gap_1()
                            .px(Spacing::SM)
                            .py(Spacing::XS)
                            .rounded(Radii::SM)
                            .cursor_pointer()
                            .text_xs()
                            .when(is_active, |el| {
                                el.bg(theme.secondary).text_color(theme.foreground)
                            })
                            .when(!is_active, |el| {
                                el.text_color(theme.muted_foreground)
                                    .hover(|d| d.bg(theme.secondary.opacity(0.5)))
                            })
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.activate_result_tab(i, cx);
                            }))
                            .child(tab.title.clone())
                            .child(
                                div()
                                    .id(ElementId::Name(
                                        format!("close-result-tab-{}", tab.id).into(),
                                    ))
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .size_4()
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .hover(|d| d.bg(theme.danger.opacity(0.2)))
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.close_result_tab(tab_id, cx);
                                    }))
                                    .child(
                                        svg()
                                            .path(AppIcon::X.path())
                                            .size_3()
                                            .text_color(theme.muted_foreground),
                                    ),
                            )
                    })),
            )
            .child(div().flex_1())
            .child(self.render_results_controls(cx))
    }

    fn render_results_controls(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_maximized = self.results_maximized;

        div()
            .flex()
            .items_center()
            .gap_1()
            .child(
                div()
                    .id("toggle-maximize-results")
                    .flex()
                    .items_center()
                    .justify_center()
                    .size_6()
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.toggle_maximize_results(cx);
                    }))
                    .child(
                        svg()
                            .path(if is_maximized {
                                AppIcon::Minimize2.path()
                            } else {
                                AppIcon::Maximize2.path()
                            })
                            .size_3p5()
                            .text_color(theme.muted_foreground),
                    ),
            )
            .child(
                div()
                    .id("hide-results-panel")
                    .flex()
                    .items_center()
                    .justify_center()
                    .size_6()
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.hide_results(cx);
                    }))
                    .child(
                        svg()
                            .path(AppIcon::PanelBottomClose.path())
                            .size_3p5()
                            .text_color(theme.muted_foreground),
                    ),
            )
    }

    fn render_collapsed_results_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let tab_count = self.result_tabs.len();

        div()
            .id("collapsed-results-bar")
            .flex()
            .items_center()
            .h(Heights::TAB)
            .px(Spacing::SM)
            .border_t_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(format!(
                        "{} result{}",
                        tab_count,
                        if tab_count == 1 { "" } else { "s" }
                    )),
            )
            .child(div().flex_1())
            .child(
                div()
                    .id("expand-results-panel")
                    .flex()
                    .items_center()
                    .justify_center()
                    .size_6()
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.layout = SqlQueryLayout::Split;
                        cx.notify();
                    }))
                    .child(
                        svg()
                            .path(AppIcon::PanelBottomOpen.path())
                            .size_3p5()
                            .text_color(theme.muted_foreground),
                    ),
            )
    }

    fn render_error_state(&self, error: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let error_color = cx.theme().danger;
        let muted_fg = cx.theme().muted_foreground;

        div()
            .size_full()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .gap_2()
            .child(
                div()
                    .text_color(error_color)
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child("Query Error"),
            )
            .child(
                div()
                    .text_color(muted_fg)
                    .text_sm()
                    .max_w(px(500.0))
                    .text_center()
                    .child(error.to_string()),
            )
    }

    fn render_empty_results(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let muted_fg = cx.theme().muted_foreground;

        div()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .child(
                div()
                    .text_color(muted_fg)
                    .child("Run a query to see results"),
            )
    }

    fn render_dangerous_query_modal(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let entity = cx.entity().clone();
        let entity_cancel = cx.entity().clone();
        let entity_suppress = cx.entity().clone();

        let (title, message) = self
            .pending_dangerous_query
            .as_ref()
            .map(|p| {
                let title = match p.kind {
                    DangerousQueryKind::DeleteNoWhere => "DELETE without WHERE",
                    DangerousQueryKind::UpdateNoWhere => "UPDATE without WHERE",
                    DangerousQueryKind::Truncate => "TRUNCATE",
                    DangerousQueryKind::Drop => "DROP",
                    DangerousQueryKind::Alter => "ALTER",
                    DangerousQueryKind::Script => "Dangerous Script",
                    DangerousQueryKind::MongoDeleteMany => "deleteMany with empty filter",
                    DangerousQueryKind::MongoUpdateMany => "updateMany with empty filter",
                    DangerousQueryKind::MongoDropCollection => "drop() collection",
                    DangerousQueryKind::MongoDropDatabase => "dropDatabase()",
                };
                (title, p.kind.message())
            })
            .unwrap_or(("Warning", "This query may be dangerous."));

        let btn_hover = theme.muted;

        div()
            .id("dangerous-query-modal-overlay")
            .absolute()
            .inset_0()
            .bg(gpui::hsla(0.0, 0.0, 0.0, 0.5))
            .flex()
            .items_center()
            .justify_center()
            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                cx.stop_propagation();
            })
            .child(
                div()
                    .bg(theme.background)
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::MD)
                    .p(Spacing::MD)
                    .min_w(px(350.0))
                    .max_w(px(500.0))
                    .flex()
                    .flex_col()
                    .gap(Spacing::MD)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(
                                svg()
                                    .path(AppIcon::TriangleAlert.path())
                                    .size_5()
                                    .text_color(theme.warning),
                            )
                            .child(
                                div()
                                    .text_size(FontSizes::SM)
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(theme.foreground)
                                    .child(title),
                            ),
                    )
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.muted_foreground)
                            .child(message),
                    )
                    .child(
                        div()
                            .flex()
                            .justify_between()
                            .items_center()
                            .child(
                                div()
                                    .id("dont-ask-again-btn")
                                    .flex()
                                    .items_center()
                                    .gap_1()
                                    .px(Spacing::SM)
                                    .py(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .text_size(FontSizes::XS)
                                    .text_color(theme.muted_foreground)
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(move |_, window, cx| {
                                        entity_suppress.update(cx, |doc, cx| {
                                            doc.confirm_dangerous_query(true, window, cx);
                                        });
                                    })
                                    .child("Don't ask again"),
                            )
                            .child(
                                div()
                                    .flex()
                                    .gap(Spacing::SM)
                                    .child(
                                        div()
                                            .id("dangerous-cancel-btn")
                                            .flex()
                                            .items_center()
                                            .gap_1()
                                            .px(Spacing::SM)
                                            .py(Spacing::XS)
                                            .rounded(Radii::SM)
                                            .cursor_pointer()
                                            .text_size(FontSizes::SM)
                                            .text_color(theme.muted_foreground)
                                            .bg(theme.secondary)
                                            .hover(|d| d.bg(btn_hover))
                                            .on_click(move |_, _, cx| {
                                                entity_cancel.update(cx, |doc, cx| {
                                                    doc.cancel_dangerous_query(cx);
                                                });
                                            })
                                            .child("Cancel"),
                                    )
                                    .child(
                                        div()
                                            .id("dangerous-confirm-btn")
                                            .flex()
                                            .items_center()
                                            .gap_1()
                                            .px(Spacing::SM)
                                            .py(Spacing::XS)
                                            .rounded(Radii::SM)
                                            .cursor_pointer()
                                            .text_size(FontSizes::SM)
                                            .text_color(theme.background)
                                            .bg(theme.warning)
                                            .hover(|d| d.opacity(0.9))
                                            .on_click(move |_, window, cx| {
                                                entity.update(cx, |doc, cx| {
                                                    doc.confirm_dangerous_query(false, window, cx);
                                                });
                                            })
                                            .child("Run Anyway"),
                                    ),
                            ),
                    ),
            )
    }
}

impl Render for SqlQueryDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Process any pending query result (needs window access)
        self.process_pending_result(window, cx);

        // Process any pending query from history modal selection
        self.process_pending_set_query(window, cx);

        let toolbar = self.render_toolbar(cx).into_any_element();
        let editor_view = self.render_editor(window, cx).into_any_element();
        let results_view = self.render_results(window, cx).into_any_element();

        let bg = cx.theme().background;
        let has_collapsed_results =
            self.layout == SqlQueryLayout::EditorOnly && !self.result_tabs.is_empty();

        div()
            .id(ElementId::Name(format!("sql-doc-{}", self.id.0).into()))
            .size_full()
            .flex()
            .flex_col()
            .bg(bg)
            .track_focus(&self.focus_handle)
            // Toolbar at top
            .child(toolbar)
            // Content area (editor/results split)
            .child(
                div().flex_1().overflow_hidden().child(match self.layout {
                    SqlQueryLayout::Split => {
                        v_resizable(SharedString::from(format!("sql-split-{}", self.id.0)))
                            .child(
                                resizable_panel()
                                    .size(px(200.0))
                                    .size_range(px(100.0)..px(1000.0))
                                    .child(editor_view),
                            )
                            .child(
                                resizable_panel()
                                    .size(px(200.0))
                                    .size_range(px(100.0)..px(1000.0))
                                    .child(results_view),
                            )
                            .into_any_element()
                    }

                    SqlQueryLayout::EditorOnly => editor_view,

                    SqlQueryLayout::ResultsOnly => results_view,
                }),
            )
            // Collapsed results bar (when in EditorOnly with results)
            .when(has_collapsed_results, |el| {
                el.child(self.render_collapsed_results_bar(cx))
            })
            // History modal overlay
            .child(self.history_modal.clone())
            // Dangerous query confirmation modal
            .when(self.pending_dangerous_query.is_some(), |el| {
                el.child(self.render_dangerous_query_modal(cx))
            })
    }
}

impl EventEmitter<DocumentEvent> for SqlQueryDocument {}
