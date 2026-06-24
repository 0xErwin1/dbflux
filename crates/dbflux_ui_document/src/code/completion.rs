use super::*;
use crate::completion_support::{
    byte_offset_to_lsp_position, completion_replace_range, extract_identifier_prefix,
    is_identifier_byte, normalize_identifier, push_completion_item, scan_identifier_start,
};

pub(super) struct QueryCompletionProvider {
    query_language: dbflux_core::QueryLanguage,
    app_state: Entity<AppStateEntity>,
    connection_id: Option<Uuid>,
}

impl QueryCompletionProvider {
    pub(super) fn new(
        query_language: dbflux_core::QueryLanguage,
        app_state: Entity<AppStateEntity>,
        connection_id: Option<Uuid>,
    ) -> Self {
        Self {
            query_language,
            app_state,
            connection_id,
        }
    }

    /// Resolves the connected driver's editor mode (e.g. `"sql"`, `"javascript"`)
    /// from its `DriverMetadata::editor_profile()`.
    ///
    /// This keys completion-style routing off a generic capability signal rather
    /// than a driver id: any driver whose editor mode is `"sql"` (relational SQL
    /// drivers and DynamoDB's PartiQL surface alike) gets SQL-style completion.
    /// Falls back to deriving the mode from the provider's `QueryLanguage` when
    /// the connection is absent or its language no longer matches (a
    /// source-context query-mode override).
    fn resolved_editor_mode(&self, cx: &App) -> String {
        if let Some(connection_id) = self.connection_id
            && let Some(connected) = self.app_state.read(cx).connections().get(&connection_id)
        {
            let metadata = connected.connection.metadata();
            if metadata.query_language == self.query_language {
                return metadata.editor_profile().editor_mode;
            }
        }

        dbflux_core::EditorLanguageProfile::from_language(&self.query_language).editor_mode
    }

    /// True when the connected driver presents an SQL-style editor surface.
    fn is_sql_style_editor(&self, cx: &App) -> bool {
        self.resolved_editor_mode(cx) == "sql"
    }

    /// Reads the connected driver's `DatabaseCategory`, or `None` when no
    /// connection is attached. Generic — no driver-id branching.
    fn connection_category(&self, cx: &App) -> Option<dbflux_core::DatabaseCategory> {
        let connection_id = self.connection_id?;
        let connected = self.app_state.read(cx).connections().get(&connection_id)?;
        Some(connected.connection.metadata().category)
    }

    fn keyword_candidates(&self) -> &'static [&'static str] {
        match self.query_language {
            dbflux_core::QueryLanguage::Sql
            | dbflux_core::QueryLanguage::OpenSearchSql
            | dbflux_core::QueryLanguage::Cql
            | dbflux_core::QueryLanguage::InfluxQuery => SQL_KEYWORDS,
            dbflux_core::QueryLanguage::CloudWatchLogsInsightsQl => &[
                "fields", "filter", "parse", "stats", "sort", "limit", "display", "dedup",
                "pattern", "diff", "anomaly", "unnest", "unmask", "SOURCE",
            ],
            dbflux_core::QueryLanguage::OpenSearchPpl => &[
                "source", "where", "fields", "stats", "sort", "head", "eval", "parse", "dedup",
                "top", "rare", "join", "flatten", "fillnull", "rename",
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
            dbflux_core::QueryLanguage::Flux => &[
                "from",
                "range",
                "filter",
                "map",
                "group",
                "aggregateWindow",
                "mean",
                "sum",
                "count",
                "last",
                "first",
                "yield",
                "join",
                "pivot",
                "sort",
                "limit",
                "drop",
                "keep",
                "rename",
                "fill",
                "toFloat",
                "toInt",
                "toString",
                "|>",
            ],
            dbflux_core::QueryLanguage::Lua
            | dbflux_core::QueryLanguage::Python
            | dbflux_core::QueryLanguage::Bash
            | dbflux_core::QueryLanguage::Custom(_) => &[],
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

        let is_document_category =
            connected.connection.metadata().category == dbflux_core::DatabaseCategory::Document;

        build_sql_completion_metadata(
            connected.schema.as_ref(),
            connected.database_schemas.values(),
            connected.table_details.values(),
            is_document_category,
        )
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

        let active_keyspace = connected
            .active_database
            .clone()
            .unwrap_or_else(|| "db0".to_string());

        if let Some(keys) = connected.redis_key_cache.get_keys(&active_keyspace) {
            metadata.cached_keys = keys.to_vec();
        }

        metadata
    }

    fn completion_items_for_sql(
        &self,
        source: &str,
        cursor: usize,
        cx: &App,
    ) -> Vec<CompletionItem> {
        let metadata = self.sql_completion_metadata(cx);
        sql_completion_items(&metadata, source, cursor)
    }
}

/// Decides whether the editor should route to SQL-style completion.
///
/// The first clause preserves main's behavior exactly: `Sql`, `Cql`, and
/// `InfluxQuery` always took the SQL path and fold their catalogs. The second
/// clause is the generic DynamoDB case — a Document-category driver whose editor
/// surface is SQL-style (PartiQL) — without any driver-id branching.
///
/// `OpenSearchSql` (CloudWatch's source-context query mode) is deliberately
/// absent: CloudWatch is `DatabaseCategory::LogStream`, not `Document`, so it
/// falls through to the keyword-only path exactly as on main, and its log-group
/// names never fold as SQL table candidates.
fn should_use_sql_completion(
    query_language: &dbflux_core::QueryLanguage,
    is_sql_style_editor: bool,
    category: Option<dbflux_core::DatabaseCategory>,
) -> bool {
    matches!(
        query_language,
        dbflux_core::QueryLanguage::Sql
            | dbflux_core::QueryLanguage::Cql
            | dbflux_core::QueryLanguage::InfluxQuery
    ) || (is_sql_style_editor && category == Some(dbflux_core::DatabaseCategory::Document))
}

/// Builds the SQL completion metadata from a connection's cached schema sources.
///
/// `is_document_category` gates whether document-snapshot collections are folded
/// as SQL table candidates. Only `DatabaseCategory::Document` drivers fold them;
/// other categories (e.g. a LogStream driver exposing an SQL editor over log
/// groups) also build document snapshots, but their collections are not tables.
fn build_sql_completion_metadata<'a>(
    snapshot: Option<&dbflux_core::SchemaSnapshot>,
    database_schemas: impl Iterator<Item = &'a dbflux_core::DbSchemaInfo>,
    table_details: impl Iterator<Item = &'a dbflux_core::TableInfo>,
    is_document_category: bool,
) -> SqlCompletionMetadata {
    let mut metadata = SqlCompletionMetadata::default();

    if let Some(snapshot) = snapshot {
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

        if is_document_category && let Some(document) = snapshot.as_document() {
            for collection in &document.collections {
                metadata.add_collection(collection);
            }
        }
    }

    for schema in database_schemas {
        for table in &schema.tables {
            metadata.add_table(table);
        }

        for view in &schema.views {
            metadata.add_view(view);
        }
    }

    for table in table_details {
        metadata.add_table(table);
    }

    metadata
}

fn sql_completion_items(
    metadata: &SqlCompletionMetadata,
    source: &str,
    cursor: usize,
) -> Vec<CompletionItem> {
    let (prefix_start, prefix) = extract_identifier_prefix(source, cursor);
    let prefix_upper = prefix.to_uppercase();
    let before_cursor = &source[..cursor];
    let replace_range = completion_replace_range(source, prefix_start, cursor);

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
            if !prefix_upper.is_empty() && !column_name.to_uppercase().starts_with(&prefix_upper) {
                continue;
            }

            push_completion_item(
                &mut items,
                &mut seen,
                column_name,
                CompletionItemKind::FIELD,
                &prefix,
                replace_range,
            );
        }

        return items;
    }

    for keyword in SQL_KEYWORDS {
        if !prefix_upper.is_empty() && !keyword.to_uppercase().starts_with(&prefix_upper) {
            continue;
        }

        push_completion_item(
            &mut items,
            &mut seen,
            keyword,
            CompletionItemKind::KEYWORD,
            &prefix,
            replace_range,
        );
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
            &prefix,
            replace_range,
        );
    }

    for view_name in metadata.view_names_iter() {
        if !prefix_upper.is_empty() && !view_name.to_uppercase().starts_with(&prefix_upper) {
            continue;
        }

        if !in_table_context && prefix_upper.is_empty() {
            continue;
        }

        push_completion_item(
            &mut items,
            &mut seen,
            view_name,
            CompletionItemKind::STRUCT,
            &prefix,
            replace_range,
        );
    }

    if !in_table_context {
        for column_name in metadata.all_columns_iter() {
            if !prefix_upper.is_empty() && !column_name.to_uppercase().starts_with(&prefix_upper) {
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
                &prefix,
                replace_range,
            );
        }
    }

    items
}

impl QueryCompletionProvider {
    fn completion_items_for_mongo(
        &self,
        source: &str,
        cursor: usize,
        cx: &App,
    ) -> Vec<CompletionItem> {
        let metadata = self.mongo_completion_metadata(cx);
        let (prefix_start, prefix) = extract_identifier_prefix(source, cursor);
        let prefix_upper = prefix.to_uppercase();
        let replace_range = completion_replace_range(source, prefix_start, cursor);

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
                        &prefix,
                        replace_range,
                    );
                }

                for method in MONGO_DB_METHODS {
                    if !prefix_upper.is_empty() && !method.to_uppercase().starts_with(&prefix_upper)
                    {
                        continue;
                    }

                    push_completion_item(
                        &mut items,
                        &mut seen,
                        method,
                        CompletionItemKind::METHOD,
                        &prefix,
                        replace_range,
                    );
                }
            }
            MongoCompletionContext::Method => {
                for method in MONGO_METHODS {
                    if !prefix_upper.is_empty() && !method.to_uppercase().starts_with(&prefix_upper)
                    {
                        continue;
                    }

                    push_completion_item(
                        &mut items,
                        &mut seen,
                        method,
                        CompletionItemKind::METHOD,
                        &prefix,
                        replace_range,
                    );
                }
            }
            MongoCompletionContext::Field { collection } => {
                let fields = metadata.fields_for_collection(&collection);

                for field in fields {
                    if !prefix_upper.is_empty() && !field.to_uppercase().starts_with(&prefix_upper)
                    {
                        continue;
                    }

                    push_completion_item(
                        &mut items,
                        &mut seen,
                        field,
                        CompletionItemKind::FIELD,
                        &prefix,
                        replace_range,
                    );
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
                            &prefix,
                            replace_range,
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
                        &prefix,
                        replace_range,
                    );
                }
            }
            MongoCompletionContext::General => {}
        }

        for keyword in self.keyword_candidates() {
            if !prefix_upper.is_empty() && !keyword.to_uppercase().starts_with(&prefix_upper) {
                continue;
            }

            push_completion_item(
                &mut items,
                &mut seen,
                keyword,
                CompletionItemKind::KEYWORD,
                &prefix,
                replace_range,
            );
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

        let prefix_start = scan_redis_token_start(source, cursor);
        let prefix_text = &source[prefix_start..cursor];

        let mut seen = HashSet::new();
        let mut items = Vec::new();

        let replace_range = completion_replace_range(source, prefix_start, cursor);

        let command_mode = tokens.is_empty() || (tokens.len() == 1 && !ends_with_space);
        if command_mode {
            let prefix = tokens.first().cloned().unwrap_or_default().to_uppercase();

            for command in REDIS_COMMANDS {
                if !prefix.is_empty() && !command.starts_with(&prefix) {
                    continue;
                }

                push_completion_item(
                    &mut items,
                    &mut seen,
                    command,
                    CompletionItemKind::FUNCTION,
                    prefix_text,
                    replace_range,
                );
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
                push_completion_item(
                    &mut items,
                    &mut seen,
                    &label,
                    CompletionItemKind::VALUE,
                    prefix_text,
                    replace_range,
                );
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

                push_completion_item(
                    &mut items,
                    &mut seen,
                    option,
                    CompletionItemKind::KEYWORD,
                    prefix_text,
                    replace_range,
                );
            }
        }

        if is_redis_key_argument(&command, argument_index) && !metadata.cached_keys.is_empty() {
            let prefix = if ends_with_space {
                String::new()
            } else {
                tokens.last().cloned().unwrap_or_default()
            };

            for key in &metadata.cached_keys {
                if !prefix.is_empty() && !key.starts_with(&prefix) {
                    continue;
                }

                push_completion_item(
                    &mut items,
                    &mut seen,
                    key,
                    CompletionItemKind::VALUE,
                    prefix_text,
                    replace_range,
                );
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

        let use_sql = should_use_sql_completion(
            &self.query_language,
            self.is_sql_style_editor(_cx),
            self.connection_category(_cx),
        );

        let items = if use_sql {
            self.completion_items_for_sql(&source, cursor, _cx)
        } else {
            match self.query_language {
                dbflux_core::QueryLanguage::MongoQuery => {
                    self.completion_items_for_mongo(&source, cursor, _cx)
                }
                dbflux_core::QueryLanguage::RedisCommands => {
                    self.completion_items_for_redis(&source, cursor, _cx)
                }
                _ => {
                    let (prefix_start, prefix) = extract_identifier_prefix(&source, cursor);
                    let prefix_upper = prefix.to_uppercase();
                    let replace_range = completion_replace_range(&source, prefix_start, cursor);
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
                            &prefix,
                            replace_range,
                        );
                    }

                    items
                }
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

        let mut keys = vec![normalize_identifier(&table.name)];
        if let Some(schema) = &table.schema {
            keys.push(normalize_identifier(&format!("{}.{}", schema, table.name)));
        }

        if let Some(columns) = &table.columns {
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

        if let Some(fields) = &table.sample_fields {
            for field in fields {
                for key in &keys {
                    self.add_collection_field(key, field);
                }
            }
        }
    }

    fn add_view(&mut self, view: &dbflux_core::ViewInfo) {
        self.view_names.insert(view.name.clone());

        if let Some(schema) = &view.schema {
            self.view_names.insert(format!("{}.{}", schema, view.name));
        }
    }

    /// Folds a document-store collection into the SQL completion metadata so an
    /// SQL-style editor over a Document-category driver (e.g. DynamoDB PartiQL)
    /// suggests the collection NAME as a table.
    ///
    /// The collection name is the real source of the table-after-`FROM`
    /// completion. The `sample_fields` loop is correct but dormant for drivers
    /// whose schema snapshot leaves `sample_fields` empty (DynamoDB emits
    /// `None` here); for those drivers the per-attribute completion arrives
    /// instead through the lazily-fetched `table_details` `TableInfo`, whose
    /// key-schema `sample_fields` are folded by `add_table`.
    fn add_collection(&mut self, collection: &dbflux_core::CollectionInfo) {
        self.table_names.insert(collection.name.clone());

        let Some(fields) = &collection.sample_fields else {
            return;
        };

        let key = normalize_identifier(&collection.name);

        for field in fields {
            self.add_collection_field(&key, field);
        }
    }

    fn add_collection_field(&mut self, table_key: &str, field: &dbflux_core::FieldInfo) {
        self.all_columns.insert(field.name.clone());

        self.columns_by_table
            .entry(table_key.to_string())
            .or_default()
            .insert(field.name.clone());

        if let Some(nested) = &field.nested_fields {
            for child in nested {
                self.add_collection_field(table_key, child);
            }
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
    cached_keys: Vec<String>,
}

enum MongoCompletionContext {
    Collection,
    Method,
    Field { collection: String },
    Operator,
    General,
}

const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON", "GROUP BY",
    "ORDER BY", "HAVING", "LIMIT", "OFFSET", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
    "CREATE", "ALTER", "DROP", "TRUNCATE", "BEGIN", "COMMIT", "ROLLBACK", "COUNT", "SUM", "AVG",
    "MIN", "MAX", "DISTINCT", "AND", "OR", "NOT", "NULL", "IS", "LIKE", "IN", "BETWEEN", "EXISTS",
    "ASC", "DESC",
];

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

const MONGO_DB_METHODS: &[&str] = &[
    "getName",
    "getCollectionNames",
    "getCollectionInfos",
    "stats",
    "serverStatus",
    "createCollection",
    "dropDatabase",
    "runCommand",
    "adminCommand",
    "version",
    "hostInfo",
    "currentOp",
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

fn scan_redis_token_start(source: &str, end: usize) -> usize {
    let bytes = source.as_bytes();
    let mut start = end;

    while start > 0 {
        let idx = start - 1;
        if bytes[idx].is_ascii_whitespace() {
            break;
        }

        start -= 1;
    }

    start
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

/// Returns true when `argument_index` is a key-name position for the given Redis command.
fn is_redis_key_argument(command: &str, argument_index: usize) -> bool {
    match command {
        // Single-key commands: key is always the first argument
        "GET" | "SET" | "DEL" | "EXISTS" | "EXPIRE" | "TTL" | "TYPE" | "INCR" | "DECR" | "HGET"
        | "HSET" | "HDEL" | "HGETALL" | "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "LRANGE" | "SADD"
        | "SREM" | "SMEMBERS" | "ZADD" | "ZREM" | "ZRANGE" | "PERSIST" | "PTTL" | "DUMP"
        | "OBJECT" | "RENAME" | "SETNX" | "GETSET" | "APPEND" | "GETRANGE" | "SETRANGE"
        | "STRLEN" | "LLEN" | "LINDEX" | "LSET" | "SCARD" | "SISMEMBER" | "ZCARD" | "ZSCORE"
        | "ZRANK" => argument_index == 0,

        // MGET: every argument is a key
        "MGET" => true,

        // MSET: alternating key/value pairs — only even indices are keys
        "MSET" => argument_index.is_multiple_of(2),

        _ => false,
    }
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

#[cfg(test)]
mod tests {
    use super::{
        CompletionItem, SqlCompletionMetadata, build_sql_completion_metadata,
        should_use_sql_completion, sql_completion_items,
    };
    use crate::completion_support::normalize_identifier;
    use dbflux_core::{
        CollectionInfo, ColumnInfo, DatabaseCategory, DatabaseInfo, DbSchemaInfo, DocumentSchema,
        FieldInfo, QueryLanguage, SchemaSnapshot, TableInfo,
    };

    fn column(name: &str, type_name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            type_name: type_name.to_string(),
            nullable: true,
            is_primary_key: false,
            default_value: None,
            enum_values: None,
        }
    }

    fn field(name: &str) -> FieldInfo {
        FieldInfo {
            name: name.to_string(),
            common_type: "S".to_string(),
            occurrence_rate: None,
            nested_fields: None,
        }
    }

    /// Mirrors the collection DynamoDB's `schema()` actually emits: a named
    /// collection with `sample_fields: None`. Per-attribute data is NOT carried
    /// here; it arrives via the lazily-fetched `table_details` `TableInfo`.
    fn dynamo_schema_collection() -> CollectionInfo {
        CollectionInfo {
            name: "Orders".to_string(),
            database: Some("default".to_string()),
            document_count: None,
            avg_document_size: None,
            sample_fields: None,
            indexes: None,
            validator: None,
            is_capped: false,
            presentation: dbflux_core::CollectionPresentation::default(),
            child_items: None,
        }
    }

    fn dynamo_document_snapshot() -> SchemaSnapshot {
        SchemaSnapshot::document(DocumentSchema {
            databases: vec![DatabaseInfo {
                name: "default".to_string(),
                is_current: true,
            }],
            current_database: Some("default".to_string()),
            collections: vec![dynamo_schema_collection()],
        })
    }

    /// Mirrors the `TableInfo` DynamoDB's `table_details()` builds: `columns:
    /// None` with the key-schema attributes carried in `sample_fields`.
    fn dynamo_table_details() -> TableInfo {
        TableInfo {
            name: "Orders".to_string(),
            schema: Some("dynamodb".to_string()),
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: Some(vec![field("pk"), field("sk")]),
            presentation: dbflux_core::CollectionPresentation::default(),
            child_items: None,
        }
    }

    fn labels(items: &[CompletionItem]) -> Vec<String> {
        items.iter().map(|item| item.label.clone()).collect()
    }

    #[test]
    fn dynamo_document_completion_offers_table_after_from() {
        // Document-category: the collection NAME folds as a table candidate
        // even though its `sample_fields` are `None`.
        let snapshot = dynamo_document_snapshot();
        let metadata = build_sql_completion_metadata(
            Some(&snapshot),
            std::iter::empty::<&DbSchemaInfo>(),
            std::iter::empty::<&TableInfo>(),
            true,
        );

        let source = "SELECT * FROM ";
        let items = sql_completion_items(&metadata, source, source.len());

        assert!(
            labels(&items).contains(&"Orders".to_string()),
            "table name should be suggested in FROM position via as_document"
        );
    }

    #[test]
    fn dynamo_key_schema_attributes_complete_in_where_position() {
        // Real DynamoDB path: the document snapshot collection has no
        // `sample_fields`; the WHERE-position attributes (pk/sk) come from the
        // lazily-fetched `table_details` `TableInfo` folded by `add_table`.
        let snapshot = dynamo_document_snapshot();
        let table_details = [dynamo_table_details()];
        let metadata = build_sql_completion_metadata(
            Some(&snapshot),
            std::iter::empty::<&DbSchemaInfo>(),
            table_details.iter(),
            true,
        );

        let qualified_source = "SELECT * FROM Orders o WHERE o.p";
        let qualified_items =
            sql_completion_items(&metadata, qualified_source, qualified_source.len());
        assert!(
            labels(&qualified_items).contains(&"pk".to_string()),
            "qualified key-schema attribute should be suggested after the alias"
        );

        let bare_source = "SELECT * FROM Orders WHERE s";
        let bare_items = sql_completion_items(&metadata, bare_source, bare_source.len());
        assert!(
            labels(&bare_items).contains(&"sk".to_string()),
            "unqualified key-schema attribute should be suggested in WHERE with a prefix"
        );
    }

    #[test]
    fn log_stream_document_snapshot_does_not_fold_collections_as_tables() {
        // A LogStream-category driver (CloudWatch-shaped) also builds a document
        // snapshot, but its collections are log groups, not SQL tables. With
        // `is_document_category` false they must NOT fold as table candidates.
        let snapshot = SchemaSnapshot::document(DocumentSchema {
            databases: vec![DatabaseInfo {
                name: "default".to_string(),
                is_current: true,
            }],
            current_database: Some("default".to_string()),
            collections: vec![CollectionInfo {
                name: "/aws/lambda/my-fn".to_string(),
                database: Some("default".to_string()),
                document_count: None,
                avg_document_size: None,
                sample_fields: None,
                indexes: None,
                validator: None,
                is_capped: false,
                presentation: dbflux_core::CollectionPresentation::default(),
                child_items: None,
            }],
        });

        let metadata = build_sql_completion_metadata(
            Some(&snapshot),
            std::iter::empty::<&DbSchemaInfo>(),
            std::iter::empty::<&TableInfo>(),
            false,
        );

        let tables: Vec<&str> = metadata.table_names_iter().collect();
        assert!(
            tables.is_empty(),
            "log-group names must not fold as SQL table candidates"
        );

        let source = "SELECT * FROM ";
        let items = sql_completion_items(&metadata, source, source.len());
        assert!(
            !labels(&items).contains(&"/aws/lambda/my-fn".to_string()),
            "log-group name must not be suggested as a table in FROM position"
        );
    }

    #[test]
    fn dynamo_document_completion_offers_partiql_keywords() {
        let metadata = SqlCompletionMetadata::default();

        let source = "SELE";
        let items = sql_completion_items(&metadata, source, source.len());
        assert!(labels(&items).contains(&"SELECT".to_string()));

        let where_source = "SELECT * FROM Orders WHE";
        let where_items = sql_completion_items(&metadata, where_source, where_source.len());
        assert!(labels(&where_items).contains(&"WHERE".to_string()));
    }

    #[test]
    fn relational_table_completion_unchanged_by_document_support() {
        let table = TableInfo {
            name: "users".to_string(),
            schema: None,
            columns: Some(vec![column("id", "integer"), column("email", "text")]),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::default(),
            child_items: None,
        };

        let mut metadata = SqlCompletionMetadata::default();
        metadata.add_table(&table);

        let tables: Vec<&str> = metadata.table_names_iter().collect();
        assert_eq!(tables, vec!["users"]);

        let columns = metadata.columns_for_table(&normalize_identifier("users"));
        assert!(columns.contains(&"id"));
        assert!(columns.contains(&"email"));

        let from_source = "SELECT * FROM ";
        let items = sql_completion_items(&metadata, from_source, from_source.len());
        assert!(labels(&items).contains(&"users".to_string()));
    }

    #[test]
    fn sql_completion_routing_preserves_main_and_adds_dynamodb() {
        // Languages that took the SQL path on main: always SQL, any category.
        assert!(should_use_sql_completion(
            &QueryLanguage::Sql,
            true,
            Some(DatabaseCategory::Relational),
        ));
        assert!(should_use_sql_completion(
            &QueryLanguage::InfluxQuery,
            true,
            Some(DatabaseCategory::TimeSeries),
        ));
        assert!(should_use_sql_completion(&QueryLanguage::Cql, false, None));

        // New generic case: Document-category driver with an SQL-style editor
        // (DynamoDB PartiQL).
        assert!(should_use_sql_completion(
            &QueryLanguage::Custom("DynamoDB".to_string()),
            true,
            Some(DatabaseCategory::Document),
        ));

        // Regression guard: CloudWatch's OpenSearchSql source-context mode is
        // SQL-style but LogStream-category — must NOT route to the SQL catalog.
        assert!(!should_use_sql_completion(
            &QueryLanguage::OpenSearchSql,
            true,
            Some(DatabaseCategory::LogStream),
        ));

        // A document driver without an SQL-style editor stays off the SQL path.
        assert!(!should_use_sql_completion(
            &QueryLanguage::MongoQuery,
            false,
            Some(DatabaseCategory::Document),
        ));

        // DynamoDB without an SQL-style editor surface does not route to SQL.
        assert!(!should_use_sql_completion(
            &QueryLanguage::Custom("DynamoDB".to_string()),
            false,
            Some(DatabaseCategory::Document),
        ));
    }

    #[test]
    fn dynamo_editor_mode_resolves_to_sql_via_profile() {
        let mongo_mode =
            dbflux_core::EditorLanguageProfile::from_language(&QueryLanguage::MongoQuery)
                .editor_mode;
        assert_ne!(mongo_mode, "sql");

        let sql_mode =
            dbflux_core::EditorLanguageProfile::from_language(&QueryLanguage::Sql).editor_mode;
        assert_eq!(sql_mode, "sql");
    }
}
