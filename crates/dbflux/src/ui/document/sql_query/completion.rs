use super::*;

pub(super) struct QueryCompletionProvider {
    query_language: dbflux_core::QueryLanguage,
    app_state: Entity<AppState>,
    connection_id: Option<Uuid>,
}

impl QueryCompletionProvider {
    pub(super) fn new(
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

        if let Some(snapshot) = &connected.schema
            && let Some(relational) = snapshot.as_relational()
        {
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

                push_completion_item(&mut items, &mut seen, key, CompletionItemKind::VALUE);
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
    cached_keys: Vec<String>,
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
