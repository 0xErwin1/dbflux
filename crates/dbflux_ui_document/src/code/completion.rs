use super::*;
use crate::completion_support::{
    byte_offset_to_lsp_position, completion_replace_range, extract_identifier_prefix,
    is_identifier_byte, normalize_identifier, push_completion_item, push_completion_item_ranked,
    scan_identifier_start,
};
use dbflux_core::{SqlCompletionContext, SqlContextEngine, SqlCursorAnalysis};
use std::cell::{Cell, RefCell};

/// Rank groups for context-aware SQL completion (`sort_text` prefix): the
/// items the context asks for come first, keywords last.
const RANK_PRIMARY: u8 = 0;
const RANK_SECONDARY: u8 = 1;
const RANK_KEYWORD: u8 = 2;

/// `(database, schema, table)` fetch key for a table-details prefetch.
type PrefetchKey = (String, Option<String>, String);

pub(super) struct QueryCompletionProvider {
    query_language: dbflux_core::QueryLanguage,
    app_state: Entity<AppStateEntity>,
    connection_id: Option<Uuid>,
    /// The document's selected database (document-local; the document
    /// reattaches the provider when it changes).
    database: Option<String>,
    /// Fetch keys with an in-flight or completed table-details prefetch. A
    /// key is added once the fetch is actually spawned and removed if the
    /// fetch fails, so a transient error does not block retry for the session.
    /// Shared with the spawned task so it can release the key on failure.
    prefetched_tables: Rc<RefCell<HashSet<PrefetchKey>>>,
    /// Databases with an in-flight or completed schema-listing prefetch, same
    /// add-on-spawn / remove-on-failure discipline as `prefetched_tables`.
    prefetched_databases: Rc<RefCell<HashSet<String>>>,
    /// `None` when the grammar failed to load; completion then stays on the
    /// heuristic path.
    sql_context: Option<SqlContextEngine>,
    /// Bumped per `completions()` call; the editor's Change handler uses it
    /// to detect deletions the menu plumbing ignored.
    completion_query_generation: Rc<Cell<u64>>,
}

impl QueryCompletionProvider {
    pub(super) fn new(
        query_language: dbflux_core::QueryLanguage,
        app_state: Entity<AppStateEntity>,
        connection_id: Option<Uuid>,
        database: Option<String>,
        completion_query_generation: Rc<Cell<u64>>,
    ) -> Self {
        Self {
            query_language,
            app_state,
            connection_id,
            database,
            prefetched_tables: Rc::new(RefCell::new(HashSet::new())),
            prefetched_databases: Rc::new(RefCell::new(HashSet::new())),
            sql_context: SqlContextEngine::new(),
            completion_query_generation,
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

    /// The database completion scopes to: the document's selection, then the
    /// connection's active database, then the snapshot's current database.
    fn effective_database(&self, connected: &dbflux_core::ConnectedProfile) -> Option<String> {
        self.database
            .clone()
            .or_else(|| connected.active_database.clone())
            .or_else(|| {
                connected
                    .schema
                    .as_ref()
                    .and_then(|snapshot| snapshot.current_database().map(String::from))
            })
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

        // Scope every source to the selected database; tables cached for
        // other databases must not leak into this tab's suggestions. Without
        // a selection, lazy-per-database drivers offer nothing rather than
        // everything, while snapshot-based drivers keep their single catalog.
        let effective_database = self.effective_database(connected);
        let lazy_per_database = connected.connection.schema_loading_strategy()
            == dbflux_core::SchemaLoadingStrategy::LazyPerDatabase;
        let database_in_scope = |database: &str| {
            effective_database
                .as_deref()
                .map_or(!lazy_per_database, |selected| selected == database)
        };

        let snapshot = connected.schema.as_ref().filter(|snapshot| {
            match (effective_database.as_deref(), snapshot.current_database()) {
                (Some(selected), Some(current)) => selected == current,
                _ => true,
            }
        });

        build_sql_completion_metadata(
            snapshot,
            connected
                .database_schemas
                .iter()
                .filter(|(database, _)| database_in_scope(database))
                .map(|(_, schema)| schema),
            connected
                .table_details
                .iter()
                .filter(|((database, _, _), _)| database_in_scope(database))
                .map(|(_, details)| details),
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
        let analysis = self
            .sql_context
            .as_ref()
            .and_then(|engine| engine.analyze(source, cursor));

        sql_completion_items_with_context(&metadata, source, cursor, analysis.as_ref())
    }

    /// Background-fetches column details for tables referenced by the statement
    /// under the cursor, so `table.` / `alias.` completion has columns without
    /// requiring the user to expand the table in the sidebar first.
    ///
    /// Results land in the connection's shared `table_details` cache via
    /// `set_table_details`, where the next completion request picks them up.
    /// Failures are logged only (autocomplete is not a user-facing operation).
    fn prefetch_sql_table_details(
        &self,
        source: &str,
        cursor: usize,
        cx: &mut Context<InputState>,
    ) {
        let Some(connection_id) = self.connection_id else {
            return;
        };

        self.prefetch_database_schema(connection_id, cx);

        let statement_range = dbflux_core::QueryLanguage::Sql.statement_bounds_at(source, cursor);
        let referenced = dbflux_core::extract_referenced_tables(&source[statement_range]);
        if referenced.is_empty() {
            return;
        }

        let keys = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&connection_id) else {
                return;
            };

            let effective_database = self.effective_database(connected);
            let lazy_per_database = connected.connection.schema_loading_strategy()
                == dbflux_core::SchemaLoadingStrategy::LazyPerDatabase;

            // On lazy-per-database drivers a fabricated fallback name would
            // reach real queries (`` `default`.`table` ``); single-catalog
            // drivers ignore the database argument, so a placeholder key is
            // safe there.
            let snapshot_database = effective_database
                .as_deref()
                .or((!lazy_per_database).then_some("default"));

            let known = known_relational_tables(
                connected.schema.as_ref(),
                connected.database_schemas.iter().filter(|(database, _)| {
                    // Same scope as the metadata builder: without a selection,
                    // lazy-per-database drivers contribute nothing (otherwise
                    // we would fetch details the menu then discards).
                    effective_database
                        .as_deref()
                        .map_or(!lazy_per_database, |selected| selected == database.as_str())
                }),
                snapshot_database,
            );

            tables_needing_details(&known, &referenced)
        };

        for key in keys {
            if self.prefetched_tables.borrow().contains(&key) {
                continue;
            }

            let params = match self.app_state.read(cx).prepare_fetch_table_details(
                connection_id,
                &key.0,
                key.1.as_deref(),
                &key.2,
            ) {
                Ok(params) => params,
                // Already cached, wrong strategy, or disconnected: nothing to
                // fetch. The key is not marked, so a later keystroke re-checks.
                Err(_) => continue,
            };

            self.prefetched_tables.borrow_mut().insert(key.clone());

            let app_state = self.app_state.clone();
            let prefetched_tables = self.prefetched_tables.clone();
            let task = cx
                .background_executor()
                .spawn(async move { params.execute() });

            cx.spawn(async move |_this, cx| match task.await {
                Ok(result) => {
                    cx.update(|cx| {
                        app_state.update(cx, |state, _| {
                            state.set_table_details(
                                result.profile_id,
                                result.database.clone(),
                                result.schema.clone(),
                                result.table.clone(),
                                result.details,
                            );
                            state.set_dependents(
                                result.profile_id,
                                result.database,
                                result.schema,
                                result.table,
                                result.dependents,
                            );
                        });
                    })
                    .ok();
                }
                Err(err) => {
                    // Release the key so a later keystroke can retry.
                    prefetched_tables.borrow_mut().remove(&key);
                    log::warn!(
                        "autocomplete: failed to prefetch table details for {}.{}: {}",
                        key.0,
                        key.2,
                        err
                    );
                }
            })
            .detach();
        }
    }

    /// Background-fetches the table listing for the editor's selected
    /// database on lazy-per-database drivers, which is otherwise only
    /// populated when the user expands the database in the sidebar.
    fn prefetch_database_schema(&self, connection_id: Uuid, cx: &mut Context<InputState>) {
        let Some(database) = ({
            let state = self.app_state.read(cx);
            state
                .connections()
                .get(&connection_id)
                .and_then(|connected| self.effective_database(connected))
        }) else {
            return;
        };

        if self.prefetched_databases.borrow().contains(&database) {
            return;
        }

        let params = match self
            .app_state
            .read(cx)
            .prepare_fetch_database_schema(connection_id, &database)
        {
            Ok(params) => params,
            // Wrong loading strategy, already cached, or disconnected: nothing
            // to fetch. The key is not marked, so a later keystroke re-checks.
            Err(_) => return,
        };

        self.prefetched_databases
            .borrow_mut()
            .insert(database.clone());

        let app_state = self.app_state.clone();
        let prefetched_databases = self.prefetched_databases.clone();
        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        cx.spawn(async move |_this, cx| match task.await {
            Ok(result) => {
                cx.update(|cx| {
                    app_state.update(cx, |state, _| {
                        state.set_database_schema(
                            result.profile_id,
                            result.database,
                            result.schema,
                        );
                    });
                })
                .ok();
            }
            Err(err) => {
                // Release the key so a later keystroke can retry.
                prefetched_databases.borrow_mut().remove(&database);
                log::warn!(
                    "autocomplete: failed to prefetch schema for database {}: {}",
                    database,
                    err
                );
            }
        })
        .detach();
    }
}

/// A table from a connection's cached schema listings, tagged with its
/// `table_details` cache key.
struct KnownTableListing {
    database: String,
    schema: Option<String>,
    name: String,
    has_columns: bool,
}

/// Flattens a connection's relational schema listings into [`KnownTableListing`]s.
///
/// Mirrors the sources of [`build_sql_completion_metadata`]: the connect-time
/// snapshot (top-level tables plus per-schema tables) and the lazily cached
/// per-database schemas. Snapshot tables are tagged with `snapshot_database`
/// as their fetch key; without one they are skipped, since there is no valid
/// key to fetch them under.
fn known_relational_tables<'a>(
    snapshot: Option<&dbflux_core::SchemaSnapshot>,
    database_schemas: impl Iterator<Item = (&'a String, &'a dbflux_core::DbSchemaInfo)>,
    snapshot_database: Option<&str>,
) -> Vec<KnownTableListing> {
    let mut tables = Vec::new();

    if let Some(database) = snapshot_database
        && let Some(snapshot) = snapshot
        && let Some(relational) = snapshot.as_relational()
    {
        let per_schema = relational.schemas.iter().flat_map(|schema| &schema.tables);
        for table in relational.tables.iter().chain(per_schema) {
            tables.push(KnownTableListing {
                database: database.to_string(),
                schema: table.schema.clone(),
                name: table.name.clone(),
                has_columns: table.columns.is_some(),
            });
        }
    }

    for (database, schema_info) in database_schemas {
        for table in &schema_info.tables {
            tables.push(KnownTableListing {
                database: database.clone(),
                schema: table.schema.clone(),
                name: table.name.clone(),
                has_columns: table.columns.is_some(),
            });
        }
    }

    tables
}

/// `(database, schema, table)` fetch keys for the referenced tables that are
/// known but still lack column details.
///
/// A schema-qualified reference must also match the listing's schema or
/// database (dialects with database-level namespaces parse `db.table` as a
/// schema qualifier); an unqualified reference matches every same-named
/// listing (fetching all candidates is bounded by the known-table list and
/// keeps completion working regardless of search-path semantics).
fn tables_needing_details(
    known: &[KnownTableListing],
    referenced: &[dbflux_core::QueryTableRef],
) -> Vec<PrefetchKey> {
    let mut keys = Vec::new();

    for table_ref in referenced {
        let name = normalize_identifier(&table_ref.table);

        for table in known {
            if table.has_columns || normalize_identifier(&table.name) != name {
                continue;
            }

            if let Some(qualifier) = &table_ref.schema {
                let qualifier = normalize_identifier(qualifier);
                let schema_matches = table
                    .schema
                    .as_deref()
                    .is_some_and(|schema| normalize_identifier(schema) == qualifier);
                let database_matches = normalize_identifier(&table.database) == qualifier;

                if !schema_matches && !database_matches {
                    continue;
                }
            }

            if let Some(database) = &table_ref.database
                && normalize_identifier(&table.database) != normalize_identifier(database)
            {
                continue;
            }

            keys.push((
                table.database.clone(),
                table.schema.clone(),
                table.name.clone(),
            ));
        }
    }

    keys.sort();
    keys.dedup();
    keys
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

/// Push-helper for the SQL item builders: applies the typed-prefix filter and
/// an optional rank group to whole candidate sets.
struct SqlItemSink<'a> {
    items: Vec<CompletionItem>,
    seen: HashSet<String>,
    prefix: &'a str,
    prefix_upper: String,
    replace_range: lsp_types::Range,
}

impl SqlItemSink<'_> {
    fn push_all<'c>(
        &mut self,
        candidates: impl IntoIterator<Item = &'c str>,
        kind: CompletionItemKind,
        rank: Option<u8>,
    ) {
        for candidate in candidates {
            if !self.prefix_upper.is_empty()
                && !candidate.to_uppercase().starts_with(&self.prefix_upper)
            {
                continue;
            }

            match rank {
                Some(rank_group) => push_completion_item_ranked(
                    &mut self.items,
                    &mut self.seen,
                    candidate,
                    kind,
                    self.prefix,
                    self.replace_range,
                    rank_group,
                ),
                None => push_completion_item(
                    &mut self.items,
                    &mut self.seen,
                    candidate,
                    kind,
                    self.prefix,
                    self.replace_range,
                ),
            }
        }
    }

    fn has_prefix(&self) -> bool {
        !self.prefix_upper.is_empty()
    }

    /// Whitespace-triggered menus (empty prefix) only open when the position
    /// has context-relevant items; a keyword-only popup after every space
    /// would be noise. Returns true when the sink should stop here.
    fn stop_before_keywords(&self) -> bool {
        !self.has_prefix() && self.items.is_empty()
    }

    /// Items for a table position (after FROM/JOIN/...): CTEs, tables, views,
    /// then keywords.
    fn into_table_ref_items(
        mut self,
        metadata: &SqlCompletionMetadata,
        scope: Option<&dbflux_core::StatementScope>,
    ) -> Vec<CompletionItem> {
        if let Some(scope) = scope {
            self.push_all(
                scope.cte_names.iter().map(String::as_str),
                CompletionItemKind::STRUCT,
                Some(RANK_PRIMARY),
            );
        }

        self.push_all(
            metadata.table_names_iter(),
            CompletionItemKind::STRUCT,
            Some(RANK_PRIMARY),
        );
        self.push_all(
            metadata.view_names_iter(),
            CompletionItemKind::STRUCT,
            Some(RANK_PRIMARY),
        );

        if self.stop_before_keywords() {
            return self.items;
        }

        self.push_all(
            SQL_KEYWORDS.iter().copied(),
            CompletionItemKind::KEYWORD,
            Some(RANK_KEYWORD),
        );
        self.items
    }

    /// Items for a column position: scoped columns (with the referenced-table
    /// fallback for broken parses), clause-valid SELECT aliases, relation
    /// aliases, then keywords.
    fn into_column_ref_items(
        mut self,
        metadata: &SqlCompletionMetadata,
        scope: Option<&dbflux_core::StatementScope>,
        clause: dbflux_core::SqlClause,
        source: &str,
        cursor: usize,
    ) -> Vec<CompletionItem> {
        let mut columns = scope.map_or_else(Vec::new, |scope| scoped_columns(metadata, scope));

        // Broken syntax can hide the FROM clause from the parser, leaving the
        // scope without relations; recover the columns from the referenced
        // tables so a valid column position still fills.
        if columns.is_empty() {
            columns = columns_from_referenced_tables(metadata, source, cursor);
        }

        // Output aliases (`SELECT c1 * 2 AS a1`) are valid completion targets
        // where engines resolve them: GROUP BY, ORDER BY, HAVING. Not in WHERE.
        if matches!(
            clause,
            dbflux_core::SqlClause::GroupBy
                | dbflux_core::SqlClause::OrderBy
                | dbflux_core::SqlClause::Having
        ) && let Some(scope) = scope
        {
            self.push_all(
                scope.select_aliases.iter().map(String::as_str),
                CompletionItemKind::FIELD,
                Some(RANK_PRIMARY),
            );
        }

        if columns.is_empty() {
            // Scope unresolved (columns not fetched yet, derived tables), so
            // keep the previous behavior of every known column behind a
            // non-empty prefix.
            if self.has_prefix() {
                self.push_all(
                    metadata.all_columns_iter(),
                    CompletionItemKind::FIELD,
                    Some(RANK_PRIMARY),
                );
            }
        } else {
            // The context is confident, so scoped columns surface even with an
            // empty prefix (`WHERE `).
            self.push_all(columns, CompletionItemKind::FIELD, Some(RANK_PRIMARY));
        }

        if let Some(scope) = scope {
            self.push_all(
                scope
                    .relations
                    .iter()
                    .filter_map(|relation| relation.alias.as_deref()),
                CompletionItemKind::VARIABLE,
                Some(RANK_SECONDARY),
            );
        }

        if self.stop_before_keywords() {
            return self.items;
        }

        self.push_all(
            SQL_KEYWORDS.iter().copied(),
            CompletionItemKind::KEYWORD,
            Some(RANK_KEYWORD),
        );
        self.items
    }

    /// Heuristic fallback when the cursor position could not be classified:
    /// keywords and (in a table position) tables/views, all prefix-gated so a
    /// whitespace trigger does not pop a menu.
    fn into_heuristic_items(
        mut self,
        metadata: &SqlCompletionMetadata,
        before_cursor: &str,
    ) -> Vec<CompletionItem> {
        if self.has_prefix() {
            self.push_all(
                SQL_KEYWORDS.iter().copied(),
                CompletionItemKind::KEYWORD,
                None,
            );
        }

        let in_table_context = is_sql_table_context(before_cursor);
        if in_table_context || self.has_prefix() {
            self.push_all(
                metadata.table_names_iter(),
                CompletionItemKind::STRUCT,
                None,
            );
            self.push_all(metadata.view_names_iter(), CompletionItemKind::STRUCT, None);
        }

        if !in_table_context && self.has_prefix() {
            self.push_all(metadata.all_columns_iter(), CompletionItemKind::FIELD, None);
        }

        self.items
    }
}

fn sql_completion_items_with_context(
    metadata: &SqlCompletionMetadata,
    source: &str,
    cursor: usize,
    analysis: Option<&SqlCursorAnalysis>,
) -> Vec<CompletionItem> {
    let (prefix_start, prefix) = extract_identifier_prefix(source, cursor);
    let before_cursor = &source[..cursor];

    let mut sink = SqlItemSink {
        items: Vec::new(),
        seen: HashSet::new(),
        prefix: &prefix,
        prefix_upper: prefix.to_uppercase(),
        replace_range: completion_replace_range(source, prefix_start, cursor),
    };

    let has_dot_before_prefix =
        prefix_start > 0 && source.as_bytes().get(prefix_start - 1) == Some(&b'.');

    if has_dot_before_prefix {
        let qualifier_end = prefix_start - 1;
        let qualifier_start = scan_identifier_start(source, qualifier_end);
        let qualifier = &source[qualifier_start..qualifier_end];
        let resolved_qualifier = resolve_qualifier(qualifier, analysis, source, cursor);
        let bare = resolved_qualifier
            .rsplit_once('.')
            .map_or(resolved_qualifier.as_str(), |(_, bare)| bare);

        let qualifier_columns = metadata.columns_for_table_or_bare(&resolved_qualifier, bare);
        sink.push_all(qualifier_columns, CompletionItemKind::FIELD, None);
        return sink.items;
    }

    let scope = analysis.map(|analysis| &analysis.scope);
    match analysis.and_then(|analysis| analysis.context) {
        Some(SqlCompletionContext::TableRef) => sink.into_table_ref_items(metadata, scope),
        Some(SqlCompletionContext::ColumnRef { clause }) => {
            sink.into_column_ref_items(metadata, scope, clause, source, cursor)
        }
        None => sink.into_heuristic_items(metadata, before_cursor),
    }
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
        self.completion_query_generation
            .set(self.completion_query_generation.get() + 1);

        let source = text.to_string();
        let cursor = source.floor_char_boundary(offset);

        let use_sql = should_use_sql_completion(
            &self.query_language,
            self.is_sql_style_editor(_cx),
            self.connection_category(_cx),
        );

        let items = if use_sql {
            self.prefetch_sql_table_details(&source, cursor, _cx);
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
        cx: &mut Context<InputState>,
    ) -> bool {
        // Deletions arrive as an empty replacement. Letting them through
        // makes an open menu re-query at the new cursor position (refresh,
        // or hide via an empty item list) instead of going stale next to the
        // removed context.
        if new_text.is_empty() {
            return self.is_sql_style_editor(cx);
        }

        if new_text.len() != 1 {
            return false;
        }

        let ch = new_text.as_bytes()[0] as char;
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' || ch == '$' {
            return true;
        }

        // On SQL-style editors whitespace also triggers, so the menu opens
        // right after clause keywords (`FROM `, `WHERE `) without typing a
        // prefix. The item builders return an empty list for positions with
        // nothing context-relevant, which keeps the menu closed there.
        (ch == ' ' || ch == '\n') && self.is_sql_style_editor(cx)
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

    /// Columns for `key`, falling back to the bare table name when the
    /// qualified key misses. A cached `TableInfo` may carry no schema, so a
    /// `schema.table` key does not hit; the bare name still does.
    fn columns_for_table_or_bare(&self, key: &str, bare: &str) -> Vec<&str> {
        let columns = self.columns_for_table(key);
        if columns.is_empty() && key != bare {
            return self.columns_for_table(bare);
        }
        columns
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

/// Resolves a dot-qualifier (`a.` / `t.` / `s.t.`) to the normalized
/// `columns_by_table` key.
///
/// The tree-sitter scope resolves first. It is subquery-aware, so an inner
/// alias shadows an outer one. The token-based alias walk stays as fallback
/// for buffers the grammar mangles (e.g. a SELECT list typed before its FROM
/// clause exists).
fn resolve_qualifier(
    qualifier: &str,
    analysis: Option<&SqlCursorAnalysis>,
    source: &str,
    cursor: usize,
) -> String {
    let normalized = normalize_identifier(qualifier);

    if let Some(analysis) = analysis {
        for relation in &analysis.scope.relations {
            let alias_matches = relation
                .alias
                .as_deref()
                .is_some_and(|alias| normalize_identifier(alias) == normalized);

            if alias_matches {
                return relation_metadata_key(relation);
            }
        }
    }

    let statement_range = dbflux_core::QueryLanguage::Sql.statement_bounds_at(source, cursor);
    let aliases = extract_sql_aliases(&source[statement_range]);
    aliases.get(&normalized).cloned().unwrap_or(normalized)
}

/// The columns of every relation in scope, innermost query level first.
fn scoped_columns<'a>(
    metadata: &'a SqlCompletionMetadata,
    scope: &dbflux_core::StatementScope,
) -> Vec<&'a str> {
    let mut columns = Vec::new();

    for relation in &scope.relations {
        columns.extend(metadata.columns_for_table_or_bare(
            &relation_metadata_key(relation),
            &normalize_identifier(&relation.table),
        ));
    }

    columns
}

/// The normalized `columns_by_table` key for a scope relation:
/// schema-qualified when the reference was, bare otherwise (`add_table`
/// indexes each table under both forms).
fn relation_metadata_key(relation: &dbflux_core::ScopeRelation) -> String {
    match &relation.schema {
        Some(schema) => normalize_identifier(&format!("{}.{}", schema, relation.table)),
        None => normalize_identifier(&relation.table),
    }
}

/// Columns of the tables referenced by the cursor's statement, resolved via
/// the keyword-based table extractor.
///
/// Fallback for when the tree-sitter scope is empty because half-typed syntax
/// broke the parse (e.g. a trailing comma in a SELECT list swallows the FROM
/// clause). The extractor is grammar-tolerant and still finds the tables.
fn columns_from_referenced_tables<'a>(
    metadata: &'a SqlCompletionMetadata,
    source: &str,
    cursor: usize,
) -> Vec<&'a str> {
    let statement_range = dbflux_core::QueryLanguage::Sql.statement_bounds_at(source, cursor);
    let mut columns = Vec::new();

    for table_ref in dbflux_core::extract_referenced_tables(&source[statement_range]) {
        let key = match &table_ref.schema {
            Some(schema) => normalize_identifier(&format!("{}.{}", schema, table_ref.table)),
            None => normalize_identifier(&table_ref.table),
        };

        columns.extend(
            metadata.columns_for_table_or_bare(&key, &normalize_identifier(&table_ref.table)),
        );
    }

    columns
}

fn extract_sql_aliases(statement: &str) -> HashMap<String, String> {
    let tokens = tokenize_sql_identifiers(statement);
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
        CompletionItem, KnownTableListing, SqlCompletionMetadata, build_sql_completion_metadata,
        known_relational_tables, should_use_sql_completion, sql_completion_items_with_context,
        tables_needing_details,
    };
    use crate::completion_support::normalize_identifier;
    use dbflux_core::{
        CollectionInfo, ColumnInfo, DatabaseCategory, DatabaseInfo, DbSchemaInfo, DocumentSchema,
        FieldInfo, QueryLanguage, QueryTableRef, RelationalSchema, SchemaSnapshot, TableInfo,
    };

    /// The heuristic-only path (no cursor analysis): the exact pre-context
    /// behavior every legacy test below asserts.
    fn sql_completion_items(
        metadata: &SqlCompletionMetadata,
        source: &str,
        cursor: usize,
    ) -> Vec<CompletionItem> {
        sql_completion_items_with_context(metadata, source, cursor, None)
    }

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
            storage_hints: None,
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
            storage_hints: None,
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

    fn t1_table() -> TableInfo {
        TableInfo {
            name: "t1".to_string(),
            schema: None,
            columns: Some(vec![
                column("c1", "text"),
                column("c2", "timestamp"),
                column("c3", "numeric"),
            ]),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::default(),
            child_items: None,
            storage_hints: None,
        }
    }

    #[test]
    fn alias_resolves_when_from_clause_is_after_cursor() {
        let mut metadata = SqlCompletionMetadata::default();
        metadata.add_table(&t1_table());

        let source = "SELECT a. FROM t1 a";
        let cursor = "SELECT a.".len();
        let items = sql_completion_items(&metadata, source, cursor);
        assert!(
            labels(&items).contains(&"c2".to_string()),
            "alias defined after the cursor must resolve"
        );
    }

    #[test]
    fn aliases_do_not_leak_across_statements() {
        let mut metadata = SqlCompletionMetadata::default();
        metadata.add_table(&t1_table());

        let source = "SELECT * FROM t1 a;\nSELECT a. FROM t2 b";
        let cursor = source.rfind("a.").expect("qualifier position") + 2;
        let items = sql_completion_items(&metadata, source, cursor);
        assert!(
            items.is_empty(),
            "alias from a neighboring statement must not resolve"
        );
    }

    #[test]
    fn alias_resolution_survives_semicolon_inside_string_literal() {
        let mut metadata = SqlCompletionMetadata::default();
        metadata.add_table(&t1_table());

        let source = "SELECT a. FROM t1 a WHERE c1 = 'a;b'";
        let cursor = "SELECT a.".len();
        let items = sql_completion_items(&metadata, source, cursor);
        assert!(labels(&items).contains(&"c3".to_string()));
    }

    fn listing(
        database: &str,
        schema: Option<&str>,
        name: &str,
        has_columns: bool,
    ) -> KnownTableListing {
        KnownTableListing {
            database: database.to_string(),
            schema: schema.map(String::from),
            name: name.to_string(),
            has_columns,
        }
    }

    fn table_ref(schema: Option<&str>, table: &str) -> QueryTableRef {
        QueryTableRef {
            database: None,
            schema: schema.map(String::from),
            table: table.to_string(),
        }
    }

    #[test]
    fn tables_needing_details_matches_unqualified_and_qualified_refs() {
        let known = vec![
            listing("db1", Some("s1"), "t1", false),
            listing("db1", Some("s2"), "t1", false),
            listing("db1", Some("s1"), "t2", true),
        ];

        let keys = tables_needing_details(&known, &[table_ref(None, "t1")]);
        assert_eq!(
            keys.len(),
            2,
            "unqualified reference matches every schema candidate"
        );

        let keys = tables_needing_details(&known, &[table_ref(Some("s2"), "t1")]);
        assert_eq!(
            keys,
            vec![("db1".to_string(), Some("s2".to_string()), "t1".to_string())]
        );

        let keys = tables_needing_details(&known, &[table_ref(None, "t2")]);
        assert!(
            keys.is_empty(),
            "listings that already carry columns are skipped"
        );

        let keys = tables_needing_details(&known, &[table_ref(None, "nonexistent")]);
        assert!(keys.is_empty(), "unknown tables are never fetched");
    }

    #[test]
    fn tables_needing_details_matches_database_qualifier_refs() {
        let known = vec![listing("db2", None, "t2", false)];

        // `db.table` parses as a schema qualifier, so it must match the
        // listing's database key.
        let keys = tables_needing_details(&known, &[table_ref(Some("db2"), "t2")]);
        assert_eq!(keys, vec![("db2".to_string(), None, "t2".to_string())]);

        let keys = tables_needing_details(&known, &[table_ref(Some("db3"), "t2")]);
        assert!(keys.is_empty());
    }

    #[test]
    fn known_relational_tables_pulls_snapshot_and_database_schemas() {
        let snapshot = SchemaSnapshot::relational(RelationalSchema {
            databases: vec![],
            current_database: Some("db1".to_string()),
            schemas: vec![DbSchemaInfo {
                name: "s1".to_string(),
                tables: vec![t1_table()],
                views: vec![],
                custom_types: None,
            }],
            tables: vec![],
            views: vec![],
        });

        let t2 = TableInfo {
            name: "t2".to_string(),
            schema: None,
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::default(),
            child_items: None,
            storage_hints: None,
        };
        let db2 = (
            "db2".to_string(),
            DbSchemaInfo {
                name: "db2".to_string(),
                tables: vec![t2],
                views: vec![],
                custom_types: None,
            },
        );

        let known = known_relational_tables(
            Some(&snapshot),
            std::iter::once((&db2.0, &db2.1)),
            Some("db1"),
        );

        assert_eq!(known.len(), 2);
        assert_eq!(
            known[0].database, "db1",
            "snapshot tables use the caller-provided database"
        );
        assert_eq!(known[0].name, "t1");
        assert!(known[0].has_columns);
        assert_eq!(known[1].database, "db2", "lazy schemas use their cache key");
        assert_eq!(known[1].name, "t2");
        assert!(!known[1].has_columns);

        // Without a snapshot database there is no valid fetch key; snapshot
        // tables are skipped rather than tagged with a fabricated name.
        let without_database =
            known_relational_tables(Some(&snapshot), std::iter::once((&db2.0, &db2.1)), None);
        assert_eq!(without_database.len(), 1);
        assert_eq!(without_database[0].name, "t2");
    }

    fn t2_table() -> TableInfo {
        TableInfo {
            name: "t2".to_string(),
            schema: None,
            columns: Some(vec![column("c4", "integer"), column("c5", "timestamp")]),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::default(),
            child_items: None,
            storage_hints: None,
        }
    }

    fn context_metadata() -> SqlCompletionMetadata {
        let mut metadata = SqlCompletionMetadata::default();
        metadata.add_table(&t1_table());
        metadata.add_table(&t2_table());
        metadata
    }

    fn analyzed_items(metadata: &SqlCompletionMetadata, source: &str) -> Vec<CompletionItem> {
        let engine = dbflux_core::SqlContextEngine::new().expect("grammar loads");
        let analysis = engine.analyze(source, source.len()).expect("analysis");
        sql_completion_items_with_context(metadata, source, source.len(), Some(&analysis))
    }

    #[test]
    fn item_builder_never_panics_on_multi_byte_input() {
        let metadata = context_metadata();
        let engine = dbflux_core::SqlContextEngine::new().expect("grammar loads");
        let fixtures = [
            "SELECT c1 FROM t1 WHERE c1 = 'héllo wörld' AND ",
            "SELECT c1 FROM t1 WHERE c1 = '名前' GROUP BY ",
            "not sql at all 🎉 ;;; ",
            "",
        ];

        for source in fixtures {
            for raw in 0..=source.len() {
                // completions() floors the offset before calling the builder.
                let cursor = source.floor_char_boundary(raw);
                let analysis = engine.analyze(source, cursor);
                let _items =
                    sql_completion_items_with_context(&metadata, source, cursor, analysis.as_ref());
            }
        }
    }

    fn sort_text_of<'a>(items: &'a [CompletionItem], label: &str) -> &'a str {
        items
            .iter()
            .find(|item| item.label == label)
            .and_then(|item| item.sort_text.as_deref())
            .unwrap_or_else(|| panic!("item {label} missing or unranked"))
    }

    #[test]
    fn select_list_fills_columns_when_trailing_comma_breaks_the_parse() {
        // A trailing comma makes the grammar swallow FROM into the select
        // list, leaving the tree-sitter scope empty; the column position must
        // still fill from the referenced table, without a typed prefix.
        let mut metadata = SqlCompletionMetadata::default();
        metadata.add_table(&t1_table());

        let source = "SELECT c1, c2, FROM t1";
        // Cursor right after the trailing comma and one space.
        let cursor = source.find(", FROM").expect("comma") + 2;
        let engine = dbflux_core::SqlContextEngine::new().expect("grammar loads");
        let analysis = engine.analyze(source, cursor).expect("analysis");
        let items = sql_completion_items_with_context(&metadata, source, cursor, Some(&analysis));

        assert!(labels(&items).contains(&"c1".to_string()));
        assert!(labels(&items).contains(&"c2".to_string()));
    }

    #[test]
    fn context_where_offers_scoped_columns_with_empty_prefix() {
        let metadata = context_metadata();
        let source = "SELECT * FROM t1 a WHERE ";
        let items = analyzed_items(&metadata, source);

        let labels = labels(&items);
        assert!(
            labels.contains(&"c2".to_string()),
            "in-scope column surfaces with an empty prefix"
        );
        assert!(
            !labels.contains(&"c5".to_string()),
            "columns of tables outside the statement stay hidden"
        );
        assert!(
            sort_text_of(&items, "c2") < sort_text_of(&items, "WHERE"),
            "scoped columns rank above keywords"
        );
        assert!(
            labels.contains(&"a".to_string()),
            "relation aliases are offered alongside columns"
        );
    }

    #[test]
    fn context_from_ranks_tables_above_keywords() {
        let metadata = context_metadata();
        let items = analyzed_items(&metadata, "SELECT * FROM ");

        assert!(
            sort_text_of(&items, "t1") < sort_text_of(&items, "SELECT"),
            "tables rank above keywords in FROM position"
        );
    }

    #[test]
    fn context_from_offers_cte_names() {
        let metadata = context_metadata();
        let source = "WITH cte1 AS (SELECT * FROM t1) SELECT * FROM ";
        let items = analyzed_items(&metadata, source);
        assert!(labels(&items).contains(&"cte1".to_string()));
    }

    #[test]
    fn dot_qualifier_prefers_innermost_scope_alias() {
        let metadata = context_metadata();
        let source = "SELECT * FROM t1 a WHERE c1 IN (SELECT a.c5 FROM t2 a WHERE a.";
        let items = analyzed_items(&metadata, source);

        let labels = labels(&items);
        assert!(
            labels.contains(&"c5".to_string()),
            "inner alias resolves to the subquery relation"
        );
        assert!(
            !labels.contains(&"c2".to_string()),
            "outer relation must be shadowed by the inner alias"
        );
    }

    #[test]
    fn context_on_offers_columns_of_both_join_sides() {
        let metadata = context_metadata();
        let source = "SELECT * FROM t1 a JOIN t2 b ON ";
        let items = analyzed_items(&metadata, source);

        let labels = labels(&items);
        assert!(labels.contains(&"c1".to_string()));
        assert!(labels.contains(&"c4".to_string()));
    }

    #[test]
    fn whitespace_after_finished_identifier_stays_silent() {
        // Space-triggered request in keyword territory: no popup.
        let metadata = context_metadata();
        let source = "SELECT * FROM t1 a WHERE a.c1 > 10 AND c2 ";
        let items = analyzed_items(&metadata, source);
        assert!(
            items.is_empty(),
            "no context-relevant items, menu stays closed"
        );
    }

    #[test]
    fn whitespace_in_column_context_without_metadata_stays_silent() {
        // `SELECT ` at buffer start: column context, but no scope columns and
        // no prefix, so a keyword-only popup is suppressed.
        let metadata = SqlCompletionMetadata::default();
        let items = analyzed_items(&metadata, "SELECT ");
        assert!(items.is_empty());
    }

    #[test]
    fn qualified_reference_falls_back_to_bare_metadata_key() {
        // The cached listings carry no schema, so a database-qualified query
        // reference must still resolve their columns via the bare name.
        let metadata = context_metadata();
        let source = "SELECT * FROM db1.t1 WHERE ";
        let items = analyzed_items(&metadata, source);
        assert!(labels(&items).contains(&"c2".to_string()));
    }

    #[test]
    fn select_alias_offered_in_group_by_but_not_in_where() {
        let metadata = context_metadata();

        let group_by = "SELECT c3 * 2 AS a1 FROM t1 GROUP BY ";
        let items = analyzed_items(&metadata, group_by);
        assert!(
            labels(&items).contains(&"a1".to_string()),
            "output alias is a valid GROUP BY target"
        );

        let where_clause = "SELECT c3 * 2 AS a1 FROM t1 WHERE ";
        let items = analyzed_items(&metadata, where_clause);
        assert!(
            !labels(&items).contains(&"a1".to_string()),
            "output aliases are not resolvable in WHERE"
        );
    }

    #[test]
    fn context_falls_back_to_all_columns_when_scope_has_no_metadata() {
        let metadata = context_metadata();
        // `tz` is not a known table: scope resolves but yields no columns, so
        // the pre-context prefix behavior applies.
        let source = "SELECT * FROM tz WHERE c5";
        let items = analyzed_items(&metadata, source);
        assert!(labels(&items).contains(&"c5".to_string()));
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
