use std::sync::Arc;

use crate::Connection;
use crate::TableInfo;
use crate::schema::{
    SchemaDiff, SchemaDriftDetected, SchemaFingerprint, diff_table_info,
    query_parser::QueryTableRef,
};

/// Key used to address an entry in `ConnectedProfile::table_details`.
pub type TableKey = (String, String);

/// Outcome returned by [`check_schema_drift`].
pub enum DriftOutcome {
    /// Driver returned `None` from `referenced_tables` — drift detection does
    /// not apply (NoSQL, key-value, or other drivers that do not parse SQL).
    Skip,

    /// No drift detected. The `Vec` contains `(key, fresh TableInfo)` entries
    /// that the caller should write back into `ConnectedProfile::table_details`
    /// so the cache stays current.
    Refresh(Vec<(TableKey, TableInfo)>),

    /// At least one table has drifted. `diffs` has one entry per changed table;
    /// `refreshes` has the unchanged tables that should be refreshed transparently
    /// once the user acknowledges the modal.
    Drift(SchemaDriftDetected),
}

/// Pure synchronous helper: compare `cached` against `fresh` and return the
/// per-table outcome. Called from inside the blocking background task so its
/// I/O-free path is always unit-testable.
///
/// Returns `None` when the two snapshots are fingerprint-identical (no drift).
/// Returns `Some(changes)` when they differ.
pub fn check_drift_sync(
    cached: &TableInfo,
    fresh: &TableInfo,
) -> Option<Vec<crate::schema::SchemaChange>> {
    let before_fp = SchemaFingerprint::from_table_info(cached);
    let after_fp = SchemaFingerprint::from_table_info(fresh);

    if before_fp == after_fp {
        return None;
    }

    Some(diff_table_info(cached, fresh))
}

/// Check whether any tables referenced by `query` have drifted since they were
/// last cached in `table_details`.
///
/// `default_schema` is the caller's hint for which schema to introspect when
/// the query does not qualify the table (e.g. the editor's active schema
/// selector). It is only consulted as a tertiary fallback — the cached
/// `TableInfo.schema` takes precedence because it records the exact schema
/// the table was originally loaded from.
///
/// **Schema resolution precedence per table reference:**
/// 1. Explicit qualifier in the query (`FROM public.users`).
/// 2. The cached entry's `schema` field, when an entry exists. The cache is
///    populated by sidebar/refresh paths that always know the right schema,
///    so the cached entry is the authoritative pointer to where the table
///    actually lives.
/// 3. The caller-supplied `default_schema` (toolbar selection).
/// 4. `"public"` as a last-resort driver-friendly fallback.
///
/// **Trigger rules:**
/// - If the driver's `referenced_tables` returns `None`, return `Skip`.
/// - For each referenced table:
///   - If the fresh fetch returns no columns at all, skip the entry silently.
///     A real table with zero columns is essentially impossible; an empty
///     result almost always means the schema lookup was wrong (search path,
///     dropped table, search-path race). Reporting "all columns removed"
///     would be noise, and writing the empty `TableInfo` into the cache
///     would poison autocomplete and the table-detail view.
///   - If no cache entry exists, this is a first encounter — add to `refreshes`
///     and proceed silently (no diff possible).
///   - If the entry exists and fingerprints match, add to `refreshes` for a
///     transparent cache update.
///   - If the entry exists and fingerprints differ, add to `diffs`.
/// - If `diffs` is empty, return `Refresh(refreshes)`.
/// - Otherwise, return `Drift` with both `diffs` and `refreshes`.
///
/// The caller is responsible for:
/// - Running this in a background thread (it issues blocking I/O).
/// - On `Refresh`: writing all entries back into `ConnectedProfile::table_details`.
/// - On `Drift`: opening the modal. After "Refresh & re-run", writing both
///   the `refreshes` entries AND each `diff.fresh` into the cache. After
///   "Continue with stale", NOT updating the cache.
pub fn check_schema_drift(
    connection: &Arc<dyn Connection>,
    table_details: &std::collections::HashMap<(String, String), TableInfo>,
    query: &str,
    database: &str,
    default_schema: Option<&str>,
) -> DriftOutcome {
    let table_refs = match connection.referenced_tables(query) {
        None => return DriftOutcome::Skip,
        Some(refs) => refs,
    };

    let mut diffs: Vec<SchemaDiff> = Vec::new();
    let mut refreshes: Vec<(TableKey, TableInfo)> = Vec::new();

    for table_ref in &table_refs {
        let table_name = &table_ref.table;
        let effective_db = table_ref.database.as_deref().unwrap_or(database);
        let cache_key: TableKey = (effective_db.to_string(), table_name.clone());
        let cached = table_details.get(&cache_key);

        let schema = table_ref
            .schema
            .as_deref()
            .or_else(|| cached.and_then(|c| c.schema.as_deref()))
            .or(default_schema)
            .unwrap_or("public");

        let fresh = match connection.table_details(effective_db, Some(schema), table_name) {
            Ok(info) => info,
            Err(_) => continue,
        };

        let fresh_has_columns = fresh.columns.as_deref().is_some_and(|c| !c.is_empty());
        if !fresh_has_columns {
            // Treat empty/None fresh columns as "schema lookup did not resolve
            // a real table" — most commonly a wrong schema (search path, the
            // user selected a different schema than where the table lives) or
            // a dropped table. Either way, do not surface a phantom diff and
            // do not write the empty TableInfo into the cache.
            log::warn!(
                "[DRIFT] fresh fetch for {effective_db}.{schema}.{table_name} returned no \
                 columns; skipping (cached had {} cols)",
                cached
                    .and_then(|c| c.columns.as_deref())
                    .map(|c| c.len())
                    .unwrap_or(0)
            );
            continue;
        }

        match cached {
            None => {
                // First encounter — populate cache silently, no drift to report.
                refreshes.push((cache_key, fresh));
            }

            Some(cached) => match check_drift_sync(cached, &fresh) {
                None => {
                    // No drift — schedule transparent cache refresh.
                    refreshes.push((cache_key, fresh));
                }

                Some(changes) => {
                    diffs.push(SchemaDiff {
                        table: QueryTableRef {
                            database: table_ref.database.clone(),
                            schema: table_ref.schema.clone(),
                            table: table_name.clone(),
                        },
                        changes,
                        fresh,
                    });
                }
            },
        }
    }

    if diffs.is_empty() {
        DriftOutcome::Refresh(refreshes)
    } else {
        DriftOutcome::Drift(SchemaDriftDetected { diffs, refreshes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnInfo, TableInfo};

    fn col(name: &str, type_name: &str, nullable: bool, is_pk: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            type_name: type_name.to_string(),
            nullable,
            is_primary_key: is_pk,
            default_value: None,
            enum_values: None,
        }
    }

    fn make_table(name: &str, columns: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            schema: Some("public".to_string()),
            columns: Some(columns),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
        }
    }

    // --- check_drift_sync covers all 4 outcomes via pure logic ---
    //
    // The four logical outcomes for a given (cached, fresh) pair are:
    //   1. Identical tables → None (no drift; caller emits Refresh)
    //   2. Changed tables   → Some(changes) (drift; caller emits Drift)
    //
    // The other two outcomes of check_schema_drift (Skip and first-encounter)
    // are determined by the Connection trait and cache key presence, not by
    // the comparison logic itself, so they are covered in integration tests
    // that use a real or fake driver.

    #[test]
    fn outcome_no_drift_identical_tables() {
        let table = make_table("users", vec![col("id", "integer", false, true)]);
        assert!(
            check_drift_sync(&table, &table).is_none(),
            "identical tables must produce no drift"
        );
    }

    #[test]
    fn outcome_drift_column_added() {
        let cached = make_table("users", vec![col("id", "integer", false, true)]);
        let fresh = make_table(
            "users",
            vec![
                col("id", "integer", false, true),
                col("email", "text", false, false),
            ],
        );
        let changes = check_drift_sync(&cached, &fresh).expect("column added must produce drift");
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, crate::schema::SchemaChange::ColumnAdded(_)))
        );
    }

    #[test]
    fn outcome_drift_column_type_changed() {
        let cached = make_table("users", vec![col("id", "integer", false, true)]);
        let fresh = make_table("users", vec![col("id", "bigint", false, true)]);
        let changes = check_drift_sync(&cached, &fresh).expect("type change must produce drift");
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, crate::schema::SchemaChange::ColumnTypeChanged { .. }))
        );
    }

    #[test]
    fn outcome_drift_column_removed() {
        let cached = make_table(
            "users",
            vec![
                col("id", "integer", false, true),
                col("email", "text", false, false),
            ],
        );
        let fresh = make_table("users", vec![col("id", "integer", false, true)]);
        let changes = check_drift_sync(&cached, &fresh).expect("removed column must produce drift");
        assert!(changes.iter().any(
            |c| matches!(c, crate::schema::SchemaChange::ColumnRemoved(s) if s.name == "email")
        ));
    }

    // --- check_schema_drift integration: schema resolution & defensive guard ---
    //
    // These tests exercise the orchestration around `check_drift_sync`:
    //   1. Which schema is passed to `connection.table_details()` for the fresh
    //      fetch (the bug fixed here was a hardcoded "public" fallback).
    //   2. The defensive skip when the fresh fetch yields no columns.
    //
    // A minimal in-module mock implements just enough of `Connection`. The
    // mock returns the configured `TableInfo` when the driver is asked about a
    // `(db, schema, table)` triple it knows, and `Ok(empty)` otherwise — this
    // mirrors how PostgreSQL's `get_columns` behaves when the table does not
    // live in the queried schema.

    use crate::{
        DbError, DbKind, DefaultSqlDialect, DriverCapabilities, DriverMetadata, QueryHandle,
        QueryLanguage, QueryRequest, QueryResult, SchemaLoadingStrategy, SchemaSnapshot,
        SqlDialect,
    };
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn minimal_metadata() -> DriverMetadata {
        DriverMetadata {
            id: "mock".to_string(),
            display_name: "Mock".to_string(),
            description: "test".to_string(),
            category: crate::DatabaseCategory::Relational,
            deployment_class: None,
            query_language: QueryLanguage::Sql,
            capabilities: DriverCapabilities::empty(),
            default_port: None,
            uri_scheme: "mock".to_string(),
            icon: crate::Icon::Database,
            syntax: None,
            query: None,
            mutation: None,
            ddl: None,
            transactions: None,
            limits: None,
            ssl_modes: None,
            ssl_cert_fields: None,
            classification_override: None,
            default_chunk_size: None,
            supports_lock_timeout: false,
            editor_profile: None,
        }
    }

    /// Driver mock that returns configured `TableInfo` for known
    /// `(database, schema, table)` triples and an empty `TableInfo` otherwise.
    struct MockDriver {
        metadata: DriverMetadata,
        refs: Vec<QueryTableRef>,
        known: HashMap<(String, String, String), TableInfo>,
        last_lookups: Mutex<Vec<(String, Option<String>, String)>>,
    }

    impl MockDriver {
        fn new(refs: Vec<QueryTableRef>) -> Self {
            Self {
                metadata: minimal_metadata(),
                refs,
                known: HashMap::new(),
                last_lookups: Mutex::new(Vec::new()),
            }
        }

        fn with_table(mut self, db: &str, schema: &str, table: &str, info: TableInfo) -> Self {
            self.known.insert(
                (db.to_string(), schema.to_string(), table.to_string()),
                info,
            );
            self
        }
    }

    impl Connection for MockDriver {
        fn metadata(&self) -> &DriverMetadata {
            &self.metadata
        }
        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }
        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }
        fn execute(&self, _req: &QueryRequest) -> Result<QueryResult, DbError> {
            Err(DbError::NotSupported("mock".into()))
        }
        fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
            Ok(())
        }
        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Ok(SchemaSnapshot::default())
        }
        fn kind(&self) -> DbKind {
            DbKind::Postgres
        }
        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::LazyPerDatabase
        }
        fn dialect(&self) -> &dyn SqlDialect {
            &DefaultSqlDialect
        }

        fn referenced_tables(&self, _query: &str) -> Option<Vec<QueryTableRef>> {
            Some(self.refs.clone())
        }

        fn table_details(
            &self,
            database: &str,
            schema: Option<&str>,
            table: &str,
        ) -> Result<TableInfo, DbError> {
            self.last_lookups.lock().unwrap().push((
                database.to_string(),
                schema.map(String::from),
                table.to_string(),
            ));

            let key = (
                database.to_string(),
                schema.unwrap_or("").to_string(),
                table.to_string(),
            );

            if let Some(info) = self.known.get(&key) {
                return Ok(info.clone());
            }

            // Mirror PostgreSQL: looking up a table in the wrong schema is not
            // an error, it returns an empty column list.
            Ok(TableInfo {
                name: table.to_string(),
                schema: schema.map(String::from),
                columns: Some(Vec::new()),
                indexes: None,
                foreign_keys: None,
                constraints: None,
                sample_fields: None,
                presentation: Default::default(),
                child_items: None,
            })
        }
    }

    fn make_table_in(schema: &str, name: &str, columns: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            schema: Some(schema.to_string()),
            columns: Some(columns),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
        }
    }

    /// The original bug: cached entry sits in `analytics`, query is unqualified,
    /// editor toolbar's schema is `public` (the document default). Without the
    /// fix, the fresh fetch is forced through `public` and reports all columns
    /// as removed. With the fix, the cached entry's `schema` field steers the
    /// fresh fetch to `analytics`, so the fingerprints match and no drift is
    /// reported.
    #[test]
    fn cached_schema_overrides_default_schema_for_fresh_fetch() {
        let table_in_analytics = make_table_in(
            "analytics",
            "events",
            vec![col("id", "integer", false, true)],
        );

        let mut cache: std::collections::HashMap<(String, String), TableInfo> =
            std::collections::HashMap::new();
        cache.insert(
            ("app_db".to_string(), "events".to_string()),
            table_in_analytics.clone(),
        );

        let driver: Arc<dyn Connection> = Arc::new(
            MockDriver::new(vec![QueryTableRef {
                database: None,
                schema: None,
                table: "events".to_string(),
            }])
            .with_table("app_db", "analytics", "events", table_in_analytics),
        );

        let outcome = check_schema_drift(
            &driver,
            &cache,
            "select * from events",
            "app_db",
            Some("public"),
        );

        match outcome {
            DriftOutcome::Refresh(entries) => {
                assert_eq!(entries.len(), 1);
                let (_, info) = &entries[0];
                assert_eq!(info.schema.as_deref(), Some("analytics"));
            }
            DriftOutcome::Skip => panic!("expected Refresh, got Skip"),
            DriftOutcome::Drift(_) => panic!(
                "must not report drift when cached.schema steers fresh fetch to the right schema"
            ),
        }
    }

    /// When no cache entry exists yet and the query is unqualified, fall back
    /// to the caller-supplied default schema (the editor's toolbar selection).
    #[test]
    fn default_schema_used_when_cache_is_empty_and_query_unqualified() {
        let cache: std::collections::HashMap<(String, String), TableInfo> =
            std::collections::HashMap::new();

        let table = make_table_in(
            "analytics",
            "tbl_v",
            vec![col("id", "integer", false, true)],
        );

        let mock_driver = Arc::new(
            MockDriver::new(vec![QueryTableRef {
                database: None,
                schema: None,
                table: "tbl_v".to_string(),
            }])
            .with_table("db", "analytics", "tbl_v", table.clone()),
        );
        let driver: Arc<dyn Connection> = mock_driver.clone();

        let outcome = check_schema_drift(
            &driver,
            &cache,
            "select * from tbl_v",
            "db",
            Some("analytics"),
        );

        assert!(matches!(outcome, DriftOutcome::Refresh(_)));

        let lookups = mock_driver.last_lookups.lock().unwrap();
        assert_eq!(lookups.len(), 1);
        assert_eq!(lookups[0].1.as_deref(), Some("analytics"));
    }

    /// Defensive skip: if the fresh fetch resolves to a schema where the
    /// table doesn't exist, the driver returns an empty column list. The
    /// drift checker must silently drop the entry instead of reporting
    /// "all columns removed" and corrupting the cache.
    #[test]
    fn empty_fresh_columns_do_not_emit_diff_or_refresh() {
        // Cached entry has columns but no recorded schema (mimics a driver
        // that returns `schema: None` for the cached TableInfo). With the
        // cached-schema fallback unavailable, the only remaining hint is
        // the default `"public"` — which the mock driver does not know,
        // so the fresh fetch comes back empty.
        let mut cached_without_schema = make_table_in(
            "public",
            "tbl_v",
            vec![
                col("id", "integer", false, true),
                col("name", "text", true, false),
            ],
        );
        cached_without_schema.schema = None;

        let mut cache: std::collections::HashMap<(String, String), TableInfo> =
            std::collections::HashMap::new();
        cache.insert(
            ("db".to_string(), "tbl_v".to_string()),
            cached_without_schema,
        );

        let driver: Arc<dyn Connection> = Arc::new(MockDriver::new(vec![QueryTableRef {
            database: None,
            schema: None,
            table: "tbl_v".to_string(),
        }]));

        let outcome = check_schema_drift(&driver, &cache, "select * from tbl_v", "db", None);

        match outcome {
            DriftOutcome::Refresh(entries) => {
                assert!(
                    entries.is_empty(),
                    "empty fresh must not be written into the cache"
                );
            }
            DriftOutcome::Skip => panic!("driver supports referenced_tables — must not Skip"),
            DriftOutcome::Drift(_) => {
                panic!("empty fresh columns must not surface as a phantom diff")
            }
        }
    }

    /// First-encounter with an empty fresh result also gets skipped — we must
    /// never persist a zero-column `TableInfo` into the cache, since the
    /// sidebar and autocomplete treat `Some(vec![])` as "loaded with no
    /// columns" and stop trying to fetch real data.
    #[test]
    fn empty_fresh_columns_skipped_on_first_encounter() {
        let cache: std::collections::HashMap<(String, String), TableInfo> =
            std::collections::HashMap::new();

        let driver: Arc<dyn Connection> = Arc::new(MockDriver::new(vec![QueryTableRef {
            database: None,
            schema: None,
            table: "tbl_v".to_string(),
        }]));

        let outcome =
            check_schema_drift(&driver, &cache, "select * from tbl_v", "db", Some("public"));

        match outcome {
            DriftOutcome::Refresh(entries) => {
                assert!(
                    entries.is_empty(),
                    "first-encounter cache writes must skip empty TableInfo to avoid poisoning"
                );
            }
            other => panic!(
                "expected empty Refresh; got {:?}",
                match other {
                    DriftOutcome::Skip => "Skip",
                    DriftOutcome::Drift(_) => "Drift",
                    DriftOutcome::Refresh(_) => unreachable!(),
                }
            ),
        }
    }

    /// Query qualifier still wins over cached schema and default schema.
    #[test]
    fn explicit_query_schema_wins_over_cached_and_default() {
        let table_in_analytics = make_table_in(
            "analytics",
            "tbl_v",
            vec![col("id", "integer", false, true)],
        );
        let table_in_public =
            make_table_in("public", "tbl_v", vec![col("id", "bigint", false, true)]);

        let mut cache: std::collections::HashMap<(String, String), TableInfo> =
            std::collections::HashMap::new();
        cache.insert(
            ("db".to_string(), "tbl_v".to_string()),
            table_in_analytics.clone(),
        );

        let mock_driver = Arc::new(
            MockDriver::new(vec![QueryTableRef {
                database: None,
                schema: Some("public".to_string()),
                table: "tbl_v".to_string(),
            }])
            .with_table("db", "analytics", "tbl_v", table_in_analytics)
            .with_table("db", "public", "tbl_v", table_in_public),
        );
        let driver: Arc<dyn Connection> = mock_driver.clone();

        let _ = check_schema_drift(
            &driver,
            &cache,
            "select * from public.tbl_v",
            "db",
            Some("analytics"),
        );

        let lookups = mock_driver.last_lookups.lock().unwrap();
        assert_eq!(lookups[0].1.as_deref(), Some("public"));
    }
}
