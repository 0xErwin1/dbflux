use super::*;

// ---------------------------------------------------------------------------
// Column check: names resolved the way SQLite resolves them
// ---------------------------------------------------------------------------

/// Creates tables whose names SQLite resolves beyond `PRAGMA table_info`: a
/// generated column, a view, a column with a non-ASCII-foldable twin, a column
/// with a space and an FTS5 virtual table.
async fn start_engine_names_agent() -> (Agent, String, tempfile::TempDir) {
    let directory = tempfile::tempdir().expect("create the test data directory");

    let connection = rusqlite::Connection::open(directory.path().join("agent.sqlite"))
        .expect("open the test database");
    connection
        .execute_batch(
            "CREATE TABLE notes (label TEXT NOT NULL);
             INSERT INTO notes (label) VALUES ('alpha'), ('beta');
             CREATE VIEW note_view AS SELECT label FROM notes;
             CREATE TABLE doubled (a INTEGER, g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL);
             INSERT INTO doubled (a) VALUES (5), (7);
             CREATE TABLE pairs (\"key\" TEXT, \"first name\" TEXT);
             INSERT INTO pairs VALUES ('k1', 'Ada');
             CREATE VIRTUAL TABLE docs USING fts5(body);
             INSERT INTO docs (body) VALUES ('hello world'), ('bye');",
        )
        .expect("create the test tables");
    drop(connection);

    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    agent
        .call_json("connect", json!({ "connection_id": connection_id }))
        .await;

    (agent, connection_id, directory)
}

#[tokio::test]
async fn a_generated_column_runs_in_where_and_columns() {
    let (agent, connection_id, _directory) = start_engine_names_agent().await;

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "doubled",
                "columns": ["a", "g"],
                "where": { "g": 14 }
            }),
        )
        .await;

    assert_eq!(selected["rows"], json!([{ "a": 7, "g": 14 }]));
}

#[tokio::test]
async fn misspelled_columns_are_refused_wherever_sqlite_would_misread_them() {
    let (agent, connection_id, _directory) = start_engine_names_agent().await;

    let cases = [
        ("note_view", json!({ "labl": "alpha" }), "labl"),
        ("NOTES", json!({ "labl": "alpha" }), "labl"),
        ("pairs", json!({ "\u{212A}ey": "k1" }), "\u{212A}ey"),
        ("pairs", json!({ "frist name": "Ada" }), "frist name"),
    ];

    for (table, filter, column) in cases {
        let message = tool_error(
            &agent,
            "select_data",
            json!({ "connection_id": connection_id, "table": table, "where": filter }),
        )
        .await;

        assert!(
            message.starts_with(&format!(
                "Column '{column}' is not listed among the columns of table '{table}'."
            )),
            "'{column}' on '{table}' must be refused before SQLite reads it as a string: {message}"
        );
        assert!(message.ends_with(NOT_RUN), "{message}");
    }

    let spaced = agent
        .call_json(
            "select_data",
            json!({ "connection_id": connection_id, "table": "pairs", "where": { "first name": "Ada" } }),
        )
        .await;
    assert_eq!(
        spaced["rows"],
        json!([{ "key": "k1", "first name": "Ada" }])
    );
}

#[tokio::test]
async fn fts5_rowid_runs_in_columns() {
    let (agent, connection_id, _directory) = start_engine_names_agent().await;

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "docs",
                "columns": ["rowid", "body"],
                "order_by": [{ "column": "rowid" }]
            }),
        )
        .await;

    assert_eq!(
        selected["rows"],
        json!([
            { "rowid": 1, "body": "hello world" },
            { "rowid": 2, "body": "bye" }
        ])
    );
}

/// A dialect that fails on an unknown column gains nothing from a refusal
/// before running: the call runs, and its failure gets the hint.
#[tokio::test]
async fn a_loudly_failing_dialect_runs_the_call_and_hints_after_the_failure() {
    let (agent, connection_id) = start_hint_agent(ALLOW_ALL_ROLE, &["postgres"]).await;

    let message = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "users", "where": { "emial": "a@b.c" } }),
    )
    .await;

    assert!(
        message.starts_with(
            "Column 'emial' is not listed among the columns of table 'users'.\n\
             Did you mean: email?\n\
             Available columns: id, email\n\nOriginal error: "
        ),
        "got: {message}"
    );
    assert!(!message.contains(NOT_RUN), "the call ran: {message}");
}

/// The SQLite driver with `misreads_unknown_quoted_identifiers` turned off,
/// standing in for an engine that fails loudly on an unknown column while
/// still browsing and generating real queries.
struct LoudSqliteDriver {
    inner: dbflux_driver_sqlite::SqliteDriver,
    metadata: dbflux_core::DriverMetadata,
}

struct LoudSqliteConnection {
    inner: Box<dyn dbflux_core::Connection>,
    metadata: dbflux_core::DriverMetadata,
}

fn loud_metadata(metadata: &dbflux_core::DriverMetadata) -> dbflux_core::DriverMetadata {
    let mut metadata = metadata.clone();

    if let Some(syntax) = metadata.syntax.as_mut() {
        syntax.misreads_unknown_quoted_identifiers = false;
    }

    metadata
}

impl LoudSqliteDriver {
    fn new() -> Arc<dyn DbDriver> {
        let inner = dbflux_driver_sqlite::SqliteDriver::new();
        let metadata = loud_metadata(inner.metadata());

        Arc::new(Self { inner, metadata })
    }
}

impl DbDriver for LoudSqliteDriver {
    fn kind(&self) -> dbflux_core::DbKind {
        self.inner.kind()
    }

    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        &self.metadata
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        self.inner.driver_key()
    }

    fn form_definition(&self) -> &dbflux_core::DriverFormDef {
        self.inner.form_definition()
    }

    fn build_config(
        &self,
        values: &dbflux_core::FormValues,
    ) -> Result<DbConfig, dbflux_core::DbError> {
        self.inner.build_config(values)
    }

    fn extract_values(&self, config: &DbConfig) -> dbflux_core::FormValues {
        self.inner.extract_values(config)
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&dbflux_core::secrecy::SecretString>,
        ssh_secret: Option<&dbflux_core::secrecy::SecretString>,
    ) -> Result<Box<dyn dbflux_core::Connection>, dbflux_core::DbError> {
        let inner = self
            .inner
            .connect_with_secrets(profile, password, ssh_secret)?;
        let metadata = loud_metadata(inner.metadata());

        Ok(Box::new(LoudSqliteConnection { inner, metadata }))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), dbflux_core::DbError> {
        self.inner.test_connection(profile)
    }
}

impl dbflux_core::Connection for LoudSqliteConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        &self.metadata
    }

    fn ping(&self) -> Result<(), dbflux_core::DbError> {
        self.inner.ping()
    }

    fn close(&mut self) -> Result<(), dbflux_core::DbError> {
        self.inner.close()
    }

    fn execute(
        &self,
        request: &dbflux_core::QueryRequest,
    ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
        self.inner.execute(request)
    }

    fn cancel(&self, handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        self.inner.cancel(handle)
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        self.inner.schema()
    }

    fn kind(&self) -> dbflux_core::DbKind {
        self.inner.kind()
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        self.inner.schema_loading_strategy()
    }

    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        self.inner.dialect()
    }

    fn browse_table(
        &self,
        request: &dbflux_core::TableBrowseRequest,
    ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
        self.inner.browse_table(request)
    }

    fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
        self.inner.query_generator()
    }

    fn language_service(&self) -> &dyn dbflux_core::LanguageService {
        self.inner.language_service()
    }

    fn table_details(
        &self,
        database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<dbflux_core::TableInfo, dbflux_core::DbError> {
        self.inner.table_details(database, schema, table)
    }
}

/// On an engine that is not checked before running, a pseudo-column in
/// `columns` is found after the browse, which cannot return it, and the call
/// reruns as a generated SELECT. An ordinary call reads no metadata.
#[tokio::test]
async fn a_loud_dialect_finds_a_projected_pseudo_column_after_the_browse() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    create_rowid_tables(&directory);

    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((LoudSqliteDriver::new(), profile))).await;

    agent
        .call_json("connect", json!({ "connection_id": connection_id }))
        .await;

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "notes",
                "columns": ["rowid", "label"],
                "where": { "label": "beta" }
            }),
        )
        .await;
    assert_eq!(selected["rows"], json!([{ "rowid": 2, "label": "beta" }]));
    assert!(
        latest_select_data_audit_details(&agent).await["query"].is_string(),
        "the rerun records its generated query"
    );

    let missing = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "notes", "columns": ["labl"] }),
    )
    .await;
    assert!(
        missing.starts_with("Select error: column 'labl' not found in result set"),
        "a column that is not a pseudo-column keeps the browse error: {missing}"
    );
}
