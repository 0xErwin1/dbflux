//! Driver registration surface: metadata, connection form, and connect.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    Connection, ConnectionProfile, DatabaseCategory, DbConfig, DbDriver, DbError, DbKind,
    DdlCapabilities, DeploymentClass, DriverCapabilities, DriverFormDef, DriverLimits,
    DriverMetadata, FormFieldKind, FormSection, FormSectionIcon, FormTab, FormValues, Icon,
    IsolationLevel, MutationCapabilities, OrderByMode, PaginationStyle, PlaceholderStyle,
    QueryCapabilities, QueryLanguage, SyntaxInfo, TransactionCapabilities, TransferFamily,
    WhereOperator, field,
};

use crate::connection::{DuckDbConnection, SharedDuckDb, format_duckdb_error};
use crate::dialect::DUCKDB_DIALECT;

/// Catalog name an attached DuckLake gets, and the default after connecting.
pub(crate) const DUCKLAKE_ALIAS: &str = "lake";

pub static DUCKDB_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![FormTab {
        id: "main".into(),
        label: "Main".into(),
        sections: vec![
            FormSection {
                title: "Database".into(),
                icon: Some(FormSectionIcon::Database),
                fields: vec![field(
                    "path",
                    "Database File",
                    FormFieldKind::FilePath,
                    "Leave empty for an in-memory database",
                )],
            },
            FormSection {
                title: "DuckLake".into(),
                icon: Some(FormSectionIcon::Connection),
                fields: vec![
                    field(
                        "ducklake_catalog",
                        "Catalog",
                        FormFieldKind::Text,
                        "metadata.ducklake or postgres:dbname=lake",
                    ),
                    field(
                        "ducklake_data_path",
                        "Data Path",
                        FormFieldKind::Text,
                        "s3://bucket/lake/ (optional)",
                    ),
                ],
            },
            FormSection {
                title: "Startup".into(),
                icon: None,
                fields: vec![field(
                    "init_sql",
                    "Init SQL",
                    FormFieldKind::Text,
                    "CREATE SECRET (TYPE s3, PROVIDER credential_chain);",
                )],
            },
        ],
    }],
});

pub static METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "duckdb".into(),
    display_name: "DuckDB".into(),
    description: "Embedded analytical database, with DuckLake support".into(),
    category: DatabaseCategory::Relational,
    transfer_family: TransferFamily::Sql,
    deployment_class: Some(DeploymentClass::Embedded),
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::MULTIPLE_DATABASES.bits()
            | DriverCapabilities::SCHEMAS.bits()
            | DriverCapabilities::NATIVE_CONSOLE.bits()
            | DriverCapabilities::REQUEST_ROW_LIMIT.bits()
            | DriverCapabilities::TRANSACTIONS.bits()
            | DriverCapabilities::VIEWS.bits()
            | DriverCapabilities::INDEXES.bits()
            | DriverCapabilities::FOREIGN_KEYS.bits()
            | DriverCapabilities::CHECK_CONSTRAINTS.bits()
            | DriverCapabilities::UNIQUE_CONSTRAINTS.bits()
            | DriverCapabilities::PREPARED_STATEMENTS.bits()
            | DriverCapabilities::INSERT.bits()
            | DriverCapabilities::UPDATE.bits()
            | DriverCapabilities::DELETE.bits()
            | DriverCapabilities::RETURNING.bits()
            | DriverCapabilities::PAGINATION.bits()
            | DriverCapabilities::SORTING.bits()
            | DriverCapabilities::FILTERING.bits()
            | DriverCapabilities::EXPORT_CSV.bits()
            | DriverCapabilities::EXPORT_JSON.bits()
            | DriverCapabilities::QUERY_CANCELLATION.bits()
            | DriverCapabilities::TRANSACTIONAL_DDL.bits()
            | DriverCapabilities::MULTI_STATEMENT.bits()
            | DriverCapabilities::BULK_INSERT.bits(),
    ),
    default_port: None,
    uri_scheme: "duckdb".into(),
    icon: Icon::Duckdb,
    syntax: Some(SyntaxInfo {
        identifier_quote: '"',
        string_quote: '\'',
        placeholder_style: PlaceholderStyle::QuestionMark,
        supports_schemas: true,
        default_schema: Some("main".into()),
        case_sensitive_identifiers: false,
        misreads_unknown_quoted_identifiers: false,
    }),
    query: Some(QueryCapabilities {
        pagination: vec![PaginationStyle::Offset],
        where_operators: vec![
            WhereOperator::Eq,
            WhereOperator::Ne,
            WhereOperator::Gt,
            WhereOperator::Gte,
            WhereOperator::Lt,
            WhereOperator::Lte,
            WhereOperator::Like,
            WhereOperator::ILike,
            WhereOperator::Null,
            WhereOperator::In,
            WhereOperator::NotIn,
            WhereOperator::And,
            WhereOperator::Or,
            WhereOperator::Not,
        ],
        supports_order_by: true,
        order_by_mode: OrderByMode::AnyColumns,
        supports_group_by: true,
        supports_having: true,
        supports_distinct: true,
        supports_limit: true,
        supports_offset: true,
        supports_joins: true,
        supports_subqueries: true,
        supports_union: true,
        supports_intersect: true,
        supports_except: true,
        supports_case_expressions: true,
        supports_window_functions: true,
        supports_ctes: true,
        supports_explain: true,
        max_query_parameters: 0,
        max_order_by_columns: 0,
        max_group_by_columns: 0,
    }),
    mutation: Some(MutationCapabilities {
        supports_insert: true,
        supports_update: true,
        supports_delete: true,
        supports_upsert: true,
        supports_returning: true,
        supports_batch: true,
        supports_bulk_update: true,
        supports_bulk_delete: true,
        max_insert_values: 0,
    }),
    ddl: Some(DdlCapabilities {
        supports_create_database: false,
        supports_drop_database: false,
        supports_create_table: true,
        supports_drop_table: true,
        supports_alter_table: true,
        supports_create_index: true,
        supports_drop_index: true,
        supports_create_view: true,
        supports_drop_view: true,
        supports_create_trigger: false,
        supports_drop_trigger: false,
        transactional_ddl: true,
        supports_add_column: true,
        supports_drop_column: true,
        supports_rename_column: true,
        supports_alter_column: true,
        supports_add_constraint: false,
        supports_drop_constraint: false,
    }),
    transactions: Some(TransactionCapabilities {
        supports_transactions: true,
        supported_isolation_levels: vec![IsolationLevel::Snapshot],
        default_isolation_level: Some(IsolationLevel::Snapshot),
        supports_savepoints: false,
        supports_nested_transactions: false,
        supports_read_only: true,
        supports_deferrable: false,
    }),
    limits: Some(DriverLimits::default()),
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

/// One live DuckDB instance per file (or per profile for in-memory
/// databases). Every connection keeps the instance alive; the entry is reused
/// while any of them is still open. Each key has its own lock, so opening one
/// file (which may download extensions) does not hold up the others.
static INSTANCES: LazyLock<Mutex<HashMap<String, Arc<InstanceSlot>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

type InstanceSlot = Mutex<Vec<Weak<SharedDuckDb>>>;

/// The profile settings applied once when an instance opens. Init SQL and the
/// DuckLake attach change the instance itself, so a connection may only join
/// an instance that was opened with the same settings.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct InstanceSettings {
    ducklake_catalog: Option<String>,
    ducklake_data_path: Option<String>,
    init_sql: Option<String>,
}

#[derive(Default)]
pub struct DuckDbDriver;

impl DuckDbDriver {
    pub fn new() -> Self {
        Self
    }

    fn open(profile: &ConnectionProfile) -> Result<DuckDbConnection, DbError> {
        let DbConfig::DuckDB {
            path,
            ducklake_catalog,
            ducklake_data_path,
            init_sql,
        } = &profile.config
        else {
            return Err(DbError::InvalidProfile(
                "Profile is not a DuckDB configuration".into(),
            ));
        };

        let settings = InstanceSettings {
            ducklake_catalog: non_blank(ducklake_catalog),
            ducklake_data_path: non_blank(ducklake_data_path),
            init_sql: non_blank(init_sql),
        };

        let key = if path.as_os_str().is_empty() {
            format!("memory:{}", profile.id)
        } else {
            format!("file:{}", instance_path(path).display())
        };

        let shared = shared_instance(&key, path, &settings)?;
        let connection = DuckDbConnection::new(shared)?;

        if settings.ducklake_catalog.is_some() {
            connection.set_active_database(Some(DUCKLAKE_ALIAS))?;
        }

        Ok(connection)
    }
}

fn non_blank(value: &Option<String>) -> Option<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// The path that identifies a database file, so `./a.duckdb` and its absolute
/// form share one instance. The file may not exist yet, which `canonicalize`
/// refuses; its parent is canonicalized instead, so the key matches the one the
/// file gets once it exists (symlinked parents, Windows verbatim prefixes).
fn instance_path(path: &Path) -> PathBuf {
    if let Ok(canonical) = path.canonicalize() {
        return canonical;
    }
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    match (absolute.parent(), absolute.file_name()) {
        (Some(parent), Some(name)) => parent
            .canonicalize()
            .map(|parent| parent.join(name))
            .unwrap_or(absolute),
        _ => absolute,
    }
}

fn instance_slot(key: &str) -> Result<Arc<InstanceSlot>, DbError> {
    let mut instances = INSTANCES
        .lock()
        .map_err(|_| DbError::connection_failed("DuckDB instance registry poisoned"))?;
    Ok(instances.entry(key.to_string()).or_default().clone())
}

/// Clones a connection from a live instance for `key`, or opens a new one and
/// runs the startup SQL and DuckLake attach on it.
fn shared_instance(
    key: &str,
    path: &Path,
    settings: &InstanceSettings,
) -> Result<Arc<SharedDuckDb>, DbError> {
    let slot = instance_slot(key)?;
    let mut entries = slot
        .lock()
        .map_err(|_| DbError::connection_failed("DuckDB instance registry poisoned"))?;
    entries.retain(|entry| entry.strong_count() > 0);

    let raw = match entries.iter().find_map(Weak::upgrade) {
        Some(existing) if existing.settings() != settings => {
            return Err(DbError::connection_failed(
                "This DuckDB database is already open with different DuckLake or Init SQL \
                 settings. Disconnect the other connection to it first.",
            ));
        }
        Some(existing) => existing.try_clone()?,
        None => open_instance(path, settings)?,
    };

    let shared = Arc::new(SharedDuckDb::new(raw, key.to_string(), settings.clone()));
    entries.push(Arc::downgrade(&shared));
    Ok(shared)
}

/// Another connection to the instance behind `shared`, registered like the
/// connections the driver opens, so it keeps the instance reachable.
pub(crate) fn fork_instance(shared: &SharedDuckDb) -> Result<Arc<SharedDuckDb>, DbError> {
    let slot = instance_slot(shared.key())?;
    let mut entries = slot
        .lock()
        .map_err(|_| DbError::connection_failed("DuckDB instance registry poisoned"))?;
    entries.retain(|entry| entry.strong_count() > 0);

    let forked = Arc::new(SharedDuckDb::new(
        shared.try_clone()?,
        shared.key().to_string(),
        shared.settings().clone(),
    ));
    entries.push(Arc::downgrade(&forked));
    Ok(forked)
}

fn open_instance(path: &Path, settings: &InstanceSettings) -> Result<duckdb::Connection, DbError> {
    let connection = if path.as_os_str().is_empty() {
        duckdb::Connection::open_in_memory()
    } else {
        duckdb::Connection::open(path)
    }
    .map_err(|error| DbError::connection_failed(format_duckdb_error(&error)))?;

    if let Some(init_sql) = &settings.init_sql {
        connection.execute_batch(init_sql).map_err(|error| {
            DbError::connection_failed(format!("Init SQL failed: {}", format_duckdb_error(&error)))
        })?;
    }

    if let Some(catalog) = &settings.ducklake_catalog {
        connection
            .execute_batch(&ducklake_attach_sql(
                catalog,
                settings.ducklake_data_path.as_deref(),
            ))
            .map_err(|error| {
                DbError::connection_failed(format!(
                    "Attaching the DuckLake catalog failed: {}",
                    format_duckdb_error(&error)
                ))
            })?;
    }

    Ok(connection)
}

fn ducklake_attach_sql(catalog: &str, data_path: Option<&str>) -> String {
    use dbflux_core::SqlDialect;

    let literal = |value: &str| format!("'{}'", DUCKDB_DIALECT.escape_string(value));
    // A catalog may store a relative data path that only resolves from the
    // directory it was created in, so a path set on the profile always wins.
    let options = data_path
        .map(|path| format!(" (DATA_PATH {}, OVERRIDE_DATA_PATH true)", literal(path)))
        .unwrap_or_default();
    format!(
        "INSTALL ducklake; LOAD ducklake; ATTACH IF NOT EXISTS {} AS {}{};",
        literal(&format!("ducklake:{catalog}")),
        DUCKDB_DIALECT.quote_identifier(DUCKLAKE_ALIAS),
        options
    )
}

impl DbDriver for DuckDbDriver {
    fn kind(&self) -> DbKind {
        DbKind::DuckDB
    }

    fn metadata(&self) -> &DriverMetadata {
        &METADATA
    }

    fn picker_hint(&self) -> String {
        "file / DuckLake".to_string()
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        "builtin:duckdb".into()
    }

    fn form_definition(&self) -> &DriverFormDef {
        &DUCKDB_FORM
    }

    fn requires_password(&self) -> bool {
        false
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let text = |key: &str| {
            values
                .get(key)
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };

        Ok(DbConfig::DuckDB {
            path: text("path").map(PathBuf::from).unwrap_or_default(),
            ducklake_catalog: text("ducklake_catalog"),
            ducklake_data_path: text("ducklake_data_path"),
            init_sql: text("init_sql"),
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let mut values = FormValues::new();
        if let DbConfig::DuckDB {
            path,
            ducklake_catalog,
            ducklake_data_path,
            init_sql,
        } = config
        {
            values.insert("path".into(), path.to_string_lossy().to_string());
            for (key, value) in [
                ("ducklake_catalog", ducklake_catalog),
                ("ducklake_data_path", ducklake_data_path),
                ("init_sql", init_sql),
            ] {
                values.insert(key.into(), value.clone().unwrap_or_default());
            }
        }
        values
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        _password: Option<&SecretString>,
        _ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        Ok(Box::new(Self::open(profile)?))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        Self::open(profile)?.ping()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{QueryRequest, Value};

    fn profile(config: DbConfig) -> ConnectionProfile {
        ConnectionProfile::new("duck", config)
    }

    fn scalar(connection: &dyn Connection, sql: &str) -> Value {
        connection
            .execute(&QueryRequest::new(sql))
            .expect("query")
            .rows
            .remove(0)
            .remove(0)
    }

    #[test]
    fn form_round_trips_and_blank_path_means_in_memory() {
        let driver = DuckDbDriver::new();
        let mut values = FormValues::new();
        values.insert("ducklake_catalog".into(), " meta.ducklake ".into());

        let config = driver.build_config(&values).unwrap();
        assert!(matches!(
            &config,
            DbConfig::DuckDB { path, ducklake_catalog: Some(catalog), ducklake_data_path: None, init_sql: None }
                if path.as_os_str().is_empty() && catalog == "meta.ducklake"
        ));
        assert_eq!(
            driver
                .extract_values(&config)
                .get("ducklake_catalog")
                .map(String::as_str),
            Some("meta.ducklake")
        );
    }

    #[test]
    fn attach_sql_escapes_quotes() {
        assert_eq!(
            ducklake_attach_sql("it's.ducklake", Some("/data/")),
            "INSTALL ducklake; LOAD ducklake; ATTACH IF NOT EXISTS 'ducklake:it''s.ducklake' AS \"lake\" (DATA_PATH '/data/', OVERRIDE_DATA_PATH true);"
        );
    }

    #[test]
    fn connections_to_one_file_share_the_instance() {
        let dir = tempfile::tempdir().unwrap();
        let config = DbConfig::DuckDB {
            path: dir.path().join("shared.duckdb"),
            ducklake_catalog: None,
            ducklake_data_path: None,
            init_sql: Some("CREATE TABLE IF NOT EXISTS t AS SELECT 42 AS answer".into()),
        };
        let driver = DuckDbDriver::new();
        let first = driver.connect(&profile(config.clone())).unwrap();
        // A second open of the same file would fail on DuckDB's file lock.
        let second = driver.connect(&profile(config)).unwrap();

        assert_eq!(
            scalar(second.as_ref(), "SELECT answer FROM t"),
            Value::Int(42)
        );
        drop(first);
        assert_eq!(
            scalar(second.as_ref(), "SELECT count(*) FROM t"),
            Value::Int(1)
        );
    }

    #[test]
    fn a_file_open_with_other_startup_settings_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let config = |init_sql: &str| DbConfig::DuckDB {
            path: dir.path().join("shared.duckdb"),
            ducklake_catalog: None,
            ducklake_data_path: None,
            init_sql: Some(init_sql.to_string()),
        };
        let driver = DuckDbDriver::new();
        let _first = driver.connect(&profile(config("SET threads = 2"))).unwrap();

        assert!(driver.connect(&profile(config("SET threads = 3"))).is_err());
        assert!(driver.connect(&profile(config("SET threads = 2"))).is_ok());
    }

    #[test]
    fn relative_and_absolute_paths_share_the_instance() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let absolute = dir.path().canonicalize().unwrap().join("a.duckdb");
        let relative = absolute
            .strip_prefix(std::env::current_dir().unwrap().canonicalize().unwrap())
            .unwrap()
            .to_path_buf();
        let config = |path: PathBuf| DbConfig::DuckDB {
            path,
            ducklake_catalog: None,
            ducklake_data_path: None,
            init_sql: None,
        };
        let driver = DuckDbDriver::new();
        let _first = driver.connect(&profile(config(absolute))).unwrap();
        assert!(driver.connect(&profile(config(relative))).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn a_new_file_keeps_its_instance_path_once_created() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let path = link.join("a.duckdb");

        let before = instance_path(&path);
        std::fs::write(&path, b"").unwrap();
        assert_eq!(before, instance_path(&path));
    }

    #[test]
    fn in_memory_profiles_are_isolated_but_shared_per_profile() {
        let driver = DuckDbDriver::new();
        let first_profile = profile(DbConfig::default_duckdb());
        let first = driver.connect(&first_profile).unwrap();
        first
            .execute(&QueryRequest::new("CREATE TABLE t (id INTEGER)"))
            .unwrap();

        let same = driver.connect(&first_profile).unwrap();
        assert_eq!(
            scalar(same.as_ref(), "SELECT count(*) FROM t"),
            Value::Int(0)
        );

        let other = driver
            .connect(&profile(DbConfig::default_duckdb()))
            .unwrap();
        assert!(
            other
                .execute(&QueryRequest::new("SELECT * FROM t"))
                .is_err()
        );
    }

    /// Downloads the `ducklake` extension, so it needs network access.
    #[test]
    #[ignore = "installs the ducklake extension over the network"]
    fn attaches_a_ducklake_catalog_as_the_default_database() {
        let dir = tempfile::tempdir().unwrap();
        let config = DbConfig::DuckDB {
            path: PathBuf::new(),
            ducklake_catalog: Some(dir.path().join("meta.ducklake").display().to_string()),
            ducklake_data_path: Some(format!("{}/", dir.path().join("data").display())),
            init_sql: None,
        };
        let connection = DuckDbDriver::new().connect(&profile(config)).unwrap();

        assert_eq!(
            connection.active_database().as_deref(),
            Some(DUCKLAKE_ALIAS)
        );
        connection
            .execute(&QueryRequest::new(
                "CREATE TABLE events AS SELECT range AS id FROM range(5)",
            ))
            .unwrap();
        assert_eq!(
            scalar(connection.as_ref(), "SELECT count(*) FROM events"),
            Value::Int(5)
        );

        let schema = connection.schema_for_database(DUCKLAKE_ALIAS).unwrap();
        assert_eq!(schema.tables[0].name, "events");
    }

    /// Downloads the `ducklake` extension, so it needs network access.
    #[test]
    #[ignore = "installs the ducklake extension over the network"]
    fn profile_data_path_overrides_the_path_stored_in_the_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = dir.path().join("meta.ducklake").display().to_string();
        let attach = |data_path: &str| {
            let config = DbConfig::DuckDB {
                path: PathBuf::new(),
                ducklake_catalog: Some(catalog.clone()),
                ducklake_data_path: Some(data_path.to_string()),
                init_sql: None,
            };
            DuckDbDriver::new().connect(&profile(config))
        };

        let first = attach(&format!("{}/", dir.path().join("first").display())).unwrap();
        first
            .execute(&QueryRequest::new("CREATE TABLE t AS SELECT 1 AS id"))
            .unwrap();
        drop(first);

        let second = attach(&format!("{}/", dir.path().join("second").display())).unwrap();
        assert_eq!(
            scalar(second.as_ref(), "SELECT count(*) FROM t"),
            Value::Int(1)
        );
    }
}
