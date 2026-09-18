//! Driver registration surface: metadata, connection form, and connect.

use std::sync::LazyLock;

use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    Connection, ConnectionProfile, DatabaseCategory, DbConfig, DbDriver, DbError, DbKind,
    DdlCapabilities, DeploymentClass, DriverCapabilities, DriverFormDef, DriverLimits,
    DriverMetadata, FormFieldKind, FormSection, FormTab, FormValues, Icon, IsolationLevel,
    MutationCapabilities, OrderByMode, PaginationStyle, PlaceholderStyle, QueryCapabilities,
    QueryLanguage, SyntaxInfo, TransactionCapabilities, TransferFamily, WhereOperator, field,
    field_required,
};
use turso_serverless::Builder;

use crate::connection::TursoConnection;

pub static TURSO_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![FormTab {
        id: "main".into(),
        label: "Main".into(),
        sections: vec![FormSection {
            title: "Connection".into(),
            fields: vec![
                field_required(
                    "url",
                    "URL",
                    FormFieldKind::Text,
                    "libsql://your-database.turso.io",
                ),
                field("password", "Auth Token", FormFieldKind::Password, ""),
            ],
        }],
    }],
});

pub static METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "turso".into(),
    display_name: "TursoDB".into(),
    description: "Remote Turso / libSQL database over HTTP".into(),
    category: DatabaseCategory::Relational,
    transfer_family: TransferFamily::Sql,
    deployment_class: Some(DeploymentClass::CloudManaged),
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::AUTHENTICATION.bits()
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
            | DriverCapabilities::PAGINATION.bits()
            | DriverCapabilities::SORTING.bits()
            | DriverCapabilities::FILTERING.bits()
            | DriverCapabilities::EXPORT_CSV.bits()
            | DriverCapabilities::EXPORT_JSON.bits()
            | DriverCapabilities::TRANSACTIONAL_DDL.bits()
            | DriverCapabilities::MULTI_STATEMENT.bits()
            | DriverCapabilities::BULK_INSERT.bits(),
    ),
    default_port: None,
    uri_scheme: "libsql".into(),
    icon: Icon::Turso,
    syntax: Some(SyntaxInfo {
        identifier_quote: '"',
        string_quote: '\'',
        placeholder_style: PlaceholderStyle::QuestionMark,
        supports_schemas: false,
        default_schema: None,
        case_sensitive_identifiers: true,
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
            WhereOperator::Null,
            WhereOperator::In,
            WhereOperator::NotIn,
            WhereOperator::Contains,
            WhereOperator::Overlap,
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
        max_query_parameters: 32766,
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
        supports_alter_table: false,
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
        supports_alter_column: false,
        supports_add_constraint: false,
        supports_drop_constraint: false,
    }),
    transactions: Some(TransactionCapabilities {
        supports_transactions: true,
        supported_isolation_levels: vec![IsolationLevel::ReadCommitted],
        default_isolation_level: Some(IsolationLevel::ReadCommitted),
        // SAVEPOINT / RELEASE / ROLLBACK TO are rejected by the editor's
        // transaction-control classifier before they reach the server.
        supports_savepoints: false,
        supports_nested_transactions: false,
        supports_read_only: true,
        supports_deferrable: true,
    }),
    limits: Some(DriverLimits {
        max_query_length: 1_000_000_000,
        max_parameters: 32766,
        max_result_rows: 0,
        max_connections: 0,
        max_nested_subqueries: 16,
        max_identifier_length: 100_000,
        max_columns: 32766,
        max_indexes_per_table: 64,
        max_bulk_insert_rows: 0,
    }),
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

#[derive(Default)]
pub struct TursoDriver;

impl TursoDriver {
    pub fn new() -> Self {
        Self
    }

    fn open(
        profile: &ConnectionProfile,
        token: Option<&SecretString>,
    ) -> Result<TursoConnection, DbError> {
        let DbConfig::Turso { url } = &profile.config else {
            return Err(DbError::InvalidProfile(
                "Profile is not a Turso configuration".into(),
            ));
        };
        let url = validate_url(url)?;

        let mut builder = Builder::new_remote(url);
        if let Some(token) = token {
            let token = token.expose_secret().trim();
            if !token.is_empty() {
                builder = builder.with_auth_token(token);
            }
        }

        TursoConnection::open_root(builder)
    }
}

impl DbDriver for TursoDriver {
    fn kind(&self) -> DbKind {
        DbKind::Turso
    }

    fn metadata(&self) -> &DriverMetadata {
        &METADATA
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        "builtin:turso".into()
    }

    fn form_definition(&self) -> &DriverFormDef {
        &TURSO_FORM
    }

    fn secret_field_label(&self, _values: &FormValues) -> Option<String> {
        Some("Auth Token".to_string())
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let url = values
            .get("url")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("Database URL is required".into()))?;
        validate_url(url)?;
        Ok(DbConfig::Turso {
            url: url.to_string(),
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let mut values = FormValues::new();
        if let DbConfig::Turso { url } = config {
            values.insert("url".to_string(), url.clone());
        }
        values
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&SecretString>,
        _ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let connection = Self::open(profile, password)?;
        connection.ping()?;
        Ok(Box::new(connection))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let mut connection = Self::open(profile, None)?;
        let ping = connection.ping();
        let close = connection.close();
        ping.and(close)
    }
}

/// Accepts `libsql://`, `turso://`, `https://` and plain `http://` (local
/// `sqld`) URLs with a host and optional port. Credentials, query strings and
/// fragments are rejected so a token can never be smuggled into the profile.
pub(crate) fn validate_url(url: &str) -> Result<&str, DbError> {
    let url = url.trim();
    let invalid = |reason: &str| DbError::InvalidProfile(format!("Invalid Turso URL: {reason}"));

    let Some((scheme, rest)) = url.split_once("://") else {
        return Err(invalid("expected a scheme such as libsql:// or https://"));
    };
    if !matches!(
        scheme.to_ascii_lowercase().as_str(),
        "libsql" | "turso" | "https" | "http"
    ) {
        return Err(invalid("scheme must be libsql, turso, https or http"));
    }

    let authority = rest.split('/').next().unwrap_or_default();
    if authority.is_empty() {
        return Err(invalid("host is missing"));
    }
    if authority.contains('@') {
        return Err(invalid(
            "credentials are not allowed in the URL; use the Auth Token field",
        ));
    }
    if rest.contains('?') || rest.contains('#') {
        return Err(invalid("query strings and fragments are not allowed"));
    }
    if url.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(invalid("URL contains whitespace or control characters"));
    }

    let path = rest.get(authority.len()..).unwrap_or_default();
    if !path.is_empty() && path != "/" {
        return Err(invalid(
            "a path is not allowed; point at the database host only",
        ));
    }

    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_validation_accepts_remote_and_local_endpoints() {
        assert!(validate_url("libsql://db-org.turso.io").is_ok());
        assert!(validate_url("turso://db-org.turso.io/").is_ok());
        assert!(validate_url("https://db-org.turso.io").is_ok());
        assert!(validate_url("http://127.0.0.1:8080").is_ok());
    }

    #[test]
    fn url_validation_rejects_unsafe_shapes() {
        assert!(validate_url("").is_err());
        assert!(validate_url("db.turso.io").is_err());
        assert!(validate_url("ftp://db.turso.io").is_err());
        assert!(validate_url("libsql://user:pw@db.turso.io").is_err());
        assert!(validate_url("libsql://db.turso.io?authToken=x").is_err());
        assert!(validate_url("libsql://db.turso.io#frag").is_err());
        assert!(validate_url("libsql://db.turso.io/v2/pipeline").is_err());
        assert!(validate_url("libsql://db.tur so.io").is_err());
    }

    #[test]
    fn form_round_trips_the_url_only() {
        let driver = TursoDriver::new();
        let mut values = FormValues::new();
        values.insert("url".into(), " libsql://db.turso.io ".into());
        values.insert("password".into(), "secret".into());

        let config = driver.build_config(&values).unwrap();
        assert!(matches!(&config, DbConfig::Turso { url } if url == "libsql://db.turso.io"));

        let extracted = driver.extract_values(&config);
        assert_eq!(
            extracted.get("url").map(String::as_str),
            Some("libsql://db.turso.io")
        );
        assert!(!extracted.contains_key("password"));

        assert!(driver.build_config(&FormValues::new()).is_err());
    }

    #[test]
    fn metadata_contract() {
        let driver = TursoDriver::new();
        assert_eq!(driver.kind(), DbKind::Turso);
        assert_eq!(driver.metadata().id, "turso");
        assert_eq!(driver.driver_key(), "builtin:turso");
        assert!(driver.requires_password());
        assert_eq!(
            driver.secret_field_label(&FormValues::new()).as_deref(),
            Some("Auth Token")
        );
        assert!(driver.supports(DriverCapabilities::TRANSACTIONS));
        assert!(!driver.supports(DriverCapabilities::QUERY_CANCELLATION));
        assert!(!driver.supports(DriverCapabilities::MULTIPLE_DATABASES));
        assert!(!driver.supports(DriverCapabilities::SSH_TUNNEL));
    }
}
