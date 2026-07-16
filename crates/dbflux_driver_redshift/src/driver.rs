use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::Duration;

use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    Connection, ConnectionProfile, DatabaseCategory, DbConfig, DbDriver, DbError, DbKind,
    DeploymentClass, DriverCapabilities, DriverFormDef, DriverKey, DriverMetadata, FormFieldKind,
    FormSection, FormTab, FormValues, Icon, OrderByMode, PaginationStyle, PlaceholderStyle,
    QueryCapabilities, QueryLanguage, SshTunnelConfig, SyntaxInfo, TransferFamily, WhereOperator,
    field_password, field_required, field_use_uri, ssh_tab, when_checked, when_unchecked,
    with_default, with_help,
};
use dbflux_ssh::SshTunnel;
use postgres::Config;

use crate::connection::{
    RedshiftConnectParams, RedshiftConnection, RedshiftSslMode, RedshiftTlsCerts, connect_redshift,
    connect_with_ssl_mode,
};
use crate::error_formatter::format_redshift_uri_error;

/// Default connect timeout applied to a URI connection that does not carry its
/// own `connect_timeout` query parameter.
const DEFAULT_URI_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Amazon Redshift driver metadata.
///
/// Read-only v1: the capability set deliberately omits every write/mutation
/// flag (`INSERT`, `UPDATE`, `DELETE`, `RETURNING`, `BULK_INSERT`,
/// `TRUNCATE_TABLE`), DDL (`TRANSACTIONAL_DDL`, `ROUTINES`), and `INDEXES`
/// (Redshift has none — PK/FK/UNIQUE constraints are accepted but purely
/// informational). It does not reuse `DriverCapabilities::RELATIONAL_BASE`
/// because that constant bundles `INSERT | UPDATE | DELETE | INDEXES |
/// TRANSACTIONS`.
pub static METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "redshift".into(),
    display_name: "Amazon Redshift".into(),
    description: "AWS managed data warehouse, wire-compatible with PostgreSQL (read-only)".into(),
    category: DatabaseCategory::Relational,
    transfer_family: TransferFamily::Sql,
    deployment_class: Some(DeploymentClass::CloudManaged),
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::MULTIPLE_DATABASES.bits()
            | DriverCapabilities::SCHEMAS.bits()
            | DriverCapabilities::SSH_TUNNEL.bits()
            | DriverCapabilities::SSL.bits()
            | DriverCapabilities::AUTHENTICATION.bits()
            | DriverCapabilities::QUERY_CANCELLATION.bits()
            | DriverCapabilities::PREPARED_STATEMENTS.bits()
            | DriverCapabilities::VIEWS.bits()
            | DriverCapabilities::PAGINATION.bits()
            | DriverCapabilities::SORTING.bits()
            | DriverCapabilities::FILTERING.bits()
            | DriverCapabilities::EXPORT_CSV.bits()
            | DriverCapabilities::EXPORT_JSON.bits(),
    ),
    default_port: Some(5439),
    uri_scheme: "redshift".into(),
    icon: Icon::Redshift,
    syntax: Some(SyntaxInfo {
        identifier_quote: '"',
        string_quote: '\'',
        placeholder_style: PlaceholderStyle::DollarNumber,
        supports_schemas: true,
        default_schema: Some("public".to_string()),
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
        max_query_parameters: 32767,
        max_order_by_columns: 0,
        max_group_by_columns: 0,
    }),
    mutation: None,
    ddl: None,
    transactions: None,
    limits: None,
    ssl_modes: Some(&[
        dbflux_core::SslModeOption {
            id: "disable",
            label: "disable",
        },
        dbflux_core::SslModeOption {
            id: "allow",
            label: "allow",
        },
        dbflux_core::SslModeOption {
            id: "prefer",
            label: "prefer",
        },
        dbflux_core::SslModeOption {
            id: "require",
            label: "require",
        },
        dbflux_core::SslModeOption {
            id: "verify-ca",
            label: "verify-ca",
        },
        dbflux_core::SslModeOption {
            id: "verify-full",
            label: "verify-full",
        },
    ]),
    ssl_cert_fields: Some(dbflux_core::SslCertFields {
        root_cert: true,
        client_cert: true,
    }),
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

/// Amazon Redshift connection form.
///
/// Shape mirrors `dbflux_driver_postgres::POSTGRES_FORM` (same 12-field
/// `DbConfig::Redshift` variant), with Redshift-specific defaults (port
/// 5439, user `awsuser`, database `dev`).
pub static REDSHIFT_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![
        FormTab {
            id: "main".into(),
            label: "Main".into(),
            sections: vec![
                FormSection {
                    title: "Server".into(),
                    fields: vec![
                        field_use_uri(),
                        when_checked(
                            field_required(
                                "uri",
                                "Connection URI",
                                FormFieldKind::Text,
                                "redshift://user:pass@cluster.abc123.us-east-1.redshift.amazonaws.com:5439/dev",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            field_required(
                                "host",
                                "Host",
                                FormFieldKind::Text,
                                "cluster.abc123.us-east-1.redshift.amazonaws.com",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required("port", "Port", FormFieldKind::Number, "5439"),
                                "5439",
                            ),
                            "use_uri",
                        ),
                        when_unchecked(
                            with_default(
                                field_required("database", "Database", FormFieldKind::Text, "dev"),
                                "dev",
                            ),
                            "use_uri",
                        ),
                    ],
                },
                FormSection {
                    title: "Authentication".into(),
                    fields: vec![
                        when_unchecked(
                            with_default(
                                field_required("user", "User", FormFieldKind::Text, "awsuser"),
                                "awsuser",
                            ),
                            "use_uri",
                        ),
                        with_help(
                            field_password(),
                            "via Auth Profile · resolved at runtime, never persisted on disk",
                        ),
                    ],
                },
            ],
        },
        ssh_tab(),
    ],
});

pub struct RedshiftDriver;

impl RedshiftDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RedshiftDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DbDriver for RedshiftDriver {
    fn kind(&self) -> DbKind {
        DbKind::Redshift
    }

    fn metadata(&self) -> &DriverMetadata {
        &METADATA
    }

    fn driver_key(&self) -> DriverKey {
        "builtin:redshift".into()
    }

    fn form_definition(&self) -> &DriverFormDef {
        &REDSHIFT_FORM
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let use_uri = values.get("use_uri").map(|s| s == "true").unwrap_or(false);
        let uri = values.get("uri").filter(|s| !s.is_empty()).cloned();

        let ssl_root_cert_path = optional_form_value(values, "ssl_root_cert_path");
        let ssl_client_cert_path = optional_form_value(values, "ssl_client_cert_path");
        let ssl_client_key_path = optional_form_value(values, "ssl_client_key_path");

        if use_uri {
            if uri.is_none() {
                return Err(DbError::InvalidProfile(
                    "Connection URI is required when using URI mode".to_string(),
                ));
            }

            return Ok(DbConfig::Redshift {
                use_uri: true,
                uri,
                host: String::new(),
                port: 5439,
                user: String::new(),
                database: String::new(),
                ssl_mode: Some("prefer".to_string()),
                ssl_root_cert_path,
                ssl_client_cert_path,
                ssl_client_key_path,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            });
        }

        let host = values
            .get("host")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("Host is required".to_string()))?
            .clone();

        let port: u16 = values
            .get("port")
            .filter(|s| !s.is_empty())
            .map(String::as_str)
            .unwrap_or("5439")
            .parse()
            .map_err(|_| DbError::InvalidProfile("Invalid port number".to_string()))?;

        let user = values
            .get("user")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("User is required".to_string()))?
            .clone();

        let database = values
            .get("database")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("Database is required".to_string()))?
            .clone();

        Ok(DbConfig::Redshift {
            use_uri: false,
            uri: None,
            host,
            port,
            user,
            database,
            ssl_mode: Some("prefer".to_string()),
            ssl_root_cert_path,
            ssl_client_cert_path,
            ssl_client_key_path,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let mut values = HashMap::new();

        if let DbConfig::Redshift {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            ssl_root_cert_path,
            ssl_client_cert_path,
            ssl_client_key_path,
            ..
        } = config
        {
            values.insert(
                "use_uri".to_string(),
                if *use_uri { "true" } else { "" }.to_string(),
            );
            values.insert("uri".to_string(), uri.clone().unwrap_or_default());
            values.insert("host".to_string(), host.clone());
            values.insert("port".to_string(), port.to_string());
            values.insert("user".to_string(), user.clone());
            values.insert("database".to_string(), database.clone());

            if let Some(path) = ssl_root_cert_path {
                values.insert("ssl_root_cert_path".to_string(), path.clone());
            }
            if let Some(path) = ssl_client_cert_path {
                values.insert("ssl_client_cert_path".to_string(), path.clone());
            }
            if let Some(path) = ssl_client_key_path {
                values.insert("ssl_client_key_path".to_string(), path.clone());
            }
        }

        values
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&SecretString>,
        ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let config = extract_redshift_config(&profile.config)?;

        let password = password.map(|value| value.expose_secret());
        let ssh_secret = ssh_secret.map(|value| value.expose_secret());

        if config.use_uri {
            return self.connect_with_uri(
                config.uri.as_deref().unwrap_or(""),
                password,
                &config.tls_certs,
            );
        }

        if let Some(tunnel_config) = &config.ssh_tunnel {
            self.connect_via_ssh_tunnel(
                tunnel_config,
                ssh_secret,
                &config.host,
                config.port,
                &config.user,
                &config.database,
                password,
                &config.ssl_mode,
                &config.tls_certs,
            )
        } else {
            self.connect_direct(
                &config.host,
                config.port,
                &config.user,
                &config.database,
                password,
                &config.ssl_mode,
                &config.tls_certs,
            )
        }
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let connection = self.connect_with_secrets(profile, None, None)?;
        connection.ping()
    }
}

/// Returns the trimmed non-empty value for `key`, or `None` when it is absent
/// or blank. Used so a blank cert-path input never becomes `Some("")`.
fn optional_form_value(values: &FormValues, key: &str) -> Option<String> {
    values
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

struct ExtractedRedshiftConfig {
    use_uri: bool,
    uri: Option<String>,
    host: String,
    port: u16,
    user: String,
    database: String,
    /// Redshift native sslmode id (e.g. `"prefer"`, `"verify-ca"`). Defaults to `"prefer"` when absent.
    ssl_mode: String,
    /// Root CA / client-certificate paths honored when opening a TLS connection.
    tls_certs: RedshiftTlsCerts,
    ssh_tunnel: Option<SshTunnelConfig>,
}

fn extract_redshift_config(config: &DbConfig) -> Result<ExtractedRedshiftConfig, DbError> {
    match config {
        DbConfig::Redshift {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            ssl_mode,
            ssl_root_cert_path,
            ssl_client_cert_path,
            ssl_client_key_path,
            ssh_tunnel,
            ..
        } => Ok(ExtractedRedshiftConfig {
            use_uri: *use_uri,
            uri: uri.clone(),
            host: host.clone(),
            port: *port,
            user: user.clone(),
            database: database.clone(),
            ssl_mode: ssl_mode.clone().unwrap_or_else(|| "prefer".to_string()),
            tls_certs: RedshiftTlsCerts {
                root_cert_path: ssl_root_cert_path.clone(),
                client_cert_path: ssl_client_cert_path.clone(),
                client_key_path: ssl_client_key_path.clone(),
            },
            ssh_tunnel: ssh_tunnel.clone(),
        }),
        _ => Err(DbError::InvalidProfile(
            "Expected Redshift configuration".to_string(),
        )),
    }
}

/// Rewrites the `redshift://` scheme the connection form advertises into the
/// `postgresql://` scheme the underlying wire client's URI parser accepts.
/// URIs already using `postgres://`/`postgresql://` pass through unchanged.
fn normalize_redshift_uri_scheme(uri: &str) -> String {
    match uri.strip_prefix("redshift://") {
        Some(rest) => format!("postgresql://{rest}"),
        None => uri.to_string(),
    }
}

/// Resolves the effective [`RedshiftSslMode`] from a URI's `sslmode` query
/// parameter, defaulting to `prefer` when the parameter is absent.
fn redshift_uri_sslmode(uri: &str) -> RedshiftSslMode {
    let Some(query_start) = uri.find('?') else {
        return RedshiftSslMode::Prefer;
    };

    let query = &uri[query_start + 1..];

    query
        .split('&')
        .find_map(|pair| pair.split_once('=').filter(|(key, _)| *key == "sslmode"))
        .map(|(_, value)| RedshiftSslMode::parse(value))
        .unwrap_or(RedshiftSslMode::Prefer)
}

/// Removes the `sslmode` query parameter from a URI.
///
/// The wire client's URI parser only accepts `disable`/`prefer`/`require` and
/// rejects the libpq `allow`/`verify-ca`/`verify-full` values outright. This
/// driver resolves the ssl mode itself via [`redshift_uri_sslmode`] and sets it
/// on the parsed [`Config`], so the raw parameter is stripped before parsing to
/// keep the URI's ssl mode the single source of truth and to avoid a parse
/// error on the values libpq accepts but the wire client does not.
fn strip_sslmode_query_param(uri: &str) -> String {
    let Some(query_start) = uri.find('?') else {
        return uri.to_string();
    };

    let (base, query_with_marker) = uri.split_at(query_start);
    let query = &query_with_marker[1..];

    let retained: Vec<&str> = query
        .split('&')
        .filter(|pair| {
            let key = pair.split_once('=').map(|(key, _)| key).unwrap_or(pair);
            !key.eq_ignore_ascii_case("sslmode")
        })
        .collect();

    if retained.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{}", retained.join("&"))
    }
}

/// Builds the `postgres::Config` for a URI connection: resolves the ssl mode,
/// strips it from the URI so the wire parser accepts every libpq value, applies
/// the default connect timeout when the URI omits one, and reports the resolved
/// ssl mode so the connector policy can be applied uniformly.
fn build_uri_config(
    base_uri: &str,
    password: Option<&str>,
) -> Result<(Config, RedshiftSslMode), DbError> {
    let normalized = normalize_redshift_uri_scheme(base_uri);
    let uri = inject_password_into_uri(&normalized, password);

    let ssl_mode = redshift_uri_sslmode(&uri);
    let stripped = strip_sslmode_query_param(&uri);

    let mut config = stripped
        .parse::<Config>()
        .map_err(|e| format_redshift_uri_error(&e, base_uri))?;

    config.ssl_mode(ssl_mode.config_ssl_mode());

    if config.get_connect_timeout().is_none() {
        config.connect_timeout(DEFAULT_URI_CONNECT_TIMEOUT);
    }

    Ok((config, ssl_mode))
}

/// Injects `password` into a `postgresql://`/`postgres://` URI when the
/// credentials segment carries an empty password placeholder.
fn inject_password_into_uri(base_uri: &str, password: Option<&str>) -> String {
    let password = match password {
        Some(p) if !p.is_empty() => p,
        _ => return base_uri.to_string(),
    };

    if !base_uri.starts_with("postgresql://") && !base_uri.starts_with("postgres://") {
        return base_uri.to_string();
    }

    let prefix_end = if base_uri.starts_with("postgresql://") {
        13
    } else {
        11
    };

    let rest = &base_uri[prefix_end..];
    let prefix = &base_uri[..prefix_end];

    let Some(at_pos) = rest.find('@') else {
        return base_uri.to_string();
    };

    let user_pass = &rest[..at_pos];
    let after_at = &rest[at_pos..];

    let Some(colon_pos) = user_pass.find(':') else {
        let encoded_password = urlencoding::encode(password);
        return format!("{prefix}{user_pass}:{encoded_password}{after_at}");
    };

    if !user_pass[colon_pos + 1..].is_empty() {
        return base_uri.to_string();
    }

    let user = &user_pass[..colon_pos];
    let encoded_password = urlencoding::encode(password);
    format!("{prefix}{user}:{encoded_password}{after_at}")
}

impl RedshiftDriver {
    fn connect_with_uri(
        &self,
        base_uri: &str,
        password: Option<&str>,
        tls_certs: &RedshiftTlsCerts,
    ) -> Result<Box<dyn Connection>, DbError> {
        let (config, ssl_mode) = build_uri_config(base_uri, password)?;

        let client = connect_with_ssl_mode(&config, ssl_mode, tls_certs, |e| {
            format_redshift_uri_error(e, base_uri)
        })?;

        let cancel_token = client.cancel_token();

        Ok(Box::new(RedshiftConnection {
            client: Arc::new(Mutex::new(client)),
            ssh_tunnel: None,
            cancel_token,
            active_query: RwLock::new(None),
            cancelled: Arc::new(AtomicBool::new(false)),
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_direct(
        &self,
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        ssl_mode: &str,
        tls_certs: &RedshiftTlsCerts,
    ) -> Result<Box<dyn Connection>, DbError> {
        let client = connect_redshift(&RedshiftConnectParams {
            host,
            port,
            user,
            password: password.unwrap_or(""),
            database,
            ssl_mode,
            tls_certs,
        })?;

        let cancel_token = client.cancel_token();

        Ok(Box::new(RedshiftConnection {
            client: Arc::new(Mutex::new(client)),
            ssh_tunnel: None,
            cancel_token,
            active_query: RwLock::new(None),
            cancelled: Arc::new(AtomicBool::new(false)),
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_via_ssh_tunnel(
        &self,
        tunnel_config: &SshTunnelConfig,
        ssh_secret: Option<&str>,
        db_host: &str,
        db_port: u16,
        db_user: &str,
        database: &str,
        db_password: Option<&str>,
        ssl_mode: &str,
        tls_certs: &RedshiftTlsCerts,
    ) -> Result<Box<dyn Connection>, DbError> {
        let ssh_session = dbflux_ssh::establish_session(tunnel_config, ssh_secret)?;
        let tunnel = SshTunnel::start(ssh_session, db_host.to_string(), db_port)?;
        let local_port = tunnel.local_port();

        let client = connect_redshift(&RedshiftConnectParams {
            host: "127.0.0.1",
            port: local_port,
            user: db_user,
            password: db_password.unwrap_or(""),
            database,
            ssl_mode,
            tls_certs,
        })?;

        let cancel_token = client.cancel_token();

        Ok(Box::new(RedshiftConnection {
            client: Arc::new(Mutex::new(client)),
            ssh_tunnel: Some(tunnel),
            cancel_token,
            active_query: RwLock::new(None),
            cancelled: Arc::new(AtomicBool::new(false)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{METADATA, RedshiftDriver};
    use dbflux_core::{
        DatabaseCategory, DbConfig, DbDriver, DbError, DriverCapabilities, FormValues,
        QueryLanguage, TransferFamily,
    };

    #[test]
    fn metadata_declares_relational_sql_read_only_contract() {
        let metadata = &*METADATA;

        assert_eq!(metadata.category, DatabaseCategory::Relational);
        assert_eq!(metadata.transfer_family, TransferFamily::Sql);
        assert_eq!(metadata.query_language, QueryLanguage::Sql);
        assert_eq!(metadata.default_port, Some(5439));

        let excluded = [
            DriverCapabilities::INSERT,
            DriverCapabilities::UPDATE,
            DriverCapabilities::DELETE,
            DriverCapabilities::RETURNING,
            DriverCapabilities::TRANSACTIONAL_DDL,
            DriverCapabilities::TRUNCATE_TABLE,
            DriverCapabilities::BULK_INSERT,
            DriverCapabilities::INDEXES,
            DriverCapabilities::ROUTINES,
            DriverCapabilities::INSTANCE_METRICS,
            DriverCapabilities::INSTANCE_INSPECTOR,
        ];

        for capability in excluded {
            assert!(
                !metadata.capabilities.contains(capability),
                "capability {capability:?} must be absent from the read-only Redshift metadata"
            );
        }
    }

    #[test]
    fn form_definition_has_a_main_tab_and_ssh_tab() {
        let driver = RedshiftDriver::new();
        let form = driver.form_definition();

        assert!(!form.tabs.is_empty());
        assert!(form.tabs.iter().any(|tab| tab.id == "main"));
        assert!(form.tabs.iter().any(|tab| tab.id == "ssh"));
    }

    #[test]
    fn build_config_defaults_port_to_5439_when_absent() {
        let driver = RedshiftDriver::new();
        let mut values = FormValues::new();
        values.insert("host".to_string(), "cluster.example.com".to_string());
        values.insert("user".to_string(), "awsuser".to_string());
        values.insert("database".to_string(), "dev".to_string());

        let config = driver
            .build_config(&values)
            .expect("build_config should succeed with no port supplied");

        let DbConfig::Redshift { port, .. } = config else {
            panic!("expected DbConfig::Redshift");
        };
        assert_eq!(port, 5439);
    }

    #[test]
    fn build_config_requires_uri_when_uri_mode_is_enabled() {
        let driver = RedshiftDriver::new();
        let mut values = FormValues::new();
        values.insert("use_uri".to_string(), "true".to_string());

        let result = driver.build_config(&values);
        assert!(matches!(result, Err(DbError::InvalidProfile(_))));
    }

    #[test]
    fn build_config_validates_manual_fields() {
        let driver = RedshiftDriver::new();
        let mut values = FormValues::new();
        values.insert("host".to_string(), "cluster.example.com".to_string());
        values.insert("port".to_string(), "not-a-port".to_string());
        values.insert("user".to_string(), "awsuser".to_string());
        values.insert("database".to_string(), "dev".to_string());

        let result = driver.build_config(&values);
        assert!(matches!(result, Err(DbError::InvalidProfile(_))));
    }

    #[test]
    fn build_config_and_extract_values_round_trip_without_leaking_password() {
        let driver = RedshiftDriver::new();
        let mut values = FormValues::new();
        values.insert("host".to_string(), "cluster.example.com".to_string());
        values.insert("port".to_string(), "5440".to_string());
        values.insert("user".to_string(), "reporting".to_string());
        values.insert("database".to_string(), "analytics".to_string());

        let config = driver
            .build_config(&values)
            .expect("build_config should succeed");
        let round_tripped = driver.extract_values(&config);

        assert_eq!(
            round_tripped.get("host").map(String::as_str),
            Some("cluster.example.com")
        );
        assert_eq!(round_tripped.get("port").map(String::as_str), Some("5440"));
        assert_eq!(
            round_tripped.get("user").map(String::as_str),
            Some("reporting")
        );
        assert_eq!(
            round_tripped.get("database").map(String::as_str),
            Some("analytics")
        );
        assert!(
            !round_tripped.contains_key("password"),
            "extract_values must never surface the password field"
        );
        assert!(
            !format!("{config:?}").contains("password"),
            "DbConfig::Redshift Debug output must never contain a literal password field"
        );
    }

    #[test]
    fn build_config_carries_ssl_cert_paths_from_form_values() {
        let driver = RedshiftDriver::new();
        let mut values = FormValues::new();
        values.insert("host".to_string(), "cluster.example.com".to_string());
        values.insert("user".to_string(), "awsuser".to_string());
        values.insert("database".to_string(), "dev".to_string());
        values.insert(
            "ssl_root_cert_path".to_string(),
            "/etc/ssl/redshift-ca.pem".to_string(),
        );
        values.insert(
            "ssl_client_cert_path".to_string(),
            "/etc/ssl/client.pem".to_string(),
        );
        values.insert(
            "ssl_client_key_path".to_string(),
            "/etc/ssl/client-key.pem".to_string(),
        );

        let config = driver
            .build_config(&values)
            .expect("build_config should succeed");

        let DbConfig::Redshift {
            ssl_root_cert_path,
            ssl_client_cert_path,
            ssl_client_key_path,
            ..
        } = config
        else {
            panic!("expected DbConfig::Redshift");
        };

        assert_eq!(
            ssl_root_cert_path.as_deref(),
            Some("/etc/ssl/redshift-ca.pem")
        );
        assert_eq!(ssl_client_cert_path.as_deref(), Some("/etc/ssl/client.pem"));
        assert_eq!(
            ssl_client_key_path.as_deref(),
            Some("/etc/ssl/client-key.pem")
        );
    }

    #[test]
    fn blank_ssl_cert_form_values_become_none() {
        let driver = RedshiftDriver::new();
        let mut values = FormValues::new();
        values.insert("host".to_string(), "cluster.example.com".to_string());
        values.insert("user".to_string(), "awsuser".to_string());
        values.insert("database".to_string(), "dev".to_string());
        values.insert("ssl_root_cert_path".to_string(), "   ".to_string());
        values.insert("ssl_client_cert_path".to_string(), String::new());

        let config = driver
            .build_config(&values)
            .expect("build_config should succeed");

        let DbConfig::Redshift {
            ssl_root_cert_path,
            ssl_client_cert_path,
            ssl_client_key_path,
            ..
        } = config
        else {
            panic!("expected DbConfig::Redshift");
        };

        assert!(ssl_root_cert_path.is_none());
        assert!(ssl_client_cert_path.is_none());
        assert!(ssl_client_key_path.is_none());
    }

    #[test]
    fn build_config_and_extract_values_round_trip_ssl_cert_paths() {
        let driver = RedshiftDriver::new();
        let mut values = FormValues::new();
        values.insert("host".to_string(), "cluster.example.com".to_string());
        values.insert("user".to_string(), "awsuser".to_string());
        values.insert("database".to_string(), "dev".to_string());
        values.insert(
            "ssl_root_cert_path".to_string(),
            "/certs/ca.pem".to_string(),
        );
        values.insert(
            "ssl_client_cert_path".to_string(),
            "/certs/client.pem".to_string(),
        );
        values.insert(
            "ssl_client_key_path".to_string(),
            "/certs/client.key".to_string(),
        );

        let config = driver
            .build_config(&values)
            .expect("build_config should succeed");
        let round_tripped = driver.extract_values(&config);

        assert_eq!(
            round_tripped.get("ssl_root_cert_path").map(String::as_str),
            Some("/certs/ca.pem")
        );
        assert_eq!(
            round_tripped
                .get("ssl_client_cert_path")
                .map(String::as_str),
            Some("/certs/client.pem")
        );
        assert_eq!(
            round_tripped.get("ssl_client_key_path").map(String::as_str),
            Some("/certs/client.key")
        );
    }

    #[test]
    fn driver_key_and_kind_are_stable() {
        let driver = RedshiftDriver::new();
        assert_eq!(driver.driver_key(), "builtin:redshift");
        assert_eq!(driver.kind(), dbflux_core::DbKind::Redshift);
    }

    mod uri_helpers {
        use super::super::{
            RedshiftSslMode, build_uri_config, inject_password_into_uri,
            normalize_redshift_uri_scheme, redshift_uri_sslmode, strip_sslmode_query_param,
        };
        use std::time::Duration;

        #[test]
        fn normalize_rewrites_redshift_scheme_to_postgresql() {
            assert_eq!(
                normalize_redshift_uri_scheme("redshift://user:pass@cluster.example.com:5439/dev"),
                "postgresql://user:pass@cluster.example.com:5439/dev"
            );
        }

        #[test]
        fn normalize_leaves_postgresql_scheme_untouched() {
            let uri = "postgresql://user:pass@cluster.example.com:5439/dev";
            assert_eq!(normalize_redshift_uri_scheme(uri), uri);
        }

        #[test]
        fn parse_sslmode_defaults_to_prefer_when_absent() {
            assert_eq!(
                redshift_uri_sslmode("postgresql://cluster.example.com:5439/dev"),
                RedshiftSslMode::Prefer
            );
        }

        #[test]
        fn parse_sslmode_reads_query_parameter() {
            assert_eq!(
                redshift_uri_sslmode("postgresql://cluster.example.com:5439/dev?sslmode=require"),
                RedshiftSslMode::Require
            );
            assert_eq!(
                redshift_uri_sslmode("postgresql://cluster.example.com:5439/dev?sslmode=disable"),
                RedshiftSslMode::Disable
            );
            assert_eq!(
                redshift_uri_sslmode(
                    "postgresql://cluster.example.com:5439/dev?sslmode=verify-full"
                ),
                RedshiftSslMode::Verify
            );
        }

        #[test]
        fn strip_sslmode_removes_only_the_sslmode_parameter() {
            assert_eq!(
                strip_sslmode_query_param(
                    "postgresql://cluster.example.com:5439/dev?sslmode=verify-full"
                ),
                "postgresql://cluster.example.com:5439/dev"
            );
            assert_eq!(
                strip_sslmode_query_param(
                    "postgresql://cluster.example.com:5439/dev?sslmode=require&connect_timeout=5"
                ),
                "postgresql://cluster.example.com:5439/dev?connect_timeout=5"
            );
            assert_eq!(
                strip_sslmode_query_param(
                    "postgresql://cluster.example.com:5439/dev?application_name=dbflux"
                ),
                "postgresql://cluster.example.com:5439/dev?application_name=dbflux"
            );
        }

        #[test]
        fn build_uri_config_applies_default_connect_timeout_when_absent() {
            let (config, ssl_mode) =
                build_uri_config("postgresql://awsuser:pw@cluster.example.com:5439/dev", None)
                    .expect("URI should parse");

            assert_eq!(config.get_connect_timeout(), Some(&Duration::from_secs(30)));
            assert_eq!(ssl_mode, RedshiftSslMode::Prefer);
        }

        #[test]
        fn build_uri_config_preserves_explicit_connect_timeout() {
            let (config, _) = build_uri_config(
                "postgresql://awsuser:pw@cluster.example.com:5439/dev?connect_timeout=7",
                None,
            )
            .expect("URI should parse");

            assert_eq!(config.get_connect_timeout(), Some(&Duration::from_secs(7)));
        }

        #[test]
        fn build_uri_config_resolves_libpq_verify_mode_the_wire_parser_rejects() {
            // `verify-full` is a valid libpq value the wire client's own URI
            // parser rejects; stripping it keeps the URI parseable while the
            // resolved ssl mode still mandates certificate validation.
            let (_, ssl_mode) = build_uri_config(
                "postgresql://awsuser:pw@cluster.example.com:5439/dev?sslmode=verify-full",
                None,
            )
            .expect("URI should parse after sslmode is stripped");

            assert_eq!(ssl_mode, RedshiftSslMode::Verify);
        }

        #[test]
        fn inject_password_fills_empty_placeholder() {
            let uri = "postgresql://awsuser:@cluster.example.com:5439/dev";
            assert_eq!(
                inject_password_into_uri(uri, Some("secret")),
                "postgresql://awsuser:secret@cluster.example.com:5439/dev"
            );
        }

        #[test]
        fn inject_password_adds_missing_colon_segment() {
            let uri = "postgresql://awsuser@cluster.example.com:5439/dev";
            assert_eq!(
                inject_password_into_uri(uri, Some("secret")),
                "postgresql://awsuser:secret@cluster.example.com:5439/dev"
            );
        }

        #[test]
        fn inject_password_leaves_uri_unchanged_when_password_already_present() {
            let uri = "postgresql://awsuser:already-set@cluster.example.com:5439/dev";
            assert_eq!(inject_password_into_uri(uri, Some("secret")), uri);
        }

        #[test]
        fn inject_password_leaves_uri_unchanged_when_no_password_given() {
            let uri = "postgresql://awsuser:@cluster.example.com:5439/dev";
            assert_eq!(inject_password_into_uri(uri, None), uri);
        }
    }
}
