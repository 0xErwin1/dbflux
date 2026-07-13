use std::collections::HashMap;
use std::sync::LazyLock;

use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    Connection, ConnectionProfile, DatabaseCategory, DbConfig, DbDriver, DbError, DbKind,
    DeploymentClass, DriverCapabilities, DriverFormDef, DriverKey, DriverMetadata, FormFieldKind,
    FormSection, FormTab, FormValues, Icon, OrderByMode, PaginationStyle, PlaceholderStyle,
    QueryCapabilities, QueryLanguage, SyntaxInfo, TransferFamily, WhereOperator, field_password,
    field_required, field_use_uri, ssh_tab, when_checked, when_unchecked, with_default, with_help,
};

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
            | DriverCapabilities::QUERY_TIMEOUT.bits()
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
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
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
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
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
        }

        values
    }

    /// Establishing a live connection lands with the connection layer; this
    /// crate currently only exposes driver metadata and the connection form.
    fn connect_with_secrets(
        &self,
        _profile: &ConnectionProfile,
        _password: Option<&SecretString>,
        _ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        Err(DbError::NotSupported(
            "Redshift connections are not yet supported".to_string(),
        ))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        self.connect_with_secrets(profile, None, None).map(|_| ())
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
    fn driver_key_and_kind_are_stable() {
        let driver = RedshiftDriver::new();
        assert_eq!(driver.driver_key(), "builtin:redshift");
        assert_eq!(driver.kind(), dbflux_core::DbKind::Redshift);
    }
}
