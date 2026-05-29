//! InfluxDB driver implementation.
//!
//! Registers the driver with metadata and implements the `DbDriver` trait.

use std::collections::HashMap;
use std::sync::LazyLock;

use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    Connection, ConnectionProfile, DatabaseCategory, DbConfig, DbDriver, DbError, DbKind,
    DriverCapabilities, DriverFormDef, DriverKey, DriverMetadata, FormFieldKind, FormSection,
    FormTab, FormValues, Icon, InfluxVersion, QueryLanguage, field, field_required, when_checked,
    when_unchecked, with_default, with_help,
};

use crate::connection::InfluxConnection;
use crate::http::{AuthCreds, HttpClient};

// ---------------------------------------------------------------------------
// Static metadata
// ---------------------------------------------------------------------------

/// Static metadata for the InfluxDB driver.
pub static INFLUXDB_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "influxdb".into(),
    display_name: "InfluxDB".into(),
    description: "InfluxDB v1 and v2 time-series database with InfluxQL and Flux query support"
        .into(),
    category: DatabaseCategory::TimeSeries,
    deployment_class: None,
    query_language: QueryLanguage::InfluxQuery,
    capabilities: DriverCapabilities::AUTHENTICATION
        | DriverCapabilities::MULTIPLE_DATABASES
        | DriverCapabilities::PAGINATION
        | DriverCapabilities::EXPORT_CSV
        | DriverCapabilities::EXPORT_JSON
        | DriverCapabilities::CHART_AUTHORING,
    default_port: Some(8086),
    uri_scheme: "http".into(),
    icon: Icon::Influxdb,
    syntax: None,
    query: None,
    mutation: None,
    ddl: None,
    transactions: None,
    limits: None,
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
});

pub static INFLUXDB_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![FormTab {
        id: "main".into(),
        label: "Main".into(),
        sections: vec![
            FormSection {
                title: "Version".into(),
                fields: vec![with_help(
                    with_default(
                        field(
                            "use_v2",
                            "Use InfluxDB v2 (token auth / Flux)",
                            FormFieldKind::Checkbox,
                            "",
                        ),
                        "true",
                    ),
                    "Enable for InfluxDB v2+ (token-based auth). Disable for InfluxDB v1 (username/password).",
                )],
            },
            FormSection {
                title: "Connection".into(),
                fields: vec![
                    field_required("url", "URL", FormFieldKind::Text, "http://localhost:8086"),
                    when_checked(
                        field("org", "Organization", FormFieldKind::Text, "my-org"),
                        "use_v2",
                    ),
                    when_checked(
                        with_help(
                            field(
                                "bucket",
                                "Default bucket (optional)",
                                FormFieldKind::Text,
                                "my-bucket",
                            ),
                            "Pre-selects a bucket in the query editor. Leave blank to choose per-query from the dropdown.",
                        ),
                        "use_v2",
                    ),
                    when_unchecked(
                        with_help(
                            field(
                                "database",
                                "Default database (optional)",
                                FormFieldKind::Text,
                                "mydb",
                            ),
                            "Pre-selects a database in the query editor. Leave blank to choose per-query from the dropdown.",
                        ),
                        "use_v2",
                    ),
                    when_unchecked(
                        field(
                            "retention_policy",
                            "Retention Policy",
                            FormFieldKind::Text,
                            "autogen",
                        ),
                        "use_v2",
                    ),
                ],
            },
            FormSection {
                title: "Authentication".into(),
                fields: vec![when_unchecked(
                    field("user", "User", FormFieldKind::Text, "optional"),
                    "use_v2",
                )],
            },
        ],
    }],
});

// ---------------------------------------------------------------------------
// Driver struct
// ---------------------------------------------------------------------------

pub struct InfluxDriver;

impl InfluxDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for InfluxDriver {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// DbDriver implementation
// ---------------------------------------------------------------------------

impl DbDriver for InfluxDriver {
    fn kind(&self) -> DbKind {
        DbKind::InfluxDB
    }

    fn metadata(&self) -> &DriverMetadata {
        &INFLUXDB_METADATA
    }

    fn form_definition(&self) -> &DriverFormDef {
        &INFLUXDB_FORM
    }

    fn driver_key(&self) -> DriverKey {
        "builtin:influxdb".into()
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let url = values
            .get("url")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("URL is required".to_string()))?
            .to_string();

        let use_v2 = values
            .get("use_v2")
            .map(|s| s.trim())
            .map(|s| s == "true" || s == "1")
            .unwrap_or(true);

        if use_v2 {
            // Bucket is now optional: empty means "no default, choose per-query".
            let bucket = values
                .get("bucket")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());

            let org = values
                .get("org")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());

            Ok(DbConfig::InfluxDB {
                version: InfluxVersion::V2,
                url,
                org,
                default_bucket: bucket,
                retention_policy: None,
                user: None,
                request_timeout_seconds: None,
            })
        } else {
            // Database is now optional: empty means "no default, choose per-query".
            let database = values
                .get("database")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());

            let retention_policy = values
                .get("retention_policy")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());

            let user = values
                .get("user")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());

            Ok(DbConfig::InfluxDB {
                version: InfluxVersion::V1,
                url,
                org: None,
                default_bucket: database,
                retention_policy,
                user,
                request_timeout_seconds: None,
            })
        }
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let DbConfig::InfluxDB {
            version,
            url,
            org,
            default_bucket,
            retention_policy,
            user,
            ..
        } = config
        else {
            return HashMap::new();
        };

        let mut values = HashMap::new();
        values.insert("url".to_string(), url.clone());

        match version {
            InfluxVersion::V2 => {
                values.insert("use_v2".to_string(), "true".to_string());
                if let Some(bucket) = default_bucket {
                    values.insert("bucket".to_string(), bucket.clone());
                }
                if let Some(org) = org {
                    values.insert("org".to_string(), org.clone());
                }
            }
            InfluxVersion::V1 => {
                values.insert("use_v2".to_string(), "false".to_string());
                if let Some(db) = default_bucket {
                    values.insert("database".to_string(), db.clone());
                }
                if let Some(rp) = retention_policy {
                    values.insert("retention_policy".to_string(), rp.clone());
                }
                if let Some(u) = user {
                    values.insert("user".to_string(), u.clone());
                }
            }
        }

        values
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&SecretString>,
        _ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let DbConfig::InfluxDB {
            version,
            url,
            org,
            default_bucket,
            user,
            ..
        } = &profile.config
        else {
            return Err(DbError::InvalidProfile(
                "Expected InfluxDB configuration".to_string(),
            ));
        };

        let auth = build_auth_creds(user.as_deref(), *version, password)?;
        let default_language = default_language_for_version(*version);

        let http = HttpClient::new(url.clone(), auth, *version)
            .map_err(|e| DbError::connection_failed(e.to_string()))?;

        Ok(Box::new(InfluxConnection::new(
            http,
            *version,
            default_language,
            default_bucket.clone(),
            org.clone(),
        )))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let conn = self.connect_with_secrets(profile, None, None)?;
        conn.ping()
    }

    fn secret_field_label(&self, values: &FormValues) -> Option<String> {
        // v2 carries an API token in the secret field; v1 carries a real password.
        let use_v2 = values
            .get("use_v2")
            .map(|s| s.as_str())
            .map(|s| s == "true" || s == "1")
            .unwrap_or(true);

        Some(if use_v2 {
            "API Token".to_string()
        } else {
            "Password".to_string()
        })
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Build the correct `AuthCreds` for the given version, optional username (v1 only),
/// and optional password/token secret.
fn build_auth_creds(
    user: Option<&str>,
    version: InfluxVersion,
    password: Option<&SecretString>,
) -> Result<AuthCreds, DbError> {
    use dbflux_core::secrecy::ExposeSecret;

    match version {
        InfluxVersion::V2 => {
            // For v2, the "password" field carries the API token.
            if let Some(token) = password {
                let token_str = token.expose_secret().to_string();
                if !token_str.is_empty() {
                    return Ok(AuthCreds::Token(token_str));
                }
            }

            Ok(AuthCreds::None)
        }
        InfluxVersion::V1 => {
            // For v1, both username and password are needed for HTTP Basic auth.
            // Anonymous (no creds) is supported for instances without auth enabled.
            let pw = password
                .map(|p| p.expose_secret().to_string())
                .unwrap_or_default();
            let user_str = user.unwrap_or("").to_string();

            if user_str.is_empty() && pw.is_empty() {
                Ok(AuthCreds::None)
            } else {
                Ok(AuthCreds::Basic {
                    user: user_str,
                    password: pw,
                })
            }
        }
    }
}

/// Default query language for a given InfluxDB version.
///
/// V1 only supports InfluxQL. V2 defaults to InfluxQL for backwards compatibility
/// (users can switch to Flux via the query mode control in the source context).
fn default_language_for_version(version: InfluxVersion) -> QueryLanguage {
    match version {
        InfluxVersion::V1 => QueryLanguage::InfluxQuery,
        InfluxVersion::V2 => QueryLanguage::InfluxQuery,
    }
}

// ---------------------------------------------------------------------------
// Tests (C.9.1 – C.9.3)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{DbConfig, DbDriver, InfluxVersion};

    // C.9.1
    #[test]
    fn influxdb_metadata_category_is_time_series() {
        assert_eq!(INFLUXDB_METADATA.category, DatabaseCategory::TimeSeries);
    }

    #[test]
    fn influxdb_metadata_capabilities_include_expected_flags() {
        assert!(
            INFLUXDB_METADATA
                .capabilities
                .contains(DriverCapabilities::AUTHENTICATION)
        );
        assert!(
            INFLUXDB_METADATA
                .capabilities
                .contains(DriverCapabilities::MULTIPLE_DATABASES)
        );
        assert!(
            INFLUXDB_METADATA
                .capabilities
                .contains(DriverCapabilities::PAGINATION)
        );
        assert!(
            INFLUXDB_METADATA
                .capabilities
                .contains(DriverCapabilities::EXPORT_CSV)
        );
        assert!(
            INFLUXDB_METADATA
                .capabilities
                .contains(DriverCapabilities::EXPORT_JSON)
        );
    }

    #[test]
    fn influxdb_metadata_advertises_chart_authoring() {
        assert!(
            INFLUXDB_METADATA
                .capabilities
                .contains(DriverCapabilities::CHART_AUTHORING),
            "CHART_AUTHORING must be advertised so the sidebar surfaces Dashboards / Saved Charts folders for Influx connections"
        );
    }

    #[test]
    fn influxdb_metadata_has_influxdb_icon() {
        assert_eq!(INFLUXDB_METADATA.icon, Icon::Influxdb);
    }

    // C.9.2 — build_config v2 with explicit bucket
    #[test]
    fn build_config_v2_produces_correct_config() {
        let driver = InfluxDriver::new();
        let mut values = HashMap::new();
        values.insert("url".to_string(), "http://localhost:8086".to_string());
        values.insert("use_v2".to_string(), "true".to_string());
        values.insert("bucket".to_string(), "my-bucket".to_string());
        values.insert("org".to_string(), "my-org".to_string());

        let config = driver.build_config(&values).expect("v2 config must build");
        let DbConfig::InfluxDB {
            version,
            url,
            org,
            default_bucket,
            ..
        } = config
        else {
            panic!("expected InfluxDB config");
        };

        assert_eq!(version, InfluxVersion::V2);
        assert_eq!(url, "http://localhost:8086");
        assert_eq!(default_bucket.as_deref(), Some("my-bucket"));
        assert_eq!(org.as_deref(), Some("my-org"));
    }

    // C.9.2 — build_config v2 without bucket → default_bucket is None
    #[test]
    fn build_config_v2_no_bucket_sets_default_bucket_none() {
        let driver = InfluxDriver::new();
        let mut values = HashMap::new();
        values.insert("url".to_string(), "http://localhost:8086".to_string());
        values.insert("use_v2".to_string(), "true".to_string());
        // No "bucket" key → bucket field is empty/absent.

        let config = driver
            .build_config(&values)
            .expect("v2 config must build without bucket");
        let DbConfig::InfluxDB { default_bucket, .. } = config else {
            panic!("expected InfluxDB config");
        };

        assert!(
            default_bucket.is_none(),
            "build_config with no bucket must produce default_bucket=None"
        );
    }

    // C.9.2 — build_config v1 with explicit database
    #[test]
    fn build_config_v1_produces_correct_config() {
        let driver = InfluxDriver::new();
        let mut values = HashMap::new();
        values.insert("url".to_string(), "http://localhost:8086".to_string());
        values.insert("use_v2".to_string(), "false".to_string());
        values.insert("database".to_string(), "mydb".to_string());

        let config = driver.build_config(&values).expect("v1 config must build");
        let DbConfig::InfluxDB {
            version,
            default_bucket,
            org,
            ..
        } = config
        else {
            panic!("expected InfluxDB config");
        };

        assert_eq!(version, InfluxVersion::V1);
        assert_eq!(default_bucket.as_deref(), Some("mydb"));
        assert!(org.is_none(), "v1 has no org");
    }

    // C.9.2 — build_config v1 without database → default_bucket is None
    #[test]
    fn build_config_v1_no_database_sets_default_bucket_none() {
        let driver = InfluxDriver::new();
        let mut values = HashMap::new();
        values.insert("url".to_string(), "http://localhost:8086".to_string());
        values.insert("use_v2".to_string(), "false".to_string());
        // No "database" key → omitted.

        let config = driver
            .build_config(&values)
            .expect("v1 config must build without database");
        let DbConfig::InfluxDB { default_bucket, .. } = config else {
            panic!("expected InfluxDB config");
        };

        assert!(
            default_bucket.is_none(),
            "build_config with no database must produce default_bucket=None"
        );
    }

    /// v1: when the form provides a username, it must be persisted into DbConfig
    /// so that the connection can perform HTTP Basic auth with both user and password.
    #[test]
    fn build_config_v1_persists_username() {
        let driver = InfluxDriver::new();
        let mut values = HashMap::new();
        values.insert("url".to_string(), "http://localhost:8086".to_string());
        values.insert("use_v2".to_string(), "false".to_string());
        values.insert("database".to_string(), "mydb".to_string());
        values.insert("user".to_string(), "admin".to_string());

        let config = driver.build_config(&values).expect("v1 config must build");
        let DbConfig::InfluxDB { user, .. } = config else {
            panic!("expected InfluxDB config");
        };

        assert_eq!(user.as_deref(), Some("admin"));
    }

    // C.9.3 — extract_values round-trips
    #[test]
    fn extract_values_round_trips_v2_config_with_bucket() {
        let driver = InfluxDriver::new();
        let config = DbConfig::InfluxDB {
            version: InfluxVersion::V2,
            url: "http://influx.example.com:8086".to_string(),
            org: Some("my-org".to_string()),
            default_bucket: Some("my-bucket".to_string()),
            retention_policy: None,
            user: None,
            request_timeout_seconds: None,
        };

        let values = driver.extract_values(&config);
        assert_eq!(
            values.get("url").map(|s| s.as_str()),
            Some("http://influx.example.com:8086")
        );
        assert_eq!(values.get("use_v2").map(|s| s.as_str()), Some("true"));
        assert_eq!(values.get("bucket").map(|s| s.as_str()), Some("my-bucket"));
        assert_eq!(values.get("org").map(|s| s.as_str()), Some("my-org"));
    }

    // C.9.3 — v2 config with no default bucket should not emit a "bucket" key
    #[test]
    fn extract_values_v2_no_bucket_omits_bucket_key() {
        let driver = InfluxDriver::new();
        let config = DbConfig::InfluxDB {
            version: InfluxVersion::V2,
            url: "http://influx.example.com:8086".to_string(),
            org: Some("my-org".to_string()),
            default_bucket: None,
            retention_policy: None,
            user: None,
            request_timeout_seconds: None,
        };

        let values = driver.extract_values(&config);
        assert_eq!(values.get("use_v2").map(|s| s.as_str()), Some("true"));
        assert!(
            !values.contains_key("bucket"),
            "bucket key must be absent when default_bucket is None"
        );
    }

    #[test]
    fn extract_values_round_trips_v1_config() {
        let driver = InfluxDriver::new();
        let config = DbConfig::InfluxDB {
            version: InfluxVersion::V1,
            url: "http://influx.example.com:8086".to_string(),
            org: None,
            default_bucket: Some("testdb".to_string()),
            retention_policy: Some("autogen".to_string()),
            user: Some("admin".to_string()),
            request_timeout_seconds: None,
        };

        let values = driver.extract_values(&config);
        assert_eq!(values.get("use_v2").map(|s| s.as_str()), Some("false"));
        assert_eq!(values.get("database").map(|s| s.as_str()), Some("testdb"));
        assert_eq!(
            values.get("retention_policy").map(|s| s.as_str()),
            Some("autogen")
        );
        assert_eq!(values.get("user").map(|s| s.as_str()), Some("admin"));
    }

    #[test]
    fn influxdb_form_v2_fields_are_gated_on_use_v2_checkbox() {
        let url_field = INFLUXDB_FORM.field("url").expect("url field must exist");
        assert!(url_field.required, "url must be required");
        assert!(
            url_field.enabled_when_checked.is_none() && url_field.enabled_when_unchecked.is_none(),
            "url must not be version-gated"
        );

        for v2_field_id in &["org", "bucket"] {
            let field = INFLUXDB_FORM
                .field(v2_field_id)
                .unwrap_or_else(|| panic!("field '{}' must exist in INFLUXDB_FORM", v2_field_id));

            assert_eq!(
                field.enabled_when_checked.as_deref(),
                Some("use_v2"),
                "field '{}' must be visible only when use_v2 is checked",
                v2_field_id
            );
        }

        for v1_field_id in &["database", "retention_policy", "user"] {
            let field = INFLUXDB_FORM
                .field(v1_field_id)
                .unwrap_or_else(|| panic!("field '{}' must exist in INFLUXDB_FORM", v1_field_id));

            assert_eq!(
                field.enabled_when_unchecked.as_deref(),
                Some("use_v2"),
                "field '{}' must be visible only when use_v2 is unchecked",
                v1_field_id
            );
        }

        for delegated_id in &["token", "password"] {
            assert!(
                INFLUXDB_FORM.field(delegated_id).is_none(),
                "field '{}' should NOT live in INFLUXDB_FORM (handled by the generic password section)",
                delegated_id
            );
        }
    }

    #[test]
    fn influxdb_form_bucket_is_optional_in_v2_mode() {
        let bucket_field = INFLUXDB_FORM
            .field("bucket")
            .expect("bucket field must exist");
        assert!(
            !bucket_field.required,
            "bucket must be optional for v2 connections (users choose per-query)"
        );
        assert!(
            bucket_field.help.is_some(),
            "bucket field must have help text explaining it pre-selects in the editor"
        );
    }

    #[test]
    fn influxdb_form_database_is_optional_in_v1_mode() {
        let db_field = INFLUXDB_FORM
            .field("database")
            .expect("database field must exist");
        assert!(
            !db_field.required,
            "database must be optional for v1 connections (users choose per-query)"
        );

        let rp_field = INFLUXDB_FORM
            .field("retention_policy")
            .expect("retention_policy field must exist");
        assert!(
            !rp_field.required,
            "retention_policy must be optional (v1 default is 'autogen')"
        );
    }

    #[test]
    fn influxdb_form_has_no_ssh_tab() {
        assert!(
            !INFLUXDB_FORM.supports_ssh(),
            "INFLUXDB_FORM must not include an SSH tab in Phase A"
        );
    }

    #[test]
    fn influxdb_form_use_v2_defaults_to_true() {
        let use_v2 = INFLUXDB_FORM
            .field("use_v2")
            .expect("use_v2 checkbox must exist");
        assert_eq!(
            use_v2.default_value, "true",
            "use_v2 must default to true (V2 is default)"
        );
    }

    #[test]
    fn driver_key_is_builtin_influxdb() {
        let driver = InfluxDriver::new();
        assert_eq!(driver.driver_key(), "builtin:influxdb");
    }

    // build_auth_creds — v1 Basic auth with username + password
    #[test]
    fn build_auth_creds_v1_basic_with_user_and_password() {
        let pw = dbflux_core::secrecy::SecretString::new("s3cret".into());
        let creds =
            build_auth_creds(Some("admin"), InfluxVersion::V1, Some(&pw)).expect("creds build");
        match creds {
            AuthCreds::Basic { user, password } => {
                assert_eq!(user, "admin");
                assert_eq!(password, "s3cret");
            }
            other => panic!("expected Basic auth, got {:?}", other),
        }
    }

    /// v1 without any credentials → anonymous (some InfluxDB v1 deployments allow it).
    #[test]
    fn build_auth_creds_v1_none_when_user_and_password_empty() {
        let creds = build_auth_creds(None, InfluxVersion::V1, None).expect("creds build");
        assert!(matches!(creds, AuthCreds::None));
    }

    /// v1 with only password (legacy single-secret setup) still produces Basic auth
    /// with empty username; InfluxDB may accept or reject depending on configuration.
    #[test]
    fn build_auth_creds_v1_basic_with_only_password() {
        let pw = dbflux_core::secrecy::SecretString::new("only-pw".into());
        let creds = build_auth_creds(None, InfluxVersion::V1, Some(&pw)).expect("creds build");
        match creds {
            AuthCreds::Basic { user, password } => {
                assert!(user.is_empty());
                assert_eq!(password, "only-pw");
            }
            other => panic!("expected Basic auth, got {:?}", other),
        }
    }

    /// v2 token path: ensures user field is ignored for v2 (token-only auth).
    #[test]
    fn build_auth_creds_v2_uses_token_ignores_user() {
        let tok = dbflux_core::secrecy::SecretString::new("influx-token".into());
        let creds =
            build_auth_creds(Some("ignored"), InfluxVersion::V2, Some(&tok)).expect("creds build");
        match creds {
            AuthCreds::Token(t) => assert_eq!(t, "influx-token"),
            other => panic!("expected Token auth, got {:?}", other),
        }
    }
}
