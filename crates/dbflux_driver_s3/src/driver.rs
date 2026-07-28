use std::collections::HashMap;
use std::sync::LazyLock;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Credentials};
use chrono::{DateTime, TimeZone, Utc};
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    BucketCreateOptions, BucketCreateOutcome, BucketDetails, BucketInfo, BucketSizeEstimate,
    Connection, ConnectionExt, ConnectionProfile, DatabaseCategory, DbConfig, DbDriver, DbError,
    DbKind, DeploymentClass, DocumentConnection, DriverCapabilities, DriverFormDef, DriverMetadata,
    FormFieldKind, FormSection, FormTab, FormValues, Icon, KeyValueConnection, ObjectListingPage,
    ObjectMetadata, ObjectStoreConnection, ObjectSummary, ObjectVersionSummary, PresignMethod,
    QueryHandle, QueryLanguage, QueryRequest, QueryResult, RelationalConnection,
    SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TransferFamily, field, field_required,
};

use crate::error_formatter::{S3_ERROR_FORMATTER, classify_connection_error, classify_query_error};

pub static S3_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "s3".into(),
    display_name: "Amazon S3".into(),
    description: "AWS S3 and S3-compatible object storage (Cloudflare R2, MinIO)".into(),
    category: DatabaseCategory::ObjectStorage,
    transfer_family: TransferFamily::Incompatible,
    deployment_class: Some(DeploymentClass::CloudManaged),
    query_language: QueryLanguage::Custom("S3".into()),
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::AUTHENTICATION.bits()
            | DriverCapabilities::OBJECT_STORAGE.bits()
            | DriverCapabilities::OBJECT_PREFIX_DELETE.bits(),
    ),
    default_port: None,
    uri_scheme: "s3".into(),
    icon: Icon::S3,
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
});

pub static S3_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![FormTab {
        id: "main".into(),
        label: "Main".into(),
        sections: vec![
            FormSection {
                title: "AWS".into(),
                fields: vec![
                    field_required("region", "Region", FormFieldKind::Text, "us-east-1"),
                    field(
                        "profile",
                        "Profile",
                        FormFieldKind::AuthProfileRef { provider_id: None },
                        "",
                    ),
                    field(
                        "access_key_id",
                        "Access Key ID",
                        FormFieldKind::Text,
                        "optional — leave blank to use the profile above or the default AWS credential chain",
                    ),
                ],
            },
            FormSection {
                title: "Endpoint".into(),
                fields: vec![
                    field(
                        "endpoint",
                        "Endpoint Override",
                        FormFieldKind::Text,
                        "https://<account-id>.r2.cloudflarestorage.com",
                    ),
                    field(
                        "path_style",
                        "Force Path-Style Addressing",
                        FormFieldKind::Checkbox,
                        "",
                    ),
                ],
            },
        ],
    }],
});

pub struct S3Driver;

impl S3Driver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for S3Driver {
    fn default() -> Self {
        Self::new()
    }
}

/// Static AWS config for an S3 connection, resolved from `DbConfig::S3`.
///
/// Carries `access_key_id` (but never the secret key itself, which only ever
/// lives in the `SecretString` passed into `connect_with_secrets`) so error
/// formatting and the client builder can both reference the same profile
/// shape without re-destructuring `DbConfig`.
#[derive(Debug, Clone)]
pub(crate) struct S3ProfileConfig {
    pub(crate) region: String,
    pub(crate) profile: Option<String>,
    pub(crate) access_key_id: Option<String>,
    pub(crate) endpoint: Option<String>,
    pub(crate) path_style: bool,
}

impl S3ProfileConfig {
    /// Non-sensitive diagnostic summary attached to formatted errors —
    /// region and endpoint only, never credentials.
    pub(crate) fn diagnostic_detail(&self) -> String {
        match &self.endpoint {
            Some(endpoint) => format!("region={}, endpoint_override={endpoint}", self.region),
            None => format!("region={}", self.region),
        }
    }
}

impl DbDriver for S3Driver {
    fn kind(&self) -> DbKind {
        DbKind::S3
    }

    fn metadata(&self) -> &DriverMetadata {
        &S3_METADATA
    }

    fn form_definition(&self) -> &DriverFormDef {
        &S3_FORM
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        "builtin:s3".into()
    }

    fn secret_field_label(&self, _values: &FormValues) -> Option<String> {
        Some("Secret Access Key".to_string())
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let region = values
            .get("region")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("AWS Region is required".to_string()))?
            .to_string();

        let profile = trimmed_optional(values, "profile");
        let access_key_id = trimmed_optional(values, "access_key_id");
        let endpoint = trimmed_optional(values, "endpoint");
        let path_style = values
            .get("path_style")
            .map(|value| value == "true")
            .unwrap_or(false);

        Ok(DbConfig::S3 {
            region,
            profile,
            access_key_id,
            endpoint,
            path_style,
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let DbConfig::S3 {
            region,
            profile,
            access_key_id,
            endpoint,
            path_style,
        } = config
        else {
            return HashMap::new();
        };

        let mut values = HashMap::new();
        values.insert("region".to_string(), region.clone());
        values.insert("profile".to_string(), profile.clone().unwrap_or_default());
        values.insert(
            "access_key_id".to_string(),
            access_key_id.clone().unwrap_or_default(),
        );
        values.insert("endpoint".to_string(), endpoint.clone().unwrap_or_default());
        values.insert(
            "path_style".to_string(),
            if *path_style { "true" } else { "" }.to_string(),
        );

        values
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&SecretString>,
        _ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let config = profile_config(&profile.config)?;
        let client = build_client(&config, password)?;

        probe_connection(&client, &config)?;

        Ok(Box::new(S3Connection { client, config }))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let config = profile_config(&profile.config)?;
        let client = build_client(&config, None)?;

        probe_connection(&client, &config)
    }
}

fn trimmed_optional(values: &FormValues, id: &str) -> Option<String> {
    values
        .get(id)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn profile_config(config: &DbConfig) -> Result<S3ProfileConfig, DbError> {
    let DbConfig::S3 {
        region,
        profile,
        access_key_id,
        endpoint,
        path_style,
    } = config
    else {
        return Err(DbError::InvalidProfile(
            "Expected S3 configuration".to_string(),
        ));
    };

    let trimmed_region = region.trim();
    if trimmed_region.is_empty() {
        return Err(DbError::InvalidProfile(
            "AWS Region is required".to_string(),
        ));
    }

    Ok(S3ProfileConfig {
        region: trimmed_region.to_string(),
        profile: profile
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToString::to_string),
        access_key_id: access_key_id
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToString::to_string),
        endpoint: endpoint
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToString::to_string),
        path_style: *path_style,
    })
}

/// Build an S3 client honoring the AWS SDK's own credential-provider
/// ordering: an explicit AWS profile/SSO session (`config.profile`) takes
/// precedence over static access-key credentials, which take precedence over
/// the default credential chain (environment, instance role, container
/// credentials) used when neither is set.
fn build_client(
    config: &S3ProfileConfig,
    secret_access_key: Option<&SecretString>,
) -> Result<Client, DbError> {
    let mut loader =
        aws_config::defaults(BehaviorVersion::latest()).region(Region::new(config.region.clone()));

    if let Some(profile) = &config.profile {
        loader = loader.profile_name(profile);
    }

    let runtime = runtime();
    let sdk_config = runtime.block_on(loader.load());

    let mut builder = S3ConfigBuilder::from(&sdk_config);

    if let (None, Some(access_key_id), Some(secret_access_key)) =
        (&config.profile, &config.access_key_id, secret_access_key)
    {
        builder = builder.credentials_provider(Credentials::new(
            access_key_id,
            secret_access_key.expose_secret(),
            None,
            None,
            "dbflux-s3-static",
        ));
    }

    if let Some(endpoint) = &config.endpoint {
        builder = builder.endpoint_url(endpoint);
    }

    if config.path_style {
        builder = builder.force_path_style(true);
    }

    Ok(Client::from_conf(builder.build()))
}

fn probe_connection(client: &Client, config: &S3ProfileConfig) -> Result<(), DbError> {
    let runtime = runtime();
    runtime
        .block_on(client.list_buckets().send())
        .map_err(|error| {
            classify_connection_error(S3_ERROR_FORMATTER.format_service_error(&error, config))
        })?;

    Ok(())
}

/// Dedicated tokio runtime for the S3 driver's blocking SDK calls.
///
/// `Connection`'s trait methods are synchronous (called from `dbflux_core`'s
/// blocking connection-pool worker), while the AWS SDK is async-only. A
/// driver-owned runtime (mirroring `dbflux_driver_dynamodb`/
/// `dbflux_driver_cloudwatch`) lets every call `block_on` without any
/// Runtime-in-async-context panic risk.
#[allow(clippy::expect_used)]
static S3_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Runtime::new().expect("S3 driver failed to construct tokio runtime")
});

fn runtime() -> &'static tokio::runtime::Runtime {
    &S3_RUNTIME
}

fn smithy_datetime_to_chrono(value: &aws_smithy_types::DateTime) -> Option<DateTime<Utc>> {
    let millis = value.to_millis().ok()?;
    Utc.timestamp_millis_opt(millis).single()
}

pub(crate) struct S3Connection {
    client: Client,
    config: S3ProfileConfig,
}

impl Connection for S3Connection {
    fn metadata(&self) -> &DriverMetadata {
        &S3_METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        probe_connection(&self.client, &self.config)
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, _req: &QueryRequest) -> Result<QueryResult, DbError> {
        Err(DbError::NotSupported(
            "S3 has no query language — browse buckets and objects through the object browser"
                .to_string(),
        ))
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "Query cancellation is not applicable to S3".to_string(),
        ))
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        Ok(SchemaSnapshot::default())
    }

    fn kind(&self) -> DbKind {
        DbKind::S3
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::SingleDatabase
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }
}

impl ConnectionExt for S3Connection {
    fn as_relational(&self) -> Option<&dyn RelationalConnection> {
        None
    }

    fn as_document(&self) -> Option<&dyn DocumentConnection> {
        None
    }

    fn as_keyvalue(&self) -> Option<&dyn KeyValueConnection> {
        None
    }

    fn as_object_store(&self) -> Option<&dyn ObjectStoreConnection> {
        Some(self)
    }
}

/// Message shared by every `ObjectStoreConnection` method that has not
/// landed yet (object-body, copy/presign, bucket-detail, and bucket-creation
/// operations arrive in later batches of the `s3-driver` change).
fn not_yet_implemented(operation: &str) -> DbError {
    DbError::NotSupported(format!(
        "S3 {operation} is not implemented yet — it lands in a later batch of the S3 driver"
    ))
}

impl ObjectStoreConnection for S3Connection {
    fn list_buckets(&self) -> Result<Vec<BucketInfo>, DbError> {
        let runtime = runtime();
        let mut buckets = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self.client.list_buckets();
            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let output = runtime.block_on(request.send()).map_err(|error| {
                classify_query_error(S3_ERROR_FORMATTER.format_service_error(&error, &self.config))
            })?;

            buckets.extend(output.buckets().iter().map(|bucket| BucketInfo {
                name: bucket.name().unwrap_or_default().to_string(),
                created_at: bucket.creation_date().and_then(smithy_datetime_to_chrono),
            }));

            match output.continuation_token() {
                Some(token) => continuation_token = Some(token.to_string()),
                None => break,
            }
        }

        Ok(buckets)
    }

    fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<&str>,
    ) -> Result<ObjectListingPage, DbError> {
        let runtime = runtime();

        let mut request = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .delimiter("/");

        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let output = runtime.block_on(request.send()).map_err(|error| {
            classify_query_error(S3_ERROR_FORMATTER.format_service_error(&error, &self.config))
        })?;

        let objects = output
            .contents()
            .iter()
            .map(|object| ObjectSummary {
                key: object.key().unwrap_or_default().to_string(),
                size_bytes: object.size().unwrap_or_default().max(0) as u64,
                storage_class: object
                    .storage_class()
                    .map(|class| class.as_str().to_string()),
                last_modified: object.last_modified().and_then(smithy_datetime_to_chrono),
            })
            .collect();

        let common_prefixes = output
            .common_prefixes()
            .iter()
            .filter_map(|common_prefix| common_prefix.prefix().map(ToString::to_string))
            .collect();

        Ok(ObjectListingPage {
            objects,
            common_prefixes,
            next_continuation_token: output.next_continuation_token().map(ToString::to_string),
        })
    }

    fn head_object(&self, _bucket: &str, _key: &str) -> Result<ObjectMetadata, DbError> {
        Err(not_yet_implemented("head_object"))
    }

    fn get_object(&self, _bucket: &str, _key: &str) -> Result<Vec<u8>, DbError> {
        Err(not_yet_implemented("get_object"))
    }

    fn put_object(
        &self,
        _bucket: &str,
        _key: &str,
        _bytes: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<(), DbError> {
        Err(not_yet_implemented("put_object"))
    }

    fn upload_object(
        &self,
        _bucket: &str,
        _key: &str,
        _source_path: &std::path::Path,
        _content_type: Option<&str>,
    ) -> Result<(), DbError> {
        Err(not_yet_implemented("upload_object"))
    }

    fn delete_object(&self, _bucket: &str, _key: &str) -> Result<(), DbError> {
        Err(not_yet_implemented("delete_object"))
    }

    fn delete_prefix(
        &self,
        _bucket: &str,
        _prefix: &str,
    ) -> Result<dbflux_core::DeletePrefixOutcome, DbError> {
        Err(not_yet_implemented("delete_prefix"))
    }

    fn copy_object(&self, _bucket: &str, _src_key: &str, _dest_key: &str) -> Result<(), DbError> {
        Err(not_yet_implemented("copy_object"))
    }

    fn presign(
        &self,
        _bucket: &str,
        _key: &str,
        _method: PresignMethod,
        _expiry: std::time::Duration,
    ) -> Result<String, DbError> {
        Err(not_yet_implemented("presign"))
    }

    fn get_bucket_details(&self, _bucket: &str) -> Result<BucketDetails, DbError> {
        Err(not_yet_implemented("get_bucket_details"))
    }

    fn estimate_bucket_size(
        &self,
        _bucket: &str,
        _object_cap: u64,
    ) -> Result<BucketSizeEstimate, DbError> {
        Err(not_yet_implemented("estimate_bucket_size"))
    }

    fn list_object_versions(
        &self,
        _bucket: &str,
        _key: &str,
    ) -> Result<Vec<ObjectVersionSummary>, DbError> {
        Err(not_yet_implemented("list_object_versions"))
    }

    fn create_bucket(
        &self,
        _bucket: &str,
        _options: BucketCreateOptions,
    ) -> Result<BucketCreateOutcome, DbError> {
        Err(not_yet_implemented("create_bucket"))
    }

    fn delete_bucket(&self, bucket: &str) -> Result<(), DbError> {
        let runtime = runtime();
        runtime
            .block_on(self.client.delete_bucket().bucket(bucket).send())
            .map_err(|error| {
                classify_query_error(S3_ERROR_FORMATTER.format_service_error(&error, &self.config))
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::secrecy::SecretString;

    fn form_values(pairs: &[(&str, &str)]) -> FormValues {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn form_declares_expected_fields() {
        let main_tab = S3_FORM
            .tabs
            .iter()
            .find(|tab| tab.id == "main")
            .expect("S3 form must declare a main tab");

        let fields: Vec<_> = main_tab
            .sections
            .iter()
            .flat_map(|section| section.fields.iter())
            .collect();

        let region = fields
            .iter()
            .find(|f| f.id == "region")
            .expect("region field");
        assert!(region.required);
        assert_eq!(region.kind, FormFieldKind::Text);

        let profile = fields
            .iter()
            .find(|f| f.id == "profile")
            .expect("profile field");
        assert_eq!(
            profile.kind,
            FormFieldKind::AuthProfileRef { provider_id: None }
        );

        let access_key_id = fields
            .iter()
            .find(|f| f.id == "access_key_id")
            .expect("access_key_id field");
        assert!(!access_key_id.required);
        assert_eq!(access_key_id.kind, FormFieldKind::Text);

        let endpoint = fields
            .iter()
            .find(|f| f.id == "endpoint")
            .expect("endpoint field");
        assert!(!endpoint.required);

        let path_style = fields
            .iter()
            .find(|f| f.id == "path_style")
            .expect("path_style field");
        assert_eq!(path_style.kind, FormFieldKind::Checkbox);
    }

    #[test]
    fn requires_password_defaults_to_true_so_the_secret_field_renders() {
        let driver = S3Driver::new();
        assert!(driver.requires_password());
        assert_eq!(
            driver.secret_field_label(&FormValues::new()),
            Some("Secret Access Key".to_string())
        );
    }

    #[test]
    fn build_config_requires_region() {
        let driver = S3Driver::new();
        let values = form_values(&[]);

        let error = driver
            .build_config(&values)
            .expect_err("region should be required");
        match error {
            DbError::InvalidProfile(message) => assert!(message.to_lowercase().contains("region")),
            other => panic!("expected InvalidProfile, got {other:?}"),
        }
    }

    #[test]
    fn build_config_trims_and_defaults_optional_fields() {
        let driver = S3Driver::new();
        let values = form_values(&[("region", "  us-west-2  ")]);

        let config = driver.build_config(&values).expect("valid config");
        match config {
            DbConfig::S3 {
                region,
                profile,
                access_key_id,
                endpoint,
                path_style,
            } => {
                assert_eq!(region, "us-west-2");
                assert_eq!(profile, None);
                assert_eq!(access_key_id, None);
                assert_eq!(endpoint, None);
                assert!(!path_style);
            }
            other => panic!("expected DbConfig::S3, got {other:?}"),
        }
    }

    #[test]
    fn build_config_captures_static_credentials_and_endpoint() {
        let driver = S3Driver::new();
        let values = form_values(&[
            ("region", "auto"),
            ("access_key_id", "AKIAEXAMPLE"),
            ("endpoint", "https://minio.local:9000"),
            ("path_style", "true"),
        ]);

        let config = driver.build_config(&values).expect("valid config");
        match config {
            DbConfig::S3 {
                access_key_id,
                endpoint,
                path_style,
                ..
            } => {
                assert_eq!(access_key_id, Some("AKIAEXAMPLE".to_string()));
                assert_eq!(endpoint, Some("https://minio.local:9000".to_string()));
                assert!(path_style);
            }
            other => panic!("expected DbConfig::S3, got {other:?}"),
        }
    }

    #[test]
    fn build_config_then_extract_values_round_trips() {
        let driver = S3Driver::new();
        let original = form_values(&[
            ("region", "eu-west-1"),
            ("profile", "my-sso-profile"),
            ("access_key_id", "AKIAROUNDTRIP"),
            ("endpoint", "https://r2.example.com"),
            ("path_style", "true"),
        ]);

        let config = driver.build_config(&original).expect("valid config");
        let extracted = driver.extract_values(&config);

        assert_eq!(
            extracted.get("region").map(String::as_str),
            Some("eu-west-1")
        );
        assert_eq!(
            extracted.get("profile").map(String::as_str),
            Some("my-sso-profile")
        );
        assert_eq!(
            extracted.get("access_key_id").map(String::as_str),
            Some("AKIAROUNDTRIP")
        );
        assert_eq!(
            extracted.get("endpoint").map(String::as_str),
            Some("https://r2.example.com")
        );
        assert_eq!(
            extracted.get("path_style").map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn extract_values_returns_empty_map_for_non_s3_config() {
        let driver = S3Driver::new();
        let config = DbConfig::default_sqlite();

        assert!(driver.extract_values(&config).is_empty());
    }

    #[test]
    fn profile_config_rejects_missing_region() {
        let config = DbConfig::S3 {
            region: "   ".to_string(),
            profile: None,
            access_key_id: None,
            endpoint: None,
            path_style: false,
        };

        let error = profile_config(&config).expect_err("blank region should be rejected");
        assert!(matches!(error, DbError::InvalidProfile(_)));
    }

    #[test]
    fn profile_config_trims_every_optional_field() {
        let config = DbConfig::S3 {
            region: " us-east-1 ".to_string(),
            profile: Some("  ".to_string()),
            access_key_id: Some(" AKIA ".to_string()),
            endpoint: Some(" https://example.com ".to_string()),
            path_style: true,
        };

        let resolved = profile_config(&config).expect("valid config");
        assert_eq!(resolved.region, "us-east-1");
        assert_eq!(resolved.profile, None);
        assert_eq!(resolved.access_key_id, Some("AKIA".to_string()));
        assert_eq!(resolved.endpoint, Some("https://example.com".to_string()));
        assert!(resolved.path_style);
    }

    #[test]
    fn diagnostic_detail_never_includes_credentials() {
        let config = S3ProfileConfig {
            region: "us-east-1".to_string(),
            profile: None,
            access_key_id: Some("AKIASHOULDNOTLEAK".to_string()),
            endpoint: Some("https://minio.local:9000".to_string()),
            path_style: true,
        };

        let detail = config.diagnostic_detail();
        assert!(!detail.contains("AKIASHOULDNOTLEAK"));
        assert!(detail.contains("us-east-1"));
        assert!(detail.contains("minio.local:9000"));
    }

    #[test]
    fn secret_string_never_surfaces_in_debug_output() {
        let secret = SecretString::from("super-secret-value".to_string());
        let debug_output = format!("{secret:?}");
        assert!(!debug_output.contains("super-secret-value"));
    }
}
