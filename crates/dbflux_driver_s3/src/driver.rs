use std::collections::HashMap;
use std::sync::LazyLock;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Credentials};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLocationConstraint, BucketVersioningStatus, CreateBucketConfiguration, Delete,
    ObjectIdentifier, PublicAccessBlockConfiguration, ServerSideEncryption,
    ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
    VersioningConfiguration,
};
use chrono::{DateTime, TimeZone, Utc};
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    BucketCreateOptions, BucketCreateOutcome, BucketDetails, BucketEncryption, BucketInfo,
    BucketSizeEstimate, Connection, ConnectionExt, ConnectionProfile, DatabaseCategory, DbConfig,
    DbDriver, DbError, DbKind, DeploymentClass, DocumentConnection, DriverCapabilities,
    DriverFormDef, DriverMetadata, FormFieldKind, FormSection, FormTab, FormValues, Icon,
    KeyValueConnection, ObjectListingPage, ObjectMetadata, ObjectStoreConnection, ObjectSummary,
    ObjectVersionSummary, PresignMethod, QueryHandle, QueryLanguage, QueryRequest, QueryResult,
    RelationalConnection, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TransferFamily,
    VersioningStatus, field, field_required,
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
        Ok(Box::new(connect_internal(profile, password)?))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let config = profile_config(&profile.config)?;
        let client = build_client(&config, None)?;

        probe_connection(&client, &config)
    }
}

impl S3Driver {
    /// Connect and return the `ObjectStoreConnection` capability directly,
    /// bypassing the `Connection` trait boxing.
    ///
    /// `ConnectionExt::as_object_store()` is the app-facing capability-cast
    /// seam that the object-browser/buckets-table UI batches will use once
    /// they land; no call site downcasts a boxed `dyn Connection` to it yet.
    /// This inherent method lets callers that only need
    /// `ObjectStoreConnection` (the driver's own live-integration suite,
    /// today) reach it without waiting on that seam.
    pub fn connect_object_store(
        &self,
        profile: &ConnectionProfile,
        password: Option<&SecretString>,
    ) -> Result<Box<dyn ObjectStoreConnection>, DbError> {
        Ok(Box::new(connect_internal(profile, password)?))
    }
}

fn connect_internal(
    profile: &ConnectionProfile,
    password: Option<&SecretString>,
) -> Result<S3Connection, DbError> {
    let config = profile_config(&profile.config)?;
    let client = build_client(&config, password)?;

    probe_connection(&client, &config)?;

    Ok(S3Connection { client, config })
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
/// landed yet. `head_object` is the only remaining stub — every other object,
/// prefix, and bucket operation is now implemented above.
fn not_yet_implemented(operation: &str) -> DbError {
    DbError::NotSupported(format!(
        "S3 {operation} is not implemented yet — it lands in a later batch of the S3 driver"
    ))
}

/// Maximum number of keys accepted by a single S3 `DeleteObjects` call.
const DELETE_BATCH_SIZE: usize = 1000;

/// Deletes one batch of up to [`DELETE_BATCH_SIZE`] keys via a single
/// `DeleteObjects` call and returns the number of keys S3 actually
/// confirmed as deleted.
fn delete_object_batch(
    client: &Client,
    config: &S3ProfileConfig,
    runtime: &tokio::runtime::Runtime,
    bucket: &str,
    keys: &[String],
) -> Result<u64, DbError> {
    if keys.is_empty() {
        return Ok(0);
    }

    let mut objects = Vec::with_capacity(keys.len());
    for key in keys {
        let identifier = ObjectIdentifier::builder()
            .key(key.clone())
            .build()
            .map_err(|error| {
                DbError::query_failed(format!("Invalid S3 object key {key}: {error}"))
            })?;
        objects.push(identifier);
    }

    let delete = Delete::builder()
        .set_objects(Some(objects))
        .build()
        .map_err(|error| {
            DbError::query_failed(format!("Failed to build S3 delete batch: {error}"))
        })?;

    let output = runtime
        .block_on(client.delete_objects().bucket(bucket).delete(delete).send())
        .map_err(|error| {
            classify_query_error(S3_ERROR_FORMATTER.format_service_error(&error, config))
        })?;

    Ok(output.deleted().len() as u64)
}

/// Builds the `x-amz-copy-source` header value (`bucket/key`). Each `/`-
/// delimited segment of the key is percent-encoded independently so folder
/// separators survive while special characters within a segment (spaces,
/// `%`, non-ASCII) round-trip correctly, per S3's `CopyObject` requirements.
fn build_copy_source(bucket: &str, key: &str) -> String {
    let encoded_key = key
        .split('/')
        .map(urlencoding::encode)
        .collect::<Vec<_>>()
        .join("/");

    format!("{bucket}/{encoded_key}")
}

/// AWS rejects an explicit `LocationConstraint` for `us-east-1` — it is the
/// API's implicit default and has no corresponding
/// `BucketLocationConstraint` enum value, so the constraint must be omitted
/// entirely for that region rather than sent as an empty/invalid value.
fn is_default_region(region: &str) -> bool {
    region.eq_ignore_ascii_case("us-east-1")
}

/// Builds the `CreateBucket` request, including the region constraint (see
/// `is_default_region`) and, when requested, the Object Lock flag. Object
/// Lock can only be set at creation time — unlike versioning, public-access
/// block, and default encryption, it has no separate post-creation API call,
/// so `create_bucket` rides it on this initial request instead of applying
/// it afterward.
fn build_create_bucket_request(
    client: &Client,
    bucket: &str,
    region: &str,
    object_lock: bool,
) -> aws_sdk_s3::operation::create_bucket::builders::CreateBucketFluentBuilder {
    let mut request = client.create_bucket().bucket(bucket);

    if !is_default_region(region) {
        let configuration = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from(region))
            .build();
        request = request.create_bucket_configuration(configuration);
    }

    if object_lock {
        request = request.object_lock_enabled_for_bucket(true);
    }

    request
}

/// Builds the `PutBucketEncryption` configuration from a `BucketEncryption`
/// choice. Callers must filter out `BucketEncryption::None` before calling
/// this — it has no corresponding SSE algorithm.
fn build_encryption_configuration(
    encryption: &BucketEncryption,
) -> Result<ServerSideEncryptionConfiguration, DbError> {
    let sse_by_default = match encryption {
        BucketEncryption::SseS3 => ServerSideEncryptionByDefault::builder()
            .sse_algorithm(ServerSideEncryption::Aes256)
            .build(),
        BucketEncryption::SseKms { key_id } => {
            let mut builder = ServerSideEncryptionByDefault::builder()
                .sse_algorithm(ServerSideEncryption::AwsKms);
            if let Some(key_id) = key_id {
                builder = builder.kms_master_key_id(key_id.clone());
            }
            builder.build()
        }
        BucketEncryption::None => {
            return Err(DbError::query_failed(
                "Default encryption is not supported for BucketEncryption::None".to_string(),
            ));
        }
    }
    .map_err(|error| {
        DbError::query_failed(format!("Failed to build S3 encryption rule: {error}"))
    })?;

    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(sse_by_default)
        .build();

    ServerSideEncryptionConfiguration::builder()
        .rules(rule)
        .build()
        .map_err(|error| {
            DbError::query_failed(format!(
                "Failed to build S3 encryption configuration: {error}"
            ))
        })
}

/// One-line, non-blocking warning surfaced when an optional bucket-creation
/// configuration call fails on the target endpoint (Amendment F, DEC-20).
/// The bucket itself is already created by the time this is called.
fn degradation_warning(field: &str, formatted: &dbflux_core::FormattedError) -> String {
    format!("{field} is not supported by this endpoint: {formatted}")
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

    fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>, DbError> {
        let runtime = runtime();

        let output = runtime
            .block_on(self.client.get_object().bucket(bucket).key(key).send())
            .map_err(|error| {
                classify_query_error(S3_ERROR_FORMATTER.format_service_error(&error, &self.config))
            })?;

        let aggregated = runtime.block_on(output.body.collect()).map_err(|error| {
            DbError::query_failed(format!("Failed to read S3 object body: {error}"))
        })?;

        Ok(aggregated.into_bytes().to_vec())
    }

    fn put_object(
        &self,
        bucket: &str,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<(), DbError> {
        let runtime = runtime();

        let mut request = self
            .client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(bytes));

        if let Some(content_type) = content_type {
            request = request.content_type(content_type);
        }

        runtime.block_on(request.send()).map_err(|error| {
            classify_query_error(S3_ERROR_FORMATTER.format_service_error(&error, &self.config))
        })?;

        Ok(())
    }

    fn upload_object(
        &self,
        bucket: &str,
        key: &str,
        source_path: &std::path::Path,
        content_type: Option<&str>,
    ) -> Result<(), DbError> {
        let runtime = runtime();

        // `ByteStream::from_path` streams the file in fixed-size chunks
        // rather than reading it fully into memory — required for large
        // uploads (no multipart splitting yet, see `ObjectStoreConnection`
        // trait docs on `upload_object`).
        let body = runtime
            .block_on(ByteStream::from_path(source_path))
            .map_err(|error| {
                DbError::query_failed(format!(
                    "Failed to stream {}: {error}",
                    source_path.display()
                ))
            })?;

        let mut request = self.client.put_object().bucket(bucket).key(key).body(body);

        if let Some(content_type) = content_type {
            request = request.content_type(content_type);
        }

        runtime.block_on(request.send()).map_err(|error| {
            classify_query_error(S3_ERROR_FORMATTER.format_service_error(&error, &self.config))
        })?;

        Ok(())
    }

    fn delete_object(&self, bucket: &str, key: &str) -> Result<(), DbError> {
        let runtime = runtime();
        runtime
            .block_on(self.client.delete_object().bucket(bucket).key(key).send())
            .map_err(|error| {
                classify_query_error(S3_ERROR_FORMATTER.format_service_error(&error, &self.config))
            })?;

        Ok(())
    }

    fn delete_prefix(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<dbflux_core::DeletePrefixOutcome, DbError> {
        let runtime = runtime();
        let mut deleted_count: u64 = 0;
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self.client.list_objects_v2().bucket(bucket).prefix(prefix);
            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let output = runtime.block_on(request.send()).map_err(|error| {
                classify_query_error(S3_ERROR_FORMATTER.format_service_error(&error, &self.config))
            })?;

            let keys: Vec<String> = output
                .contents()
                .iter()
                .filter_map(|object| object.key().map(ToString::to_string))
                .collect();

            for batch in keys.chunks(DELETE_BATCH_SIZE) {
                deleted_count +=
                    delete_object_batch(&self.client, &self.config, runtime, bucket, batch)?;
            }

            match output.next_continuation_token() {
                Some(token) => continuation_token = Some(token.to_string()),
                None => break,
            }
        }

        Ok(dbflux_core::DeletePrefixOutcome { deleted_count })
    }

    fn copy_object(&self, bucket: &str, src_key: &str, dest_key: &str) -> Result<(), DbError> {
        let runtime = runtime();
        let copy_source = build_copy_source(bucket, src_key);

        runtime
            .block_on(
                self.client
                    .copy_object()
                    .bucket(bucket)
                    .copy_source(copy_source)
                    .key(dest_key)
                    .send(),
            )
            .map_err(|error| {
                classify_query_error(S3_ERROR_FORMATTER.format_service_error(&error, &self.config))
            })?;

        Ok(())
    }

    fn presign(
        &self,
        bucket: &str,
        key: &str,
        method: PresignMethod,
        expiry: std::time::Duration,
    ) -> Result<String, DbError> {
        let runtime = runtime();

        let presigning_config = PresigningConfig::expires_in(expiry)
            .map_err(|error| DbError::query_failed(format!("Invalid presign expiry: {error}")))?;

        // The generated request URI is returned directly to the caller and
        // must never be logged, persisted, or embedded in an error/audit
        // record here — only the calling action (bucket, key, method,
        // expiry) is audited by the caller.
        let presigned_uri = match method {
            PresignMethod::Get => runtime
                .block_on(
                    self.client
                        .get_object()
                        .bucket(bucket)
                        .key(key)
                        .presigned(presigning_config),
                )
                .map(|presigned| presigned.uri().to_string())
                .map_err(|error| {
                    classify_query_error(
                        S3_ERROR_FORMATTER.format_service_error(&error, &self.config),
                    )
                })?,
            PresignMethod::Put => runtime
                .block_on(
                    self.client
                        .put_object()
                        .bucket(bucket)
                        .key(key)
                        .presigned(presigning_config),
                )
                .map(|presigned| presigned.uri().to_string())
                .map_err(|error| {
                    classify_query_error(
                        S3_ERROR_FORMATTER.format_service_error(&error, &self.config),
                    )
                })?,
        };

        Ok(presigned_uri)
    }

    fn get_bucket_details(&self, bucket: &str) -> Result<BucketDetails, DbError> {
        let runtime = runtime();

        let location_output = runtime
            .block_on(self.client.get_bucket_location().bucket(bucket).send())
            .map_err(|error| {
                classify_query_error(S3_ERROR_FORMATTER.format_service_error(&error, &self.config))
            })?;

        // An empty/absent location constraint means `us-east-1` — S3's own
        // API quirk (see `is_default_region`/`build_create_bucket_request`).
        let region = location_output
            .location_constraint()
            .map(|constraint| constraint.as_str().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "us-east-1".to_string());

        let versioning_output = runtime
            .block_on(self.client.get_bucket_versioning().bucket(bucket).send())
            .map_err(|error| {
                classify_query_error(S3_ERROR_FORMATTER.format_service_error(&error, &self.config))
            })?;

        let versioning = match versioning_output.status() {
            Some(BucketVersioningStatus::Enabled) => VersioningStatus::Enabled,
            Some(BucketVersioningStatus::Suspended) => VersioningStatus::Suspended,
            _ => VersioningStatus::Disabled,
        };

        Ok(BucketDetails { region, versioning })
    }

    fn estimate_bucket_size(
        &self,
        bucket: &str,
        object_cap: u64,
    ) -> Result<BucketSizeEstimate, DbError> {
        let runtime = runtime();
        let mut object_count: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut continuation_token: Option<String> = None;
        let mut truncated = false;

        loop {
            let mut request = self.client.list_objects_v2().bucket(bucket);
            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let output = runtime.block_on(request.send()).map_err(|error| {
                classify_query_error(S3_ERROR_FORMATTER.format_service_error(&error, &self.config))
            })?;

            for object in output.contents() {
                object_count += 1;
                total_bytes += object.size().unwrap_or_default().max(0) as u64;

                if object_count >= object_cap {
                    truncated = true;
                    break;
                }
            }

            if truncated {
                break;
            }

            match output.next_continuation_token() {
                Some(token) => continuation_token = Some(token.to_string()),
                None => break,
            }
        }

        Ok(BucketSizeEstimate {
            object_count,
            total_bytes,
            truncated,
        })
    }

    fn list_object_versions(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<ObjectVersionSummary>, DbError> {
        let runtime = runtime();
        let mut versions = Vec::new();
        let mut key_marker: Option<String> = None;
        let mut version_id_marker: Option<String> = None;

        loop {
            let mut request = self
                .client
                .list_object_versions()
                .bucket(bucket)
                .prefix(key);

            if let Some(marker) = &key_marker {
                request = request.key_marker(marker);
            }
            if let Some(marker) = &version_id_marker {
                request = request.version_id_marker(marker);
            }

            let output = runtime.block_on(request.send()).map_err(|error| {
                classify_query_error(S3_ERROR_FORMATTER.format_service_error(&error, &self.config))
            })?;

            versions.extend(
                output
                    .versions()
                    .iter()
                    .filter(|version| version.key() == Some(key))
                    .map(|version| ObjectVersionSummary {
                        version_id: version.version_id().unwrap_or_default().to_string(),
                        is_latest: version.is_latest().unwrap_or(false),
                        size_bytes: version.size().unwrap_or_default().max(0) as u64,
                        last_modified: version.last_modified().and_then(smithy_datetime_to_chrono),
                    }),
            );

            if output.is_truncated().unwrap_or(false) {
                key_marker = output.next_key_marker().map(ToString::to_string);
                version_id_marker = output.next_version_id_marker().map(ToString::to_string);
            } else {
                break;
            }
        }

        Ok(versions)
    }

    /// Creates the bucket, then applies each optional configuration
    /// (versioning, public-access block, default encryption) as its own
    /// follow-up call so a rejection on one S3-compatible endpoint degrades
    /// to a warning instead of aborting the whole flow (Amendment F,
    /// DEC-20). Object Lock is the one exception: S3 only allows enabling it
    /// at `CreateBucket` time, so it rides on the initial request — if the
    /// endpoint rejects that flag, the bucket creation is retried without it
    /// and a warning is recorded instead.
    fn create_bucket(
        &self,
        bucket: &str,
        options: BucketCreateOptions,
    ) -> Result<BucketCreateOutcome, DbError> {
        let runtime = runtime();
        let mut warnings = Vec::new();

        let base_creation = runtime.block_on(
            build_create_bucket_request(&self.client, bucket, &options.region, options.object_lock)
                .send(),
        );

        if let Err(error) = base_creation {
            if !options.object_lock {
                return Err(classify_query_error(
                    S3_ERROR_FORMATTER.format_service_error(&error, &self.config),
                ));
            }

            warnings.push(degradation_warning(
                "Object lock",
                &S3_ERROR_FORMATTER.format_service_error(&error, &self.config),
            ));

            runtime
                .block_on(
                    build_create_bucket_request(&self.client, bucket, &options.region, false)
                        .send(),
                )
                .map_err(|error| {
                    classify_query_error(
                        S3_ERROR_FORMATTER.format_service_error(&error, &self.config),
                    )
                })?;
        }

        if options.versioning {
            let result = runtime.block_on(
                self.client
                    .put_bucket_versioning()
                    .bucket(bucket)
                    .versioning_configuration(
                        VersioningConfiguration::builder()
                            .status(BucketVersioningStatus::Enabled)
                            .build(),
                    )
                    .send(),
            );

            if let Err(error) = result {
                warnings.push(degradation_warning(
                    "Versioning",
                    &S3_ERROR_FORMATTER.format_service_error(&error, &self.config),
                ));
            }
        }

        if options.block_public_access {
            let result = runtime.block_on(
                self.client
                    .put_public_access_block()
                    .bucket(bucket)
                    .public_access_block_configuration(
                        PublicAccessBlockConfiguration::builder()
                            .block_public_acls(true)
                            .ignore_public_acls(true)
                            .block_public_policy(true)
                            .restrict_public_buckets(true)
                            .build(),
                    )
                    .send(),
            );

            if let Err(error) = result {
                warnings.push(degradation_warning(
                    "Block public access",
                    &S3_ERROR_FORMATTER.format_service_error(&error, &self.config),
                ));
            }
        }

        if !matches!(options.encryption, BucketEncryption::None) {
            match build_encryption_configuration(&options.encryption) {
                Ok(configuration) => {
                    let result = runtime.block_on(
                        self.client
                            .put_bucket_encryption()
                            .bucket(bucket)
                            .server_side_encryption_configuration(configuration)
                            .send(),
                    );

                    if let Err(error) = result {
                        warnings.push(degradation_warning(
                            "Default encryption",
                            &S3_ERROR_FORMATTER.format_service_error(&error, &self.config),
                        ));
                    }
                }
                Err(error) => warnings.push(error.to_string()),
            }
        }

        Ok(BucketCreateOutcome { warnings })
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

    #[test]
    fn build_copy_source_joins_bucket_and_key() {
        let copy_source = build_copy_source("my-bucket", "reports/2026/summary.csv");
        assert_eq!(copy_source, "my-bucket/reports/2026/summary.csv");
    }

    #[test]
    fn build_copy_source_encodes_special_characters_per_segment_only() {
        let copy_source = build_copy_source("my-bucket", "folder with spaces/file name.txt");
        assert_eq!(
            copy_source,
            "my-bucket/folder%20with%20spaces/file%20name.txt"
        );
        assert!(!copy_source.contains("%2F"));
    }

    #[test]
    fn is_default_region_matches_us_east_1_case_insensitively() {
        assert!(is_default_region("us-east-1"));
        assert!(is_default_region("US-EAST-1"));
        assert!(!is_default_region("us-west-2"));
        assert!(!is_default_region("eu-west-1"));
    }

    #[test]
    fn delete_batch_size_matches_s3_delete_objects_limit() {
        assert_eq!(DELETE_BATCH_SIZE, 1000);
    }

    #[test]
    fn key_chunking_splits_large_batches_at_the_s3_limit() {
        let keys: Vec<String> = (0..2500).map(|index| format!("key-{index}")).collect();
        let chunk_sizes: Vec<usize> = keys.chunks(DELETE_BATCH_SIZE).map(<[_]>::len).collect();

        assert_eq!(chunk_sizes, vec![1000, 1000, 500]);
    }

    #[test]
    fn build_encryption_configuration_rejects_none() {
        let error = build_encryption_configuration(&BucketEncryption::None)
            .expect_err("None should be rejected by the caller before this point");
        assert!(matches!(error, DbError::QueryFailed(_)));
    }

    #[test]
    fn build_encryption_configuration_accepts_sse_s3() {
        let configuration = build_encryption_configuration(&BucketEncryption::SseS3)
            .expect("SseS3 should build a valid configuration");
        assert_eq!(configuration.rules().len(), 1);
    }

    #[test]
    fn build_encryption_configuration_accepts_sse_kms_without_key_id() {
        let configuration =
            build_encryption_configuration(&BucketEncryption::SseKms { key_id: None })
                .expect("SseKms without a key id should still build");
        assert_eq!(configuration.rules().len(), 1);
    }

    #[test]
    fn degradation_warning_names_field_and_includes_error_detail() {
        let formatted =
            dbflux_core::FormattedError::new("Object lock is not supported".to_string());
        let warning = degradation_warning("Object lock", &formatted);

        assert!(warning.starts_with("Object lock is not supported by this endpoint"));
        assert!(warning.contains("Object lock is not supported"));
    }
}
