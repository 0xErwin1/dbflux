use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use aws_config::{AppName, BehaviorVersion, Region, SdkConfig};
use aws_sdk_secretsmanager::config::Credentials;
use chrono::{DateTime, TimeZone, Utc};
use dbflux_core::SecretStoreSource;
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    Connection, ConnectionProfile, DatabaseCategory, DbConfig, DbDriver, DbError, DbKind,
    DeploymentClass, DriverCapabilities, DriverFormDef, DriverMetadata, FormFieldKind, FormSection,
    FormTab, FormValues, Icon, QueryHandle, QueryLanguage, QueryRequest, QueryResult,
    SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TransferFamily, field, field_required,
};

use crate::error_formatter::{ErrorTarget, classify_operation_error, format_service_error};
use crate::parameter_store::ParameterStore;
use crate::secrets_manager::SecretsManagerStore;

pub static AWS_SECRETS_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "aws_secrets".into(),
    display_name: "AWS Secrets".into(),
    description: "AWS Secrets Manager and Systems Manager Parameter Store".into(),
    category: DatabaseCategory::SecretStore,
    transfer_family: TransferFamily::Incompatible,
    deployment_class: Some(DeploymentClass::CloudManaged),
    query_language: QueryLanguage::Custom("AWS Secrets".into()),
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::AUTHENTICATION.bits()
            | DriverCapabilities::SECRET_STORE.bits()
            | DriverCapabilities::REGION_SWITCH.bits(),
    ),
    default_port: None,
    uri_scheme: "aws-secrets".into(),
    icon: Icon::Database,
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

pub static AWS_SECRETS_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![FormTab {
        id: "main".into(),
        label: "Main".into(),
        sections: vec![
            FormSection {
                title: "AWS".into(),
                icon: Some(dbflux_core::FormSectionIcon::Cloud),
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
                        "optional — leave blank when a profile is set, or to use the default AWS credential chain",
                    ),
                    field("password", "Secret Access Key", FormFieldKind::Password, ""),
                ],
            },
            FormSection {
                title: "Endpoint".into(),
                icon: Some(dbflux_core::FormSectionIcon::Server),
                fields: vec![field(
                    "endpoint",
                    "Endpoint Override",
                    FormFieldKind::Text,
                    "http://localhost:4566",
                )],
            },
        ],
    }],
});

pub struct AwsSecretsDriver;

impl AwsSecretsDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AwsSecretsDriver {
    fn default() -> Self {
        Self::new()
    }
}

/// Static AWS config for a connection, resolved from `DbConfig::AwsSecrets`.
///
/// Carries `access_key_id` but never the secret key, which only lives in the
/// `SecretString` passed to `connect_with_secrets`.
#[derive(Debug, Clone)]
pub(crate) struct AwsSecretsProfileConfig {
    pub(crate) region: String,
    pub(crate) profile: Option<String>,
    pub(crate) access_key_id: Option<String>,
    pub(crate) endpoint: Option<String>,
}

impl AwsSecretsProfileConfig {
    /// Non-sensitive diagnostic summary attached to formatted errors: region
    /// and endpoint only, never credentials.
    pub(crate) fn diagnostic_detail(&self) -> String {
        match &self.endpoint {
            Some(endpoint) => format!("region={}, endpoint_override={endpoint}", self.region),
            None => format!("region={}", self.region),
        }
    }
}

impl DbDriver for AwsSecretsDriver {
    fn kind(&self) -> DbKind {
        DbKind::AwsSecrets
    }

    fn metadata(&self) -> &DriverMetadata {
        &AWS_SECRETS_METADATA
    }

    fn picker_hint(&self) -> String {
        "Secrets Manager, Parameter Store".to_string()
    }

    fn form_definition(&self) -> &DriverFormDef {
        &AWS_SECRETS_FORM
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        "builtin:aws_secrets".into()
    }

    fn secret_field_label(&self, _values: &FormValues) -> Option<String> {
        Some("Secret Access Key".to_string())
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let region = trimmed_optional(values, "region")
            .ok_or_else(|| DbError::InvalidProfile("AWS Region is required".to_string()))?;

        let profile = trimmed_optional(values, "profile");
        let access_key_id = trimmed_optional(values, "access_key_id");
        ensure_single_credential_source(profile.as_deref(), access_key_id.as_deref())?;

        Ok(DbConfig::AwsSecrets {
            region,
            profile,
            access_key_id,
            endpoint: trimmed_optional(values, "endpoint"),
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let DbConfig::AwsSecrets {
            region,
            profile,
            access_key_id,
            endpoint,
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
        connect_internal(profile, None).map(|_connection| ())
    }

    /// A connection reaches one region, so moving it keeps every other part of
    /// the config: the credential source, the endpoint override and whatever
    /// else the profile carries decide who connects and how, and none of that
    /// changes with the region.
    fn with_region(&self, config: &DbConfig, region: &str) -> Option<DbConfig> {
        let DbConfig::AwsSecrets {
            profile,
            access_key_id,
            endpoint,
            ..
        } = config
        else {
            return None;
        };

        let region = region.trim();

        if region.is_empty() {
            return None;
        }

        Some(DbConfig::AwsSecrets {
            region: region.to_string(),
            profile: profile.clone(),
            access_key_id: access_key_id.clone(),
            endpoint: endpoint.clone(),
        })
    }
}

fn connect_internal(
    profile: &ConnectionProfile,
    password: Option<&SecretString>,
) -> Result<AwsSecretsConnection, DbError> {
    let config = profile_config(&profile.config)?;
    let sdk_config = load_sdk_config(&config, password);

    let connection = AwsSecretsConnection {
        secrets_manager: SecretsManagerStore::new(
            secrets_manager_client(&sdk_config, &config),
            config.clone(),
        ),
        parameter_store: ParameterStore::new(ssm_client(&sdk_config, &config), config.clone()),
        ec2: ec2_client(&sdk_config, &config),
        config,
        regions: RegionCatalog::new(),
    };

    connection.probe()?;

    Ok(connection)
}

fn trimmed_optional(values: &FormValues, id: &str) -> Option<String> {
    values
        .get(id)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn trimmed(value: Option<&String>) -> Option<String> {
    value
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

/// A profile and static keys are alternative credential sources; accepting
/// both would silently ignore one of them.
fn ensure_single_credential_source(
    profile: Option<&str>,
    access_key_id: Option<&str>,
) -> Result<(), DbError> {
    if profile.is_some() && access_key_id.is_some() {
        return Err(DbError::InvalidProfile(
            "Profile and Access Key ID cannot both be set: choose an AWS profile or static access keys, not both".to_string(),
        ));
    }

    Ok(())
}

pub(crate) fn profile_config(config: &DbConfig) -> Result<AwsSecretsProfileConfig, DbError> {
    let DbConfig::AwsSecrets {
        region,
        profile,
        access_key_id,
        endpoint,
    } = config
    else {
        return Err(DbError::InvalidProfile(
            "Expected AWS Secrets configuration".to_string(),
        ));
    };

    let region = trimmed(Some(region))
        .ok_or_else(|| DbError::InvalidProfile("AWS Region is required".to_string()))?;

    let profile = trimmed(profile.as_ref());
    let access_key_id = trimmed(access_key_id.as_ref());
    ensure_single_credential_source(profile.as_deref(), access_key_id.as_deref())?;

    Ok(AwsSecretsProfileConfig {
        region,
        profile,
        access_key_id,
        endpoint: trimmed(endpoint.as_ref()),
    })
}

/// Static credentials apply only when both the access key id and the secret
/// key are present. Otherwise the named profile, or the default credential
/// chain when there is none, provides them.
struct ResolvedSdkConfig {
    shared: SdkConfig,
    static_credentials: Option<Credentials>,
}

/// Loads the shared AWS config from exactly one credential source: an AWS
/// profile/SSO session (`config.profile`) or static access-key credentials,
/// which `profile_config` guarantees are never both set.
fn load_sdk_config(
    config: &AwsSecretsProfileConfig,
    secret_access_key: Option<&SecretString>,
) -> ResolvedSdkConfig {
    let mut loader =
        aws_config::defaults(BehaviorVersion::latest()).region(Region::new(config.region.clone()));

    match AppName::new(dbflux_core::client_identity_token()) {
        Ok(app_name) => loader = loader.app_name(app_name),
        Err(error) => log::warn!("failed to set AWS SDK app name: {error}"),
    }

    if let Some(profile) = &config.profile {
        loader = loader.profile_name(profile);
    }

    let shared = runtime().block_on(loader.load());

    let static_credentials = match (&config.access_key_id, secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => Some(Credentials::new(
            access_key_id,
            secret_access_key.expose_secret(),
            None,
            None,
            "dbflux-aws-secrets-static",
        )),
        _ => None,
    };

    ResolvedSdkConfig {
        shared,
        static_credentials,
    }
}

fn secrets_manager_client(
    sdk_config: &ResolvedSdkConfig,
    config: &AwsSecretsProfileConfig,
) -> aws_sdk_secretsmanager::Client {
    let mut builder = aws_sdk_secretsmanager::config::Builder::from(&sdk_config.shared);

    if let Some(credentials) = &sdk_config.static_credentials {
        builder = builder.credentials_provider(credentials.clone());
    }

    if let Some(endpoint) = &config.endpoint {
        builder = builder.endpoint_url(endpoint);
    }

    aws_sdk_secretsmanager::Client::from_conf(builder.build())
}

/// The provider's region list for one connection, fetched at most once.
///
/// `DescribeRegions` answers the same list for every request made with the same
/// credentials, so the control that offers the regions asks the provider once
/// and reuses the answer. A failed fetch is not cached: a missing
/// `ec2:DescribeRegions` permission or a throttled call must be retriable
/// without reconnecting.
#[derive(Debug, Default)]
pub(crate) struct RegionCatalog {
    cached: Mutex<Option<Vec<String>>>,
}

impl RegionCatalog {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns the regions, calling `fetch` only when nothing was cached yet.
    pub(crate) fn regions<F>(&self, fetch: F) -> Result<Vec<String>, DbError>
    where
        F: FnOnce() -> Result<Vec<String>, DbError>,
    {
        if let Some(regions) = self.lock().clone() {
            return Ok(regions);
        }

        let regions = fetch()?;
        *self.lock() = Some(regions.clone());

        Ok(regions)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Vec<String>>> {
        self.cached.lock().unwrap_or_else(|poisoned| {
            log::warn!("AWS Secrets region catalog lock was poisoned, recovering...");
            poisoned.into_inner()
        })
    }
}

/// Trimmed, sorted and deduplicated region names: `DescribeRegions` reports
/// them in no guaranteed order and a repeated name would render twice in the
/// control that offers them.
pub(crate) fn normalize_regions(names: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut regions: Vec<String> = names
        .into_iter()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect();

    regions.sort();
    regions.dedup();
    regions
}

/// Builds the `DescribeRegions` client. It shares the credentials and the
/// endpoint override of the secret stores, so a local endpoint such as
/// LocalStack answers it too.
fn ec2_client(
    sdk_config: &ResolvedSdkConfig,
    config: &AwsSecretsProfileConfig,
) -> aws_sdk_ec2::Client {
    let mut builder = aws_sdk_ec2::config::Builder::from(&sdk_config.shared);

    if let Some(credentials) = &sdk_config.static_credentials {
        builder = builder.credentials_provider(credentials.clone());
    }

    if let Some(endpoint) = &config.endpoint {
        builder = builder.endpoint_url(endpoint);
    }

    aws_sdk_ec2::Client::from_conf(builder.build())
}

/// The regions this connection can move to, as the provider reports them for
/// these credentials. Regions the account has not enabled are left out: moving
/// there would only fail on the next call, and the control must not offer a
/// destination that cannot be reached.
fn describe_regions(
    client: &aws_sdk_ec2::Client,
    config: &AwsSecretsProfileConfig,
) -> Result<Vec<String>, DbError> {
    let output = runtime()
        .block_on(client.describe_regions().send())
        .map_err(|error| {
            classify_operation_error(format_service_error(&error, config, &ErrorTarget::None))
        })?;

    Ok(normalize_regions(
        output
            .regions()
            .iter()
            .filter_map(|region| region.region_name())
            .map(str::to_string),
    ))
}

fn ssm_client(
    sdk_config: &ResolvedSdkConfig,
    config: &AwsSecretsProfileConfig,
) -> aws_sdk_ssm::Client {
    let mut builder = aws_sdk_ssm::config::Builder::from(&sdk_config.shared);

    if let Some(credentials) = &sdk_config.static_credentials {
        builder = builder.credentials_provider(credentials.clone());
    }

    if let Some(endpoint) = &config.endpoint {
        builder = builder.endpoint_url(endpoint);
    }

    aws_sdk_ssm::Client::from_conf(builder.build())
}

/// Dedicated tokio runtime for the driver's blocking SDK calls.
///
/// `Connection` and `SecretStoreSource` methods are synchronous while the AWS
/// SDK is async-only. A driver-owned runtime (mirroring `dbflux_driver_s3`)
/// lets every call `block_on` without a Runtime-in-async-context panic.
#[allow(clippy::expect_used)]
static AWS_SECRETS_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Runtime::new().expect("AWS Secrets driver failed to construct tokio runtime")
});

pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    &AWS_SECRETS_RUNTIME
}

pub(crate) fn smithy_datetime_to_chrono(
    value: &aws_smithy_types::DateTime,
) -> Option<DateTime<Utc>> {
    let millis = value.to_millis().ok()?;
    Utc.timestamp_millis_opt(millis).single()
}

pub(crate) struct AwsSecretsConnection {
    secrets_manager: SecretsManagerStore,
    parameter_store: ParameterStore,
    /// Answers `DescribeRegions` with the connection's own credentials.
    ec2: aws_sdk_ec2::Client,
    /// The config the connection was built from, kept so the region in use is
    /// reported without asking any store.
    config: AwsSecretsProfileConfig,
    regions: RegionCatalog,
}

impl AwsSecretsConnection {
    /// Succeeds when either service answers, so credentials scoped to only
    /// one of them still connect. When both fail, the Secrets Manager error
    /// is reported.
    fn probe(&self) -> Result<(), DbError> {
        let Err(secrets_manager_error) = self.secrets_manager.probe() else {
            return Ok(());
        };

        if self.parameter_store.probe().is_ok() {
            return Ok(());
        }

        Err(secrets_manager_error)
    }
}

impl Connection for AwsSecretsConnection {
    fn metadata(&self) -> &DriverMetadata {
        &AWS_SECRETS_METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        self.probe()
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, _req: &QueryRequest) -> Result<QueryResult, DbError> {
        Err(DbError::NotSupported(
            "AWS Secrets has no query language — browse secrets and parameters from the sidebar"
                .to_string(),
        ))
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "Query cancellation is not applicable to AWS Secrets".to_string(),
        ))
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        Ok(SchemaSnapshot::default())
    }

    fn kind(&self) -> DbKind {
        DbKind::AwsSecrets
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::SingleDatabase
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }

    fn secret_stores(&self) -> Vec<&dyn SecretStoreSource> {
        vec![&self.secrets_manager, &self.parameter_store]
    }

    fn current_region(&self) -> Option<String> {
        Some(self.config.region.clone())
    }

    fn available_regions(&self) -> Result<Vec<String>, DbError> {
        self.regions
            .regions(|| describe_regions(&self.ec2, &self.config))
    }
}

#[cfg(test)]
mod tests;
