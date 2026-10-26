use std::collections::HashMap;

use aws_sdk_secretsmanager::Client;
use aws_sdk_secretsmanager::primitives::Blob;
use aws_sdk_secretsmanager::types::{
    Filter, FilterNameStringType, RotationRulesType, SecretListEntry, Tag,
};
use aws_smithy_types::DateTime as SmithyDateTime;
use dbflux_core::{
    DbError, FormattedError, SecretDetails, SecretListPage, SecretListRequest, SecretRotation,
    SecretStoreCapabilities, SecretStoreDescriptor, SecretStoreSource, SecretSummary, SecretTag,
    SecretValue,
};

use crate::driver::{AwsSecretsProfileConfig, runtime, smithy_datetime_to_chrono};
use crate::error_formatter::{
    ErrorTarget, classify_connection_error, classify_operation_error, format_service_error,
};

/// Descriptor id of the Secrets Manager store.
pub const SECRETS_MANAGER_ID: &str = "secrets-manager";

/// `ListSecrets` accepts 1 to 100 results per page.
const MAX_PAGE_SIZE: u32 = 100;

/// Staging label of the version `GetSecretValue` returns by default.
const CURRENT_STAGE: &str = "AWSCURRENT";

/// Key Secrets Manager encrypts with when a secret names none. `KmsKeyId` is
/// omitted from responses in that case.
const DEFAULT_KMS_KEY: &str = "aws/secretsmanager";

pub(crate) struct SecretsManagerStore {
    client: Client,
    config: AwsSecretsProfileConfig,
    descriptor: SecretStoreDescriptor,
}

impl SecretsManagerStore {
    pub(crate) fn new(client: Client, config: AwsSecretsProfileConfig) -> Self {
        Self {
            client,
            config,
            descriptor: SecretStoreDescriptor::new(
                SECRETS_MANAGER_ID,
                "Secrets Manager",
                SecretStoreCapabilities::VERSIONS
                    | SecretStoreCapabilities::VERSION_LABELS
                    | SecretStoreCapabilities::ROTATION
                    | SecretStoreCapabilities::SOFT_DELETE,
            ),
        }
    }

    /// Cheapest authenticated call: one `ListSecrets` result.
    pub(crate) fn probe(&self) -> Result<(), DbError> {
        runtime()
            .block_on(self.client.list_secrets().max_results(1).send())
            .map_err(|error| {
                classify_connection_error(format_service_error(
                    &error,
                    &self.config,
                    &ErrorTarget::Store(SECRETS_MANAGER_ID),
                ))
            })?;

        Ok(())
    }

    fn operation_error<E>(
        &self,
        error: &aws_sdk_secretsmanager::error::SdkError<E>,
        name: Option<&str>,
    ) -> DbError
    where
        E: aws_sdk_secretsmanager::error::ProvideErrorMetadata,
    {
        let target = match name {
            Some(name) => ErrorTarget::Secret {
                store: SECRETS_MANAGER_ID,
                name,
            },
            None => ErrorTarget::Store(SECRETS_MANAGER_ID),
        };

        classify_operation_error(format_service_error(error, &self.config, &target))
    }
}

impl SecretStoreSource for SecretsManagerStore {
    fn descriptor(&self) -> &SecretStoreDescriptor {
        &self.descriptor
    }

    /// `ListSecrets` with planned deletions included. `name_filter` is a
    /// `Name` filter, which Secrets Manager evaluates as a prefix match.
    fn list(&self, request: &SecretListRequest) -> Result<SecretListPage, DbError> {
        let mut call = self
            .client
            .list_secrets()
            .include_planned_deletion(true)
            .set_next_token(request.page_token.clone());

        if let Some(page_size) = request.page_size {
            call = call.max_results(clamp_page_size(page_size));
        }

        if let Some(name_filter) = request
            .name_filter
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            call = call.filters(
                Filter::builder()
                    .key(FilterNameStringType::Name)
                    .values(name_filter)
                    .build(),
            );
        }

        let output = runtime()
            .block_on(call.send())
            .map_err(|error| self.operation_error(&error, None))?;

        Ok(SecretListPage {
            entries: output
                .secret_list()
                .iter()
                .map(summary_from_entry)
                .collect(),
            child_paths: Vec::new(),
            next_page_token: output.next_token().map(ToString::to_string),
        })
    }

    fn describe(&self, name: &str) -> Result<SecretDetails, DbError> {
        let output = runtime()
            .block_on(self.client.describe_secret().secret_id(name).send())
            .map_err(|error| self.operation_error(&error, Some(name)))?;

        let versions = output.version_ids_to_stages();

        let summary = SecretSummary {
            name: output.name().unwrap_or(name).to_string(),
            description: output.description().map(ToString::to_string),
            value_type: None,
            tier: None,
            version: versions.and_then(current_version),
            last_changed: output
                .last_changed_date()
                .and_then(smithy_datetime_to_chrono),
            rotation: rotation(&RotationFields {
                enabled: output.rotation_enabled(),
                rules: output.rotation_rules(),
                function: output.rotation_lambda_arn(),
                last_rotated: output.last_rotated_date(),
                next_rotation: output.next_rotation_date(),
            }),
            owning_service: output.owning_service().map(ToString::to_string),
            deletion_scheduled_for: output.deleted_date().and_then(smithy_datetime_to_chrono),
        };

        Ok(SecretDetails {
            summary,
            resource_id: output.arn().map(ToString::to_string),
            encryption_key: Some(output.kms_key_id().unwrap_or(DEFAULT_KMS_KEY).to_string()),
            data_type: None,
            created: output.created_date().and_then(smithy_datetime_to_chrono),
            last_changed_by: None,
            last_accessed: output
                .last_accessed_date()
                .and_then(smithy_datetime_to_chrono),
            version_count: versions.map(|versions| saturating_count(versions.len())),
            version_labels: versions.map(version_labels).unwrap_or_default(),
            tags: tags(output.tags()),
            deleted_at: output.deleted_date().and_then(smithy_datetime_to_chrono),
        })
    }

    fn get_value(&self, name: &str, version: Option<&str>) -> Result<SecretValue, DbError> {
        let output = runtime()
            .block_on(
                self.client
                    .get_secret_value()
                    .secret_id(name)
                    .set_version_id(version.map(ToString::to_string))
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(name)))?;

        secret_value(
            output.version_id,
            output.secret_string,
            output.secret_binary,
        )
        .ok_or_else(|| {
            DbError::QueryFailed(
                FormattedError::new(format!("Secrets Manager returned no value for \"{name}\""))
                    .with_detail(self.config.diagnostic_detail()),
            )
        })
    }
}

fn clamp_page_size(page_size: u32) -> i32 {
    i32::try_from(page_size.clamp(1, MAX_PAGE_SIZE)).unwrap_or(1)
}

fn saturating_count(count: usize) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}

/// Builds the retrieved value from `GetSecretValue`'s two exclusive value
/// fields. `None` when the response carries neither, so a missing value is
/// an error and never an empty string.
pub(crate) fn secret_value(
    version: Option<String>,
    text: Option<String>,
    binary: Option<Blob>,
) -> Option<SecretValue> {
    if let Some(text) = text {
        return Some(SecretValue::text(version, text));
    }

    binary.map(|binary| SecretValue::binary(version, binary.into_inner()))
}

fn summary_from_entry(entry: &SecretListEntry) -> SecretSummary {
    SecretSummary {
        name: entry.name().unwrap_or_default().to_string(),
        description: entry.description().map(ToString::to_string),
        value_type: None,
        tier: None,
        version: entry.secret_versions_to_stages().and_then(current_version),
        last_changed: entry
            .last_changed_date()
            .and_then(smithy_datetime_to_chrono),
        rotation: rotation(&RotationFields {
            enabled: entry.rotation_enabled(),
            rules: entry.rotation_rules(),
            function: entry.rotation_lambda_arn(),
            last_rotated: entry.last_rotated_date(),
            next_rotation: entry.next_rotation_date(),
        }),
        owning_service: entry.owning_service().map(ToString::to_string),
        deletion_scheduled_for: entry.deleted_date().and_then(smithy_datetime_to_chrono),
    }
}

/// Rotation fields shared by `SecretListEntry` and `DescribeSecretOutput`.
struct RotationFields<'a> {
    enabled: Option<bool>,
    rules: Option<&'a RotationRulesType>,
    function: Option<&'a str>,
    last_rotated: Option<&'a SmithyDateTime>,
    next_rotation: Option<&'a SmithyDateTime>,
}

/// `None` for a secret that was never configured for rotation, which AWS
/// reports by omitting every rotation field.
fn rotation(fields: &RotationFields) -> Option<SecretRotation> {
    if fields.enabled.is_none() && fields.rules.is_none() && fields.function.is_none() {
        return None;
    }

    Some(SecretRotation {
        enabled: fields.enabled.unwrap_or(false),
        schedule: fields.rules.and_then(rotation_schedule),
        window: fields
            .rules
            .and_then(RotationRulesType::duration)
            .map(ToString::to_string),
        function: fields.function.map(ToString::to_string),
        last_rotated: fields.last_rotated.and_then(smithy_datetime_to_chrono),
        next_rotation: fields.next_rotation.and_then(smithy_datetime_to_chrono),
    })
}

/// A rule carries either a schedule expression or, in its older form, a day
/// interval, which is the same as a `rate(N days)` expression.
pub(crate) fn rotation_schedule(rules: &RotationRulesType) -> Option<String> {
    if let Some(expression) = rules.schedule_expression() {
        return Some(expression.to_string());
    }

    rules
        .automatically_after_days()
        .map(|days| format!("rate({days} days)"))
}

fn current_version(versions: &HashMap<String, Vec<String>>) -> Option<String> {
    versions
        .iter()
        .find(|(_, stages)| stages.iter().any(|stage| stage == CURRENT_STAGE))
        .map(|(version, _)| version.clone())
}

/// Every staging label in use, sorted, because the API returns them in a map
/// with no order of its own.
pub(crate) fn version_labels(versions: &HashMap<String, Vec<String>>) -> Vec<String> {
    let mut labels: Vec<String> = versions.values().flatten().cloned().collect();
    labels.sort();
    labels.dedup();
    labels
}

fn tags(tags: &[Tag]) -> Vec<SecretTag> {
    tags.iter()
        .map(|tag| {
            SecretTag::new(
                tag.key().unwrap_or_default(),
                tag.value().unwrap_or_default(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests;
