use std::collections::HashMap;

use aws_sdk_secretsmanager::Client;
use aws_sdk_secretsmanager::primitives::Blob;
use aws_sdk_secretsmanager::types::{
    Filter, FilterNameStringType, RotationRulesType, SecretListEntry, Tag,
};
use aws_smithy_types::DateTime as SmithyDateTime;
use dbflux_core::secrecy::ExposeSecret;
use dbflux_core::{
    DbError, FormattedError, SecretDetails, SecretListPage, SecretListRequest, SecretPayload,
    SecretPutRequest, SecretRotation, SecretStoreCapabilities, SecretStoreDescriptor,
    SecretStoreSource, SecretSummary, SecretTag, SecretValue, SecretVersionStage,
};

use crate::driver::{AwsSecretsProfileConfig, runtime, smithy_datetime_to_chrono};
use crate::error_formatter::{
    ErrorTarget, classify_connection_error, classify_operation_error, format_service_error,
};

/// Descriptor id of the Secrets Manager store.
pub const SECRETS_MANAGER_ID: &str = "secrets-manager";

/// `ListSecrets` accepts 1 to 100 results per page.
const MAX_PAGE_SIZE: u32 = 100;

/// Marks of the wrapped page token, one per filter, so a listing keeps paging
/// with the filter its token belongs to.
const NAME_TOKEN_MARK: &str = "sst1n:";
const ALL_TOKEN_MARK: &str = "sst1a:";

/// The filter a listing page is asked with, the service token to send, and
/// whether the page belongs to the `Name` fallback listing. A marked token
/// keeps that listing on its own filter on every page, so a token from one
/// filter is never sent with the other.
fn page_filter(page_token: Option<&str>) -> (FilterNameStringType, Option<String>, bool) {
    match page_token.map(decode_page_token) {
        Some(Some((key, service_token))) => {
            let in_name_listing = matches!(key, FilterNameStringType::Name);
            (key, Some(service_token), in_name_listing)
        }
        // A token this store did not wrap is passed back as the service gave
        // it, with the word filter it was asked with.
        _ => (
            FilterNameStringType::All,
            page_token.map(ToString::to_string),
            false,
        ),
    }
}

/// Whether a page that matched nothing is asked again with the `Name` filter:
/// only for a search, only on the first page, never while a prefix listing is
/// being paged, and only when the empty page ended the listing. An empty page
/// that carries a next token is a page of a filtered listing, not its end, so
/// falling back there would drop the secrets the following pages hold.
fn needs_prefix_fallback(
    entries_are_empty: bool,
    has_next_page: bool,
    search: Option<&str>,
    first_page: bool,
    in_prefix_listing: bool,
) -> bool {
    entries_are_empty && !has_next_page && search.is_some() && first_page && !in_prefix_listing
}

/// Wraps the filter a page token belongs to and the service token itself.
///
/// `ListSecrets` tokens are opaque, so no unreserved prefix can be taken from
/// their namespace: every token this store returns carries this wrapper, and
/// the service token travels hex-encoded inside it, which no wrapper mark can
/// be confused with.
fn encode_page_token(key: &FilterNameStringType, token: &str) -> String {
    let mark = if matches!(key, FilterNameStringType::Name) {
        NAME_TOKEN_MARK
    } else {
        ALL_TOKEN_MARK
    };
    format!("{mark}{}", hex_encode(token))
}

/// The filter and the service token behind a wrapped one, or `None` for a
/// token this store did not produce.
fn decode_page_token(token: &str) -> Option<(FilterNameStringType, String)> {
    let (mark, encoded) = [NAME_TOKEN_MARK, ALL_TOKEN_MARK]
        .into_iter()
        .find_map(|mark| token.strip_prefix(mark).map(|rest| (mark, rest)))?;
    let key = if mark == NAME_TOKEN_MARK {
        FilterNameStringType::Name
    } else {
        FilterNameStringType::All
    };
    Some((key, hex_decode(encoded)?))
}

fn hex_encode(value: &str) -> String {
    value.bytes().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_decode(value: &str) -> Option<String> {
    if value.is_empty() || !value.len().is_multiple_of(2) {
        return None;
    }
    let bytes = value
        .as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok())
        .collect::<Option<Vec<u8>>>()?;
    String::from_utf8(bytes).ok()
}

/// Staging label of the version `GetSecretValue` returns by default.
const CURRENT_STAGE: &str = "AWSCURRENT";

/// Key Secrets Manager encrypts with when a secret names none. `KmsKeyId` is
/// omitted from responses in that case.
const DEFAULT_KMS_KEY: &str = "aws/secretsmanager";

/// Staging label a `Pending` write attaches to the new version.
const PENDING_STAGE: &str = "AWSPENDING";

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
                    | SecretStoreCapabilities::SOFT_DELETE
                    | SecretStoreCapabilities::WRITE,
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

impl SecretsManagerStore {
    /// One `ListSecrets` page, with the filter the caller chose and, when the
    /// page came from the prefix fallback, a token that says so.
    fn list_page(
        &self,
        request: &SecretListRequest,
        filter_key: FilterNameStringType,
        search: Option<&str>,
        token: Option<&str>,
    ) -> Result<SecretListPage, DbError> {
        let mut call = self
            .client
            .list_secrets()
            .include_planned_deletion(true)
            .set_next_token(token.map(ToString::to_string));

        if let Some(page_size) = request.page_size {
            call = call.max_results(clamp_page_size(page_size));
        }

        if let Some(search) = search {
            call = call.filters(
                Filter::builder()
                    .key(filter_key.clone())
                    .values(search)
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
            next_page_token: output
                .next_token()
                .map(|token| encode_page_token(&filter_key, token)),
        })
    }
}

impl SecretStoreSource for SecretsManagerStore {
    fn descriptor(&self) -> &SecretStoreDescriptor {
        &self.descriptor
    }

    /// `ListSecrets` with planned deletions included. `name_filter` is an
    /// `all` filter: Secrets Manager splits names, descriptions and tags into
    /// words and matches each word by prefix, ignoring case, so a word in the
    /// middle of a path finds the secret. The `Name` filter only matches the
    /// start of the full name, case-sensitively.
    ///
    /// An endpoint that treats `all` as a name prefix instead (LocalStack
    /// does) still matches a typed prefix and finds nothing else. An endpoint
    /// that does not implement `all` at all answers it with no entries, so a
    /// search that finds nothing is asked once more with the `Name` filter,
    /// which is what this store used before: the fallback narrows a search
    /// rather than widening it, and costs one request only when the word
    /// filter matched nothing. Its pages carry a wrapped token so the rest of
    /// that listing keeps using the filter its token belongs to.
    fn list(&self, request: &SecretListRequest) -> Result<SecretListPage, DbError> {
        let search = request
            .name_filter
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());

        let (key, token, in_prefix_listing) = page_filter(request.page_token.as_deref());

        let page = self.list_page(request, key, search, token.as_deref())?;

        let fall_back = needs_prefix_fallback(
            page.entries.is_empty(),
            page.next_page_token.is_none(),
            search,
            token.is_none(),
            in_prefix_listing,
        );
        match (fall_back, search) {
            (true, Some(search)) => {
                self.list_page(request, FilterNameStringType::Name, Some(search), None)
            }
            _ => Ok(page),
        }
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

    /// One `PutSecretValue` call with the new payload, after the checks the
    /// service does not make.
    ///
    /// Secrets another service owns are read only in DBFlux, and AWS does not
    /// enforce that, so the metadata is asked for first and the write is
    /// refused with the owning service named. A secret scheduled for deletion
    /// enforces itself: the service rejects `PutSecretValue` with
    /// `InvalidRequestException`, which the error formatter already maps.
    fn put_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
        self.descriptor.ensure_can_write(&request.name)?;

        let metadata = runtime()
            .block_on(
                self.client
                    .describe_secret()
                    .secret_id(&request.name)
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(&request.name)))?;

        if let Some(refusal) = managed_write_refusal_for(metadata.owning_service(), &request.name) {
            return Err(refusal);
        }

        let mut call = self.client.put_secret_value().secret_id(&request.name);
        call = match put_payload(&request.payload) {
            PutPayload::Text(text) => call.secret_string(text),
            PutPayload::Binary(bytes) => call.secret_binary(Blob::new(bytes)),
        };

        if let Some(label) = write_stage_label(request.stage) {
            call = call.version_stages(label);
        }

        let output = runtime()
            .block_on(call.send())
            .map_err(|error| self.operation_error(&error, Some(&request.name)))?;

        Ok(SecretValue {
            version: output.version_id().map(ToString::to_string),
            payload: request.payload.clone(),
        })
    }
}

/// The staging label the requested write stage maps to. `None` keeps the
/// service default, under which a write moves `AWSCURRENT` to the new
/// version without naming it, the way the console's own edit does.
pub(crate) fn write_stage_label(stage: Option<SecretVersionStage>) -> Option<&'static str> {
    match stage {
        None | Some(SecretVersionStage::Current) => None,
        Some(SecretVersionStage::Pending) => Some(PENDING_STAGE),
    }
}

/// The value fields a `PutSecretValue` call carries, mapped from the seam's
/// payload. Exactly one is set, like `GetSecretValue`'s answer.
pub(crate) enum PutPayload {
    Text(String),
    Binary(Vec<u8>),
}

pub(crate) fn put_payload(payload: &SecretPayload) -> PutPayload {
    match payload {
        SecretPayload::Text(text) => PutPayload::Text(text.expose_secret().to_owned()),
        SecretPayload::Binary(bytes) => PutPayload::Binary(bytes.expose_secret().to_vec()),
    }
}

/// DBFlux keeps secrets another service owns read only (see
/// `SecretSummary::is_managed`), and AWS does not enforce it, so a write to
/// one is refused before the write call, naming the store, the owning
/// service and the secret. `None` for an unmanaged secret, which the write
/// proceeds with.
pub(crate) fn managed_write_refusal_for(owning: Option<&str>, name: &str) -> Option<DbError> {
    owning.map(|service| managed_write_refusal(service, name))
}

pub(crate) fn managed_write_refusal(service: &str, name: &str) -> DbError {
    DbError::PermissionDenied(
        FormattedError::new(format!(
            "Secrets Manager secret \"{name}\" is managed by {service} and is read only"
        ))
        .with_hint("Its value is maintained by the service that owns it."),
    )
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
