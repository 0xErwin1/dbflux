use std::collections::HashMap;

use aws_sdk_secretsmanager::Client;
use aws_sdk_secretsmanager::operation::describe_secret::DescribeSecretOutput;
use aws_sdk_secretsmanager::primitives::Blob;
use aws_sdk_secretsmanager::types::{
    Filter, FilterNameStringType, RotationRulesType, SecretListEntry, SecretVersionsListEntry, Tag,
};
use aws_smithy_types::DateTime as SmithyDateTime;
use dbflux_core::secrecy::ExposeSecret;
use dbflux_core::{
    DbError, FormattedError, SecretDeleteRequest, SecretDeletion, SecretDescriptionChange,
    SecretDescriptionUpdate, SecretDetails, SecretListPage, SecretListRequest, SecretPayload,
    SecretPutRequest, SecretRollback, SecretRollbackRequest, SecretRotation, SecretRotationChange,
    SecretRotationStart, SecretRotationUpdate, SecretStoreCapabilities, SecretStoreDescriptor,
    SecretStoreSource, SecretSummary, SecretTag, SecretTagsUpdate, SecretValue, SecretVersionInfo,
    SecretVersionStage, scrub_submitted_payload, validate_rotation_window_fits,
};
use dbflux_core::{SecretCreateRequest, SecretCreated};

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
                    | SecretStoreCapabilities::WRITE
                    | SecretStoreCapabilities::DELETE
                    | SecretStoreCapabilities::TAGS
                    | SecretStoreCapabilities::DESCRIPTION
                    | SecretStoreCapabilities::CREATE,
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
    /// `TagResource` for the tags of the update, when it sets any.
    fn tag(&self, update: &SecretTagsUpdate) -> Result<(), DbError> {
        if update.set.is_empty() {
            return Ok(());
        }

        runtime()
            .block_on(
                self.client
                    .tag_resource()
                    .secret_id(&update.name)
                    .set_tags(Some(sdk_tags(&update.set)))
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(&update.name)))?;

        Ok(())
    }

    /// `UntagResource` for the keys of the update, when it removes any.
    fn untag(&self, update: &SecretTagsUpdate) -> Result<(), DbError> {
        if update.remove.is_empty() {
            return Ok(());
        }

        runtime()
            .block_on(
                self.client
                    .untag_resource()
                    .secret_id(&update.name)
                    .set_tag_keys(Some(update.remove.clone()))
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(&update.name)))?;

        Ok(())
    }

    /// `DescribeSecret`, refusing a secret another service owns: DBFlux
    /// keeps those read only and AWS does not enforce it, so every write
    /// that is not a value put asks first and names the owner.
    fn describe_unmanaged(&self, name: &str) -> Result<DescribeSecretOutput, DbError> {
        let metadata = runtime()
            .block_on(self.client.describe_secret().secret_id(name).send())
            .map_err(|error| self.operation_error(&error, Some(name)))?;

        match managed_write_refusal_for(metadata.owning_service(), name) {
            Some(refusal) => Err(refusal),
            None => Ok(metadata),
        }
    }

    /// Every version of `name`, deprecated ones included, following every
    /// page of `ListSecretVersionIds`. `DescribeSecret` omits versions with
    /// no staging label, so it cannot tell whether such a version exists.
    fn version_entries(&self, name: &str) -> Result<Vec<SecretVersionsListEntry>, DbError> {
        collect_version_entries(|token| {
            let output = runtime()
                .block_on(
                    self.client
                        .list_secret_version_ids()
                        .secret_id(name)
                        .include_deprecated(true)
                        .set_next_token(token)
                        .send(),
                )
                .map_err(|error| self.operation_error(&error, Some(name)))?;

            Ok((
                output.versions().to_vec(),
                output.next_token().map(ToString::to_string),
            ))
        })
    }

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

        Ok(details_from_describe(name, &output))
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

    /// `ListSecretVersionIds` with deprecated versions included, because a
    /// version with no staging label is still a version of the secret. Every
    /// page is followed, and the metadata is mapped page by page.
    fn list_versions(&self, name: &str) -> Result<Vec<SecretVersionInfo>, DbError> {
        list_versions_on_pages(|token| {
            let output = runtime()
                .block_on(
                    self.client
                        .list_secret_version_ids()
                        .secret_id(name)
                        .include_deprecated(true)
                        .set_next_token(token)
                        .send(),
                )
                .map_err(|error| self.operation_error(&error, Some(name)))?;

            Ok((
                output.versions().to_vec(),
                output.next_token().map(ToString::to_string),
            ))
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

        let output = runtime().block_on(call.send()).map_err(|error| {
            // The service may echo the submitted value in its own
            // message; a put error never carries it onward.
            scrub_submitted_payload(
                self.operation_error(&error, Some(&request.name)),
                &request.payload,
            )
        })?;

        Ok(SecretValue {
            version: output.version_id().map(ToString::to_string),
            payload: request.payload.clone(),
        })
    }

    /// One `UpdateSecretVersionStage` call moving `AWSCURRENT` to the chosen
    /// version and removing it from the version that holds it. Secrets
    /// Manager attaches `AWSPREVIOUS` to the version `AWSCURRENT` was moved
    /// away from by itself, so one call moves both labels and no value is
    /// rewritten or created.
    ///
    /// The metadata is asked for first: a secret another service owns is
    /// refused naming the owner, and the plan names the version that holds
    /// `AWSCURRENT` right now, so the call fails at the service when that
    /// changed in between instead of moving the wrong label.
    fn roll_back(&self, request: &SecretRollbackRequest) -> Result<SecretRollback, DbError> {
        self.descriptor.ensure_can(
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::VERSION_LABELS,
            "roll back",
            &request.name,
        )?;
        request.validate()?;

        self.describe_unmanaged(&request.name)?;
        let stages = stages_by_version(&self.version_entries(&request.name)?);
        let plan = plan_rollback(&stages, &request.name, &request.version_id)?;

        runtime()
            .block_on(
                self.client
                    .update_secret_version_stage()
                    .secret_id(&request.name)
                    .version_stage(CURRENT_STAGE)
                    .move_to_version_id(&plan.to_version)
                    .remove_from_version_id(&plan.from_version)
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(&request.name)))?;

        Ok(SecretRollback {
            current_version: plan.to_version,
            previous_version: Some(plan.from_version),
        })
    }

    /// One `DeleteSecret` call with the recovery window, never with
    /// `ForceDeleteWithoutRecovery`. A secret another service owns is
    /// refused naming the owner; one already scheduled is rejected by the
    /// service itself.
    fn delete_secret(&self, request: &SecretDeleteRequest) -> Result<SecretDeletion, DbError> {
        self.descriptor.ensure_can(
            SecretStoreCapabilities::WRITE
                | SecretStoreCapabilities::DELETE
                | SecretStoreCapabilities::SOFT_DELETE,
            "delete",
            request.name(),
        )?;
        let days = deletion_window(request)?;

        let metadata = self.describe_unmanaged(request.name())?;
        ensure_not_scheduled(metadata.deleted_date(), request.name())?;

        let output = runtime()
            .block_on(delete_secret_call(&self.client, request.name(), days).send())
            .map_err(|error| self.operation_error(&error, Some(request.name())))?;

        Ok(deletion_from_date(output.deletion_date()))
    }

    /// One `RotateSecret` call that rotates now with the rotation already
    /// configured, only on a secret whose rotation is enabled and has a
    /// function. The service runs the function asynchronously: the answer
    /// names the version the rotation created, and the value changes when
    /// the function finishes.
    fn rotate_now(&self, name: &str) -> Result<SecretRotationStart, DbError> {
        self.descriptor.ensure_can(
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::ROTATION,
            "rotate",
            name,
        )?;

        let metadata = self.describe_unmanaged(name)?;
        ensure_rotatable(
            metadata.rotation_enabled(),
            metadata.rotation_lambda_arn(),
            name,
        )?;

        let output = runtime()
            .block_on(
                self.client
                    .rotate_secret()
                    .secret_id(name)
                    .rotate_immediately(true)
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(name)))?;

        Ok(SecretRotationStart {
            version: output.version_id().map(ToString::to_string),
        })
    }

    /// Turns automatic rotation on with `RotateSecret` — rules and function
    /// set, `RotateImmediately` false so editing never rotates — or off with
    /// `CancelRotateSecret`, which leaves the pending stage of a rotation in
    /// progress where it was; the version that holds it is reported so the
    /// caller can say the labels need reconciling before another rotation.
    fn update_rotation(
        &self,
        update: &SecretRotationUpdate,
    ) -> Result<SecretRotationChange, DbError> {
        self.descriptor.ensure_can(
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::ROTATION,
            "edit the rotation of",
            &update.name,
        )?;
        update.validate()?;

        let metadata = self.describe_unmanaged(&update.name)?;
        let unfinished_version = unfinished_rotation_version(metadata.version_ids_to_stages());
        let plan = plan_rotation_update(
            update,
            metadata.rotation_enabled() == Some(true),
            metadata.rotation_lambda_arn(),
            metadata
                .rotation_rules()
                .and_then(RotationRulesType::duration),
        )?;

        match plan {
            RotationPlan::Enable {
                schedule,
                window,
                function,
            } => {
                let rules = rotation_rules(schedule, window);

                runtime()
                    .block_on(
                        self.client
                            .rotate_secret()
                            .secret_id(&update.name)
                            .rotate_immediately(false)
                            .rotation_rules(rules)
                            .set_rotation_lambda_arn(function)
                            .send(),
                    )
                    .map_err(|error| self.operation_error(&error, Some(&update.name)))?;
            }
            RotationPlan::Disable => {
                runtime()
                    .block_on(
                        self.client
                            .cancel_rotate_secret()
                            .secret_id(&update.name)
                            .send(),
                    )
                    .map_err(|error| self.operation_error(&error, Some(&update.name)))?;

                return Ok(SecretRotationChange { unfinished_version });
            }
        }

        Ok(SecretRotationChange::default())
    }

    /// One `CreateSecret` call with the name, the text value, the
    /// description, the KMS key and the tags. It never overwrites: Secrets
    /// Manager refuses a name that exists with `ResourceExistsException`,
    /// which is mapped to a constraint violation. A secret created without a
    /// key uses the account's `aws/secretsmanager` key. The service may echo
    /// the value in its own message, so a failure is scrubbed of it.
    fn create_secret(&self, request: &SecretCreateRequest) -> Result<SecretCreated, DbError> {
        self.descriptor.ensure_can(
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::CREATE,
            "create",
            &request.name,
        )?;
        validate_create(request)?;

        let PutPayload::Text(text) = put_payload(&request.payload) else {
            return Err(DbError::constraint_violation(format!(
                "\"{}\" was not created: a new secret takes a text value.",
                request.name
            )));
        };

        let tags = (!request.tags.is_empty()).then(|| sdk_tags(&request.tags));
        let output = runtime()
            .block_on(
                self.client
                    .create_secret()
                    .name(&request.name)
                    .secret_string(text)
                    .set_description(request.description.clone())
                    .set_kms_key_id(request.encryption_key.clone())
                    .set_tags(tags)
                    .send(),
            )
            .map_err(|error| {
                scrub_submitted_payload(
                    self.operation_error(&error, Some(&request.name)),
                    &request.payload,
                )
            })?;

        Ok(SecretCreated {
            version: output.version_id().map(ToString::to_string),
            resource_id: output.arn().map(ToString::to_string),
        })
    }

    /// `TagResource` for the tags to set (it overwrites a key that exists)
    /// and `UntagResource` for the keys to remove. Neither touches the value
    /// or any version. A secret another service owns is refused. The two
    /// calls are separate, so a removal that fails after the tags were set
    /// says so.
    fn update_tags(&self, update: &SecretTagsUpdate) -> Result<(), DbError> {
        self.descriptor.ensure_can(
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::TAGS,
            "edit the tags of",
            &update.name,
        )?;
        update.validate()?;

        let metadata = self.describe_unmanaged(&update.name)?;
        let existing_keys: Vec<String> = metadata
            .tags()
            .iter()
            .filter_map(|existing| existing.key().map(ToString::to_string))
            .collect();
        update.check_quota(&existing_keys)?;

        // A secret at its quota cannot take a new key before the old one is
        // gone, so the removal goes first and a failure after it says so.
        if update.removes_first(&existing_keys) {
            self.untag(update)?;

            return self.tag(update).map_err(|error| {
                DbError::query_failed(format!(
                    "The tags of \"{}\" were removed, but setting the others failed: {error}",
                    update.name
                ))
            });
        }

        self.tag(update)?;

        self.untag(update).map_err(|error| {
            if update.set.is_empty() {
                error
            } else {
                DbError::query_failed(format!(
                    "The tags of \"{}\" were set, but removing the others failed: {error}",
                    update.name
                ))
            }
        })
    }

    /// `UpdateSecret` with the description and nothing else: no secret
    /// value and no KMS key are sent, so the call creates no version and
    /// changes nothing about how the value is protected.
    fn update_description(
        &self,
        update: &SecretDescriptionUpdate,
    ) -> Result<SecretDescriptionChange, DbError> {
        self.descriptor.ensure_can(
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::DESCRIPTION,
            "edit the description of",
            &update.name,
        )?;
        ensure_description_fits(&update.description, &update.name)?;

        self.describe_unmanaged(&update.name)?;

        runtime()
            .block_on(
                self.client
                    .update_secret()
                    .secret_id(&update.name)
                    .description(&update.description)
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(&update.name)))?;

        Ok(SecretDescriptionChange::default())
    }

    /// One `RestoreSecret` call, which cancels a scheduled deletion. A
    /// secret another service owns is refused; one that is not scheduled
    /// for deletion is rejected by the service itself.
    fn restore_secret(&self, name: &str) -> Result<(), DbError> {
        self.descriptor.ensure_can(
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::SOFT_DELETE,
            "restore",
            name,
        )?;

        let metadata = self.describe_unmanaged(name)?;
        ensure_scheduled(metadata.deleted_date(), name)?;

        runtime()
            .block_on(self.client.restore_secret().secret_id(name).send())
            .map_err(|error| self.operation_error(&error, Some(name)))?;

        Ok(())
    }
}

/// The recovery window a `DeleteSecret` call carries. Secrets Manager
/// deletes with a recovery window only: the service's
/// `ForceDeleteWithoutRecovery` is never sent, so an irreversible request
/// is refused, whatever it confirms.
pub(crate) fn deletion_window(request: &SecretDeleteRequest) -> Result<i64, DbError> {
    match request {
        SecretDeleteRequest::Scheduled {
            recovery_window_days,
            ..
        } => {
            request.validate()?;
            Ok(i64::from(*recovery_window_days))
        }
        SecretDeleteRequest::Irreversible { name, .. } => Err(DbError::NotSupported(format!(
            "Secrets Manager deletes with a recovery window only; refused to delete \"{name}\" \
             without recovery."
        ))),
    }
}

/// Refuses a secret name the service would reject: 1 to 512 letters,
/// digits and `/ _ + = . @ -`.
pub(crate) fn validate_secret_name(name: &str) -> Result<(), DbError> {
    let length = name.chars().count();

    if length == 0
        || length > 512
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "/_+=.@-".contains(c))
    {
        return Err(DbError::constraint_violation(format!(
            "\"{name}\" is not a valid secret name: 1 to 512 letters, digits and / _ + = . @ -"
        )));
    }

    Ok(())
}

/// Refuses a create the service would reject, before any call, with
/// messages that name the secret and never the value: an invalid name or
/// tags, a type or a tier (a Secrets Manager secret has neither), and a
/// description past the limit.
pub(crate) fn validate_create(request: &SecretCreateRequest) -> Result<(), DbError> {
    request.validate()?;
    validate_secret_name(&request.name)?;

    if request.value_type.is_some() || request.tier.is_some() {
        return Err(DbError::NotSupported(format!(
            "Secrets Manager secrets have no value type or tier; refused to create \"{}\".",
            request.name
        )));
    }
    if let Some(description) = &request.description {
        ensure_description_fits(description, &request.name)?;
    }

    Ok(())
}

/// The tags of a request as the Secrets Manager SDK takes them. An empty
/// value is kept: a tag with no value is a valid tag.
pub(crate) fn sdk_tags(tags: &[SecretTag]) -> Vec<Tag> {
    tags.iter()
        .map(|tag| Tag::builder().key(&tag.key).value(&tag.value).build())
        .collect()
}

/// Most characters a secret description holds.
const MAX_DESCRIPTION_CHARACTERS: usize = 2048;

/// Refuses a description the service would reject. An empty one is valid
/// and clears the description.
pub(crate) fn ensure_description_fits(description: &str, name: &str) -> Result<(), DbError> {
    if description.chars().count() > MAX_DESCRIPTION_CHARACTERS {
        return Err(DbError::constraint_violation(format!(
            "The description of \"{name}\" is longer than {MAX_DESCRIPTION_CHARACTERS} characters."
        )));
    }

    Ok(())
}

/// What a rotation edit sends.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RotationPlan {
    /// `RotateSecret` with rules and without rotating now. `function` is
    /// `None` when the secret keeps the one it has.
    Enable {
        schedule: String,
        window: Option<String>,
        function: Option<String>,
    },

    /// `CancelRotateSecret`.
    Disable,
}

/// The rotation rules a `RotateSecret` call sends. The rules replace the
/// ones on the secret, so a window that is not sent is a window dropped.
pub(crate) fn rotation_rules(schedule: String, window: Option<String>) -> RotationRulesType {
    RotationRulesType::builder()
        .schedule_expression(schedule)
        .set_duration(window)
        .build()
}

/// Decides a rotation edit from the update and the secret's current
/// rotation, refusing before any call what the service would reject or
/// what would not change anything: an invalid update, enabling with no
/// function anywhere, and turning off rotation that is already off.
/// `RotateSecret` replaces the rules, so an update that names no window
/// re-sends the window the secret already has instead of dropping it.
pub(crate) fn plan_rotation_update(
    update: &SecretRotationUpdate,
    enabled: bool,
    existing_function: Option<&str>,
    existing_window: Option<&str>,
) -> Result<RotationPlan, DbError> {
    update.validate()?;

    if !update.enabled {
        if !enabled {
            return Err(DbError::query_failed(format!(
                "Rotation of \"{}\" is not enabled, so there is nothing to turn off",
                update.name
            )));
        }

        return Ok(RotationPlan::Disable);
    }

    if update.function.is_none() && existing_function.is_none() {
        return Err(DbError::query_failed(format!(
            "Rotation of \"{}\" needs a rotation function, and the secret has none",
            update.name
        )));
    }

    let schedule = update.schedule.clone().unwrap_or_default();
    let window = update
        .window
        .clone()
        .or_else(|| existing_window.map(ToString::to_string));

    if let Some(window) = &window {
        validate_rotation_window_fits(&schedule, window).map_err(|reason| {
            DbError::constraint_violation(format!(
                "The rotation of \"{}\" was not changed: {reason}",
                update.name
            ))
        })?;
    }

    Ok(RotationPlan::Enable {
        schedule,
        window,
        function: update.function.clone(),
    })
}

/// Refuses rotating now unless rotation is enabled with a function.
/// `RotateSecret` also switches automatic rotation on for the secret, so
/// calling it on one with rotation off would enable it as a side effect;
/// the driver refuses instead and the user turns rotation on by editing it.
pub(crate) fn ensure_rotatable(
    enabled: Option<bool>,
    function: Option<&str>,
    name: &str,
) -> Result<(), DbError> {
    if enabled != Some(true) {
        return Err(DbError::query_failed(format!(
            "Rotation of \"{name}\" is not enabled; enable it before rotating"
        )));
    }

    if function.is_none() {
        return Err(DbError::query_failed(format!(
            "Secrets Manager secret \"{name}\" has no rotation function to run"
        )));
    }

    Ok(())
}

/// Refuses scheduling the deletion of a secret whose deletion is already
/// scheduled, so the recovery window of the first request is not silently
/// replaced.
pub(crate) fn ensure_not_scheduled(
    deleted: Option<&SmithyDateTime>,
    name: &str,
) -> Result<(), DbError> {
    match deleted {
        Some(_) => Err(DbError::query_failed(format!(
            "Secrets Manager secret \"{name}\" is already scheduled for deletion"
        ))),
        None => Ok(()),
    }
}

/// Refuses restoring a secret that is not scheduled for deletion, so the
/// answer does not depend on how an endpoint treats a restore with nothing
/// to restore.
pub(crate) fn ensure_scheduled(
    deleted: Option<&SmithyDateTime>,
    name: &str,
) -> Result<(), DbError> {
    match deleted {
        Some(_) => Ok(()),
        None => Err(DbError::query_failed(format!(
            "Secrets Manager secret \"{name}\" is not scheduled for deletion"
        ))),
    }
}

/// The details a `DescribeSecret` answer reports.
///
/// `DeletedDate` is the date the deletion takes effect, so it fills the
/// scheduled date only: the service reports neither the moment the deletion
/// was requested nor the recovery window, and neither is inferred.
pub(crate) fn details_from_describe(name: &str, output: &DescribeSecretOutput) -> SecretDetails {
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

    SecretDetails {
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
        deleted_at: None,
    }
}

/// The outcome of `DeleteSecret`: the date the service says the deletion
/// takes effect.
pub(crate) fn deletion_from_date(date: Option<&SmithyDateTime>) -> SecretDeletion {
    SecretDeletion {
        deletion_scheduled_for: date.and_then(smithy_datetime_to_chrono),
    }
}

/// Where a roll back moves `AWSCURRENT` from and to.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RollbackPlan {
    pub from_version: String,
    pub to_version: String,
}

/// The `DeleteSecret` call: the name and the recovery window, and nothing
/// else. `ForceDeleteWithoutRecovery` is never set, so a deletion made
/// through DBFlux can always be restored inside its window.
pub(crate) fn delete_secret_call(
    client: &Client,
    name: &str,
    recovery_window_days: i64,
) -> aws_sdk_secretsmanager::operation::delete_secret::builders::DeleteSecretFluentBuilder {
    client
        .delete_secret()
        .secret_id(name)
        .recovery_window_in_days(recovery_window_days)
}

/// The staging labels of every version `ListSecretVersionIds` reported,
/// keyed by version id. A version without a label is in the map with an
/// empty list.
pub(crate) fn stages_by_version(
    entries: &[SecretVersionsListEntry],
) -> HashMap<String, Vec<String>> {
    entries
        .iter()
        .filter_map(|entry| {
            Some((
                entry.version_id()?.to_string(),
                entry.version_stages().to_vec(),
            ))
        })
        .collect()
}

/// Decides a roll back from the stages of every version: the target must be
/// a known version other than the one holding `AWSCURRENT`, and a secret
/// with no current version cannot be rolled back, because the label would
/// have nothing to move away from.
pub(crate) fn plan_rollback(
    versions: &HashMap<String, Vec<String>>,
    name: &str,
    target: &str,
) -> Result<RollbackPlan, DbError> {
    if !versions.contains_key(target) {
        return Err(DbError::object_not_found(format!(
            "Secrets Manager secret \"{name}\" has no version \"{target}\""
        )));
    }

    let Some(current) = current_version(versions) else {
        return Err(DbError::query_failed(format!(
            "Secrets Manager secret \"{name}\" has no current version to roll back from"
        )));
    };

    if current == target {
        return Err(DbError::query_failed(format!(
            "Version \"{target}\" of \"{name}\" is already the current version"
        )));
    }

    if versions
        .get(target)
        .is_some_and(|stages| stages.iter().any(|stage| stage == PENDING_STAGE))
    {
        return Err(DbError::query_failed(format!(
            "Version \"{target}\" of \"{name}\" holds {PENDING_STAGE}, an unfinished rotation; \
             rolling back onto it would promote a value the rotation has not validated."
        )));
    }

    Ok(RollbackPlan {
        from_version: current,
        to_version: target.to_string(),
    })
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

    rules.automatically_after_days().map(|days| match days {
        1 => "rate(1 day)".to_string(),
        days => format!("rate({days} days)"),
    })
}

/// Walks every page of a `ListSecretVersionIds` listing. The fetcher sends
/// one page: it receives the token to ask with (`None` for the first page)
/// and answers with that page's entries and the next token, or `None` when
/// the listing ended.
pub(crate) fn list_versions_on_pages<F>(fetch_page: F) -> Result<Vec<SecretVersionInfo>, DbError>
where
    F: FnMut(Option<String>) -> Result<(Vec<SecretVersionsListEntry>, Option<String>), DbError>,
{
    Ok(versions_from_entries(&collect_version_entries(fetch_page)?))
}

/// The raw entries of every page, for callers that need the staging labels
/// as the service reports them (a roll back plans from them).
pub(crate) fn collect_version_entries<F>(
    mut fetch_page: F,
) -> Result<Vec<SecretVersionsListEntry>, DbError>
where
    F: FnMut(Option<String>) -> Result<(Vec<SecretVersionsListEntry>, Option<String>), DbError>,
{
    let mut entries = Vec::new();
    let mut token = None;

    loop {
        let (page, next_token) = fetch_page(token)?;
        entries.extend(page);

        match next_token {
            Some(next) => token = Some(next),
            None => return Ok(entries),
        }
    }
}

/// The history rows of `ListSecretVersionIds` entries, newest first by
/// `CreatedDate`. An entry without a creation date sorts last, and one
/// without a version id cannot be selected, so it is dropped. The sort is
/// stable, so versions with equal dates keep the order the service returned
/// them in, which AWS does not specify.
pub(crate) fn versions_from_entries(entries: &[SecretVersionsListEntry]) -> Vec<SecretVersionInfo> {
    let mut versions: Vec<SecretVersionInfo> = entries
        .iter()
        .filter_map(|entry| {
            let labels = entry.version_stages().to_vec();

            Some(SecretVersionInfo {
                id: entry.version_id()?.to_string(),
                is_current: labels.iter().any(|stage| stage == CURRENT_STAGE),
                is_pending: labels.iter().any(|stage| stage == PENDING_STAGE),
                labels,
                created: entry.created_date().and_then(smithy_datetime_to_chrono),
                modified_by: None,
            })
        })
        .collect();

    versions.sort_by_key(|version| std::cmp::Reverse(version.created));
    versions
}

/// The version a cancelled rotation leaves holding `AWSPENDING` without
/// `AWSCURRENT`, if any; the lowest id when several do, so the answer is
/// stable.
pub(crate) fn unfinished_rotation_version(
    versions: Option<&HashMap<String, Vec<String>>>,
) -> Option<String> {
    let mut unfinished: Vec<&String> = versions?
        .iter()
        .filter(|(_, stages)| {
            stages.iter().any(|stage| stage == PENDING_STAGE)
                && !stages.iter().any(|stage| stage == CURRENT_STAGE)
        })
        .map(|(version, _)| version)
        .collect();
    unfinished.sort();

    unfinished.first().map(|version| version.to_string())
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
