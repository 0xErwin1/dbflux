mod hierarchy;

use aws_sdk_ssm::Client;
use aws_sdk_ssm::types::{
    ParameterHistory, ParameterMetadata, ParameterStringFilter, ParameterTier, ParameterType,
    ResourceTypeForTagging,
};
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    DbError, FormattedError, SecretDeleteRequest, SecretDeletion, SecretDescriptionChange,
    SecretDescriptionUpdate, SecretDetails, SecretListPage, SecretListRequest, SecretPayload,
    SecretPutRequest, SecretStoreCapabilities, SecretStoreDescriptor, SecretStoreSource,
    SecretSummary, SecretTag, SecretTagsUpdate, SecretValue, SecretVersionInfo, SecretVersionStage,
    scrub_submitted_payload,
};
use dbflux_core::{SecretCreateChoices, SecretCreateRequest, SecretCreated};

use crate::driver::{AwsSecretsProfileConfig, runtime, smithy_datetime_to_chrono};
use crate::error_formatter::{
    ErrorTarget, classify_connection_error, classify_operation_error, format_service_error,
};

use hierarchy::{group_level, normalize_path, path_filter_value};

/// Descriptor id of the Parameter Store store.
pub const PARAMETER_STORE_ID: &str = "parameter-store";

/// `DescribeParameters` accepts 1 to 50 results per page.
const MAX_PAGE_SIZE: u32 = 50;

pub(crate) struct ParameterStore {
    client: Client,
    config: AwsSecretsProfileConfig,
    descriptor: SecretStoreDescriptor,
}

impl ParameterStore {
    pub(crate) fn new(client: Client, config: AwsSecretsProfileConfig) -> Self {
        Self {
            client,
            config,
            descriptor: SecretStoreDescriptor::new(
                PARAMETER_STORE_ID,
                "Parameter Store",
                SecretStoreCapabilities::VERSIONS
                    | SecretStoreCapabilities::HIERARCHICAL
                    | SecretStoreCapabilities::TYPED_VALUES
                    | SecretStoreCapabilities::WRITE
                    | SecretStoreCapabilities::DELETE
                    | SecretStoreCapabilities::TAGS
                    | SecretStoreCapabilities::DESCRIPTION
                    | SecretStoreCapabilities::CREATE,
            )
            .with_create_choices(create_choices()),
        }
    }

    /// Cheapest authenticated call: one `DescribeParameters` result.
    pub(crate) fn probe(&self) -> Result<(), DbError> {
        runtime()
            .block_on(self.client.describe_parameters().max_results(1).send())
            .map_err(|error| {
                classify_connection_error(format_service_error(
                    &error,
                    &self.config,
                    &ErrorTarget::Store(PARAMETER_STORE_ID),
                ))
            })?;

        Ok(())
    }

    fn operation_error<E>(
        &self,
        error: &aws_sdk_ssm::error::SdkError<E>,
        name: Option<&str>,
    ) -> DbError
    where
        E: aws_sdk_ssm::error::ProvideErrorMetadata,
    {
        let target = match name {
            Some(name) => ErrorTarget::Secret {
                store: PARAMETER_STORE_ID,
                name,
            },
            None => ErrorTarget::Store(PARAMETER_STORE_ID),
        };

        classify_operation_error(format_service_error(error, &self.config, &target))
    }

    /// The keys of the tags a parameter carries now.
    fn tag_keys(&self, name: &str) -> Result<Vec<String>, DbError> {
        let output = runtime()
            .block_on(
                self.client
                    .list_tags_for_resource()
                    .resource_type(ResourceTypeForTagging::Parameter)
                    .resource_id(name)
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(name)))?;

        Ok(output
            .tag_list()
            .iter()
            .map(|existing| existing.key().to_string())
            .collect())
    }

    /// `AddTagsToResource` for the tags of the update, when it sets any.
    fn tag(&self, update: &SecretTagsUpdate) -> Result<(), DbError> {
        if update.set.is_empty() {
            return Ok(());
        }

        runtime()
            .block_on(
                self.client
                    .add_tags_to_resource()
                    .resource_type(ResourceTypeForTagging::Parameter)
                    .resource_id(&update.name)
                    .set_tags(Some(ssm_tags(&update.set)?))
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(&update.name)))?;

        Ok(())
    }

    /// `RemoveTagsFromResource` for the keys of the update, when it removes
    /// any.
    fn untag(&self, update: &SecretTagsUpdate) -> Result<(), DbError> {
        if update.remove.is_empty() {
            return Ok(());
        }

        runtime()
            .block_on(
                self.client
                    .remove_tags_from_resource()
                    .resource_type(ResourceTypeForTagging::Parameter)
                    .resource_id(&update.name)
                    .set_tag_keys(Some(update.remove.clone()))
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(&update.name)))?;

        Ok(())
    }

    fn describe_metadata(&self, name: &str) -> Result<ParameterMetadata, DbError> {
        let filter = string_filter("Name", "Equals", name)?;
        let mut fetch_page = |token: Option<String>| {
            let output = runtime()
                .block_on(
                    self.client
                        .describe_parameters()
                        .parameter_filters(filter.clone())
                        .set_next_token(token)
                        .send(),
                )
                .map_err(|error| self.operation_error(&error, Some(name)))?;

            Ok((
                output.parameters().to_vec(),
                output.next_token().map(ToString::to_string),
            ))
        };

        find_parameter_on_pages(name, &mut fetch_page)?.ok_or_else(|| {
            DbError::ObjectNotFound(
                FormattedError::new(format!("Parameter \"{name}\" was not found"))
                    .with_hint("Check the parameter name, including its leading path.")
                    .with_detail(self.config.diagnostic_detail()),
            )
        })
    }

    fn tags(&self, name: &str) -> Result<Vec<SecretTag>, DbError> {
        let output = runtime()
            .block_on(
                self.client
                    .list_tags_for_resource()
                    .resource_type(ResourceTypeForTagging::Parameter)
                    .resource_id(name)
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(name)))?;

        Ok(output
            .tag_list()
            .iter()
            .map(|tag| SecretTag::new(tag.key(), tag.value()))
            .collect())
    }
}

impl SecretStoreSource for ParameterStore {
    fn descriptor(&self) -> &SecretStoreDescriptor {
        &self.descriptor
    }

    /// Lists through `DescribeParameters` only, which never returns values
    /// (unlike `GetParametersByPath`).
    ///
    /// No API reports the sub-paths of a path, so a one-level listing asks
    /// for everything below `path` and groups the page client-side: names
    /// with no further `/` are entries, the rest contribute their next path
    /// segment to `child_paths`. `name_filter` is a `Name` filter with the
    /// `Contains` option, a substring match on the full name.
    fn list(&self, request: &SecretListRequest) -> Result<SecretListPage, DbError> {
        let path = normalize_path(request.path.as_deref());

        let mut call = self
            .client
            .describe_parameters()
            .set_next_token(request.page_token.clone());

        if let Some(page_size) = request.page_size {
            call = call.max_results(clamp_page_size(page_size));
        }

        if let Some(path_value) = path_filter_value(&path) {
            call = call.parameter_filters(string_filter("Path", "Recursive", &path_value)?);
        }

        if let Some(name_filter) = request
            .name_filter
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            call = call.parameter_filters(string_filter("Name", "Contains", name_filter)?);
        }

        let output = runtime()
            .block_on(call.send())
            .map_err(|error| self.operation_error(&error, None))?;

        let next_page_token = output.next_token().map(ToString::to_string);
        let parameters = output.parameters.unwrap_or_default();

        let (parameters, child_paths) = if request.recursive {
            (parameters, Vec::new())
        } else {
            group_level(&path, parameters, |parameter| {
                parameter.name().unwrap_or_default()
            })
        };

        Ok(SecretListPage {
            entries: parameters.iter().map(summary_from_metadata).collect(),
            child_paths,
            next_page_token,
        })
    }

    fn describe(&self, name: &str) -> Result<SecretDetails, DbError> {
        let metadata = self.describe_metadata(name)?;
        let tags = self.tags(name)?;

        Ok(SecretDetails {
            summary: summary_from_metadata(&metadata),
            resource_id: metadata.arn().map(ToString::to_string),
            encryption_key: metadata.key_id().map(ToString::to_string),
            data_type: metadata.data_type().map(ToString::to_string),
            created: None,
            last_changed_by: metadata.last_modified_user().map(ToString::to_string),
            last_accessed: None,
            version_count: None,
            version_labels: Vec::new(),
            tags,
            deleted_at: None,
        })
    }

    /// `GetParameter` with decryption, so a `SecureString` whose key the
    /// caller may not use fails with the KMS error instead of returning the
    /// ciphertext.
    fn get_value(&self, name: &str, version: Option<&str>) -> Result<SecretValue, DbError> {
        let selector = match version {
            Some(version) => format!("{name}:{version}"),
            None => name.to_string(),
        };

        let output = runtime()
            .block_on(
                self.client
                    .get_parameter()
                    .name(selector)
                    .with_decryption(true)
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(name)))?;

        output
            .parameter
            .and_then(|parameter| {
                let version = Some(parameter.version.to_string());
                parameter
                    .value
                    .map(|value| SecretValue::text(version, value))
            })
            .ok_or_else(|| {
                DbError::QueryFailed(
                    FormattedError::new(format!(
                        "Parameter Store returned no value for \"{name}\""
                    ))
                    .with_detail(self.config.diagnostic_detail()),
                )
            })
    }

    /// `GetParameterHistory` without decryption, every page. The response
    /// carries each version's `Value` (plaintext for `String` and
    /// `StringList`), so each page is mapped field by field into
    /// [`SecretVersionInfo`] right away and the SDK entries, and the values
    /// in them, are dropped before the next page is requested.
    fn list_versions(&self, name: &str) -> Result<Vec<SecretVersionInfo>, DbError> {
        list_versions_on_pages(|token| {
            let output = runtime()
                .block_on(
                    self.client
                        .get_parameter_history()
                        .name(name)
                        .with_decryption(false)
                        .set_next_token(token)
                        .send(),
                )
                .map_err(|error| self.operation_error(&error, Some(name)))?;

            // Mapped before the next page is asked for, so the SDK entries
            // and the values in them are dropped with this closure call.
            Ok((
                page_versions(output.parameters()).collect(),
                output.next_token().map(ToString::to_string),
            ))
        })
    }

    /// One `PutParameter` call with `Overwrite`, after the checks the shape
    /// of the store demands.
    ///
    /// The call shape comes from [`put_parameter_call`], which decides
    /// everything from the parameter's metadata alone: the existing type is
    /// passed explicitly on every overwrite (`String`, `StringList` and
    /// `SecureString`), so the write never relies on how the service resolves
    /// an omitted type, and likewise the existing data type, so the service
    /// never applies its own default. A `SecureString` keeps its KMS key —
    /// the write re-encrypts, so an unknown key refuses the write instead of
    /// silently moving the value to the account default key. A missing
    /// parameter is the service's own `ParameterNotFound`, and a denied
    /// `kms:Encrypt` fails the call — both through the existing error
    /// formatter.
    fn put_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
        self.descriptor.ensure_can_write(&request.name)?;

        // The shape refusals come before any call: a staged or binary write
        // has no parameter-store shape, so the parameter is never asked for.
        // `put_parameter_call` re-derives the same text, purely.
        put_text(request)?;

        let metadata = self.describe_metadata(&request.name)?;
        let call = put_parameter_call(&metadata, request)?;

        let mut fluent = self
            .client
            .put_parameter()
            .name(&request.name)
            .value(call.value)
            .overwrite(true)
            .r#type(call.parameter_type)
            .data_type(call.data_type);

        if let Some(key_id) = call.key_id {
            fluent = fluent.key_id(key_id);
        }

        let output = runtime().block_on(fluent.send()).map_err(|error| {
            // The service may echo the submitted value in its own
            // message; a put error never carries it onward.
            scrub_submitted_payload(
                self.operation_error(&error, Some(&request.name)),
                &request.payload,
            )
        })?;

        Ok(SecretValue {
            version: Some(output.version().to_string()),
            payload: request.payload.clone(),
        })
    }

    /// One `DeleteParameter` call, which cannot be undone: the parameter
    /// and its history are gone. It is made only after the exact name was
    /// typed back, whatever the caller checked before.
    fn delete_secret(&self, request: &SecretDeleteRequest) -> Result<SecretDeletion, DbError> {
        self.descriptor.ensure_can(
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::DELETE,
            "delete",
            request.name(),
        )?;
        let name = confirmed_deletion_name(request)?;

        runtime()
            .block_on(self.client.delete_parameter().name(name).send())
            .map_err(|error| self.operation_error(&error, Some(name)))?;

        Ok(SecretDeletion::default())
    }

    /// One `PutParameter` call without `Overwrite`: Parameter Store refuses
    /// a name that exists with `ParameterAlreadyExists`, which is mapped to
    /// a constraint violation, so creating cannot replace a value. The type
    /// defaults to `String`; the tier is left to the service's own default
    /// unless one is given; a `SecureString` may name its KMS key and
    /// otherwise uses the account's default key. The tags travel in the same
    /// call, which is the only moment `PutParameter` accepts them. The
    /// service may echo the value in its own message, so a failure is
    /// scrubbed of it.
    fn create_secret(&self, request: &SecretCreateRequest) -> Result<SecretCreated, DbError> {
        self.descriptor.ensure_can(
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::CREATE,
            "create",
            &request.name,
        )?;
        let plan = plan_create(request, &self.config.region)?;

        let SecretPayload::Text(text) = &request.payload else {
            return Err(DbError::constraint_violation(format!(
                "\"{}\" was not created: a new parameter takes a text value.",
                request.name
            )));
        };

        let tags = (!plan.tags.is_empty()).then_some(plan.tags);
        let output = runtime()
            .block_on(
                self.client
                    .put_parameter()
                    .name(&request.name)
                    .value(text.expose_secret())
                    .overwrite(false)
                    .r#type(plan.parameter_type)
                    .set_tier(plan.tier)
                    .set_key_id(plan.key_id)
                    .set_description(plan.description)
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
            version: Some(output.version().to_string()),
            resource_id: None,
        })
    }

    /// `AddTagsToResource` for the tags to set (it overwrites a key that
    /// exists) and `RemoveTagsFromResource` for the keys to remove. Neither
    /// touches the value or any version. The calls are separate, so a
    /// removal that fails after the tags were set says so.
    fn update_tags(&self, update: &SecretTagsUpdate) -> Result<(), DbError> {
        self.descriptor.ensure_can(
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::TAGS,
            "edit the tags of",
            &update.name,
        )?;
        update.validate()?;

        // The service answers an unknown parameter itself, but only after
        // the tags of a different one could have been named; asking first
        // keeps the answer the same `ParameterNotFound` a read gives.
        self.describe_metadata(&update.name)?;
        let existing_keys = self.tag_keys(&update.name)?;
        update.check_quota(&existing_keys)?;

        // A parameter at its quota cannot take a new key before the old one
        // is gone, so the removal goes first and a failure after it says so.
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

    /// A parameter's description can only be changed by `PutParameter` with
    /// `Overwrite`, which writes the value and creates a version. This
    /// method does that as a value-preserving write, and refuses whenever it
    /// cannot prove it preserves everything:
    ///
    /// - the metadata is read first and [`plan_description_write`] re-sends
    ///   the existing type, data type, tier, KMS key and allowed pattern
    ///   explicitly, and refuses a parameter whose type, data type, tier or
    ///   `SecureString` key is not reported, or that has policies;
    /// - the current value is read with decryption (a denied `kms:Decrypt`
    ///   fails the whole change before anything is written), and the change
    ///   is refused when its version is not the one the metadata described;
    /// - after the write, the new version must be the very next one, or the
    ///   error says another version landed in between and points to the
    ///   history, where the earlier values are still readable.
    ///
    /// `PutParameter` has no compare-and-swap, so a value written between the
    /// read and the write is replaced by the value that was read; the
    /// version check after the write detects that and the history keeps it.
    fn update_description(
        &self,
        update: &SecretDescriptionUpdate,
    ) -> Result<SecretDescriptionChange, DbError> {
        self.descriptor.ensure_can(
            SecretStoreCapabilities::WRITE | SecretStoreCapabilities::DESCRIPTION,
            "edit the description of",
            &update.name,
        )?;

        let metadata = self.describe_metadata(&update.name)?;
        let plan = plan_description_write(&metadata, &update.name, &update.description)?;

        let read = runtime()
            .block_on(
                self.client
                    .get_parameter()
                    .name(&update.name)
                    .with_decryption(true)
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(&update.name)))?;
        let parameter = read.parameter.ok_or_else(|| {
            DbError::query_failed(format!(
                "Parameter Store returned no parameter for \"{}\"",
                update.name
            ))
        })?;
        ensure_unchanged_while_reading(plan.read_version, parameter.version, &update.name)?;
        let value = SecretString::from(parameter.value.ok_or_else(|| {
            DbError::query_failed(format!(
                "Parameter Store returned no value for \"{}\"",
                update.name
            ))
        })?);

        let current = runtime()
            .block_on(
                self.client
                    .get_parameter()
                    .name(&update.name)
                    .with_decryption(false)
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(&update.name)))?;
        ensure_unchanged_before_writing(
            plan.read_version,
            current.parameter.map(|parameter| parameter.version),
            &update.name,
        )?;

        let mut fluent = self
            .client
            .put_parameter()
            .name(&update.name)
            .value(value.expose_secret())
            .overwrite(true)
            .r#type(plan.parameter_type)
            .data_type(plan.data_type)
            .tier(plan.tier)
            .description(plan.description)
            .set_allowed_pattern(plan.allowed_pattern);

        if let Some(key_id) = plan.key_id {
            fluent = fluent.key_id(key_id);
        }

        let payload = SecretPayload::Text(value);
        let output = runtime().block_on(fluent.send()).map_err(|error| {
            description_write_error(self.operation_error(&error, Some(&update.name)), &payload)
        })?;

        ensure_written_after(plan.read_version, output.version(), &update.name)?;

        Ok(SecretDescriptionChange {
            version: Some(output.version().to_string()),
        })
    }
}

/// What a parameter create sends, decided from the request alone.
#[derive(Debug)]
pub(crate) struct CreateParameterPlan {
    pub parameter_type: ParameterType,

    /// `None` leaves the tier to the service's own default.
    pub tier: Option<ParameterTier>,

    /// The KMS key of a `SecureString`; `None` uses the account default.
    pub key_id: Option<String>,

    pub description: Option<String>,
    pub tags: Vec<aws_sdk_ssm::types::Tag>,
}

/// Most levels a hierarchical parameter name has.
const MAX_PARAMETER_LEVELS: usize = 15;

/// The types and tiers a create dialog offers: every one `plan_create`
/// accepts, so a choice the dialog shows is never refused as unknown.
fn create_choices() -> SecretCreateChoices {
    SecretCreateChoices::new(["String", "StringList", "SecureString"], "SecureString")
        .with_encrypted_value_type("SecureString")
        .with_tiers(["Standard", "Advanced", "Intelligent-Tiering"], "Standard")
}

/// Decides a parameter create, refusing before any call what the service
/// would reject, with messages that name the parameter and never the value:
/// an invalid name (letters, digits and `_ . - /`, a leading slash when it
/// has levels, at most 15 levels, no empty level, not starting with `aws` or
/// `ssm`, and short enough to fit with its ARN in the 1011 characters the
/// service counts), a type
/// or tier the service does not have, a key on a parameter that is not a
/// `SecureString`, a description past the limit, and invalid tags.
pub(crate) fn plan_create(
    request: &SecretCreateRequest,
    region: &str,
) -> Result<CreateParameterPlan, DbError> {
    request.validate()?;

    let name = &request.name;
    let refuse = |reason: String| {
        DbError::constraint_violation(format!("\"{name}\" was not created: {reason}"))
    };

    let unprefixed = name.strip_prefix('/').unwrap_or(name);
    let lowered = unprefixed.to_ascii_lowercase();
    let levels: Vec<&str> = unprefixed.split('/').collect();

    if !name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || "_.-/".contains(character))
        || levels.iter().any(|level| level.is_empty())
    {
        return Err(refuse(
            "a parameter name is letters, digits and _ . - / (with no empty level).".to_string(),
        ));
    }
    if name.contains('/') && !name.starts_with('/') {
        return Err(refuse(
            "a parameter name with levels needs its leading slash; the service would store it \
             under a different name than the one asked for."
                .to_string(),
        ));
    }
    let longest = longest_name(name, region);
    if name.chars().count() > longest {
        return Err(refuse(format!(
            "with its ARN a parameter name is at most {MAX_QUALIFIED_NAME_CHARACTERS} \
             characters, so in {region} the name takes at most {longest}."
        )));
    }
    if lowered.starts_with("aws") || lowered.starts_with("ssm") {
        return Err(refuse(
            "a parameter name cannot start with aws or ssm.".to_string(),
        ));
    }
    if levels.len() > MAX_PARAMETER_LEVELS {
        return Err(refuse(format!(
            "a parameter name has at most {MAX_PARAMETER_LEVELS} levels."
        )));
    }

    let parameter_type = match request.value_type.as_deref() {
        None | Some("String") => ParameterType::String,
        Some("StringList") => ParameterType::StringList,
        Some("SecureString") => ParameterType::SecureString,
        Some(other) => {
            return Err(refuse(format!(
                "\"{other}\" is not a parameter type (String, StringList or SecureString)."
            )));
        }
    };

    let tier = match request.tier.as_deref() {
        None => None,
        Some("Standard") => Some(ParameterTier::Standard),
        Some("Advanced") => Some(ParameterTier::Advanced),
        Some("Intelligent-Tiering") => Some(ParameterTier::IntelligentTiering),
        Some(other) => {
            return Err(refuse(format!(
                "\"{other}\" is not a parameter tier (Standard, Advanced or Intelligent-Tiering)."
            )));
        }
    };

    if request.encryption_key.is_some() && parameter_type != ParameterType::SecureString {
        return Err(refuse(
            "only a SecureString parameter takes an encryption key.".to_string(),
        ));
    }
    if let Some(description) = &request.description
        && description.chars().count() > MAX_DESCRIPTION_CHARACTERS
    {
        return Err(refuse(format!(
            "the description is longer than {MAX_DESCRIPTION_CHARACTERS} characters."
        )));
    }

    Ok(CreateParameterPlan {
        parameter_type,
        tier,
        key_id: request.encryption_key.clone(),
        description: request.description.clone(),
        tags: ssm_tags(&request.tags)?,
    })
}

/// Most characters the service counts for a parameter name together with
/// the ARN it prepends.
const MAX_QUALIFIED_NAME_CHARACTERS: usize = 1011;

/// The longest name that fits with its ARN: `arn:<partition>:ssm:<region>:
/// <12-digit account>:parameter`, plus the slash a flat name gets between
/// `parameter` and the name.
fn longest_name(name: &str, region: &str) -> usize {
    let partition = match region {
        region if region.starts_with("cn-") => "aws-cn",
        region if region.starts_with("us-gov-") => "aws-us-gov",
        region if region.starts_with("us-isob-") => "aws-iso-b",
        region if region.starts_with("us-isof-") => "aws-iso-f",
        region if region.starts_with("us-iso-") => "aws-iso",
        region if region.starts_with("eu-isoe-") => "aws-iso-e",
        _ => "aws",
    };
    let arn = format!("arn:{partition}:ssm:{region}:123456789012:parameter");
    let separator = usize::from(!name.starts_with('/'));

    MAX_QUALIFIED_NAME_CHARACTERS.saturating_sub(arn.len() + separator)
}

/// The error of a description rewrite. The call re-sends the decrypted
/// current value, so the service may echo that value in its own message;
/// it is scrubbed in every form before the error leaves the driver.
pub(crate) fn description_write_error(error: DbError, resent: &SecretPayload) -> DbError {
    scrub_submitted_payload(error, resent)
}

/// Most characters a parameter description holds.
const MAX_DESCRIPTION_CHARACTERS: usize = 1024;

/// What a value-preserving description write sends: the new description and
/// everything the parameter already has, so the write changes nothing but
/// the description and the version number.
#[derive(Debug)]
pub(crate) struct DescriptionWritePlan {
    pub description: String,
    pub parameter_type: ParameterType,
    pub data_type: String,
    pub tier: ParameterTier,
    pub key_id: Option<String>,
    pub allowed_pattern: Option<String>,

    /// The version the metadata describes, which the value read and the
    /// version written are checked against.
    pub read_version: i64,
}

/// Decides a description write from the parameter's metadata, refusing
/// before any value is read what it could not preserve: a description past
/// the limit, an empty one (a rewrite cannot clear a description, so the
/// change is not offered), a parameter with policies, and one whose type,
/// data type, tier or `SecureString` key is not reported.
pub(crate) fn plan_description_write(
    metadata: &ParameterMetadata,
    name: &str,
    description: &str,
) -> Result<DescriptionWritePlan, DbError> {
    if description.chars().count() > MAX_DESCRIPTION_CHARACTERS {
        return Err(DbError::constraint_violation(format!(
            "The description of \"{name}\" is longer than {MAX_DESCRIPTION_CHARACTERS} characters."
        )));
    }
    if description.is_empty() {
        return Err(DbError::constraint_violation(format!(
            "The description of \"{name}\" cannot be cleared; write a replacement text instead."
        )));
    }
    if !metadata.policies().is_empty() {
        return Err(DbError::query_failed(format!(
            "Parameter \"{name}\" has policies, and rewriting it to change its description \
             could not keep them; refused."
        )));
    }
    if metadata.data_type() == Some("aws:ec2:image") {
        return Err(DbError::query_failed(format!(
            "Parameter \"{name}\" is an aws:ec2:image parameter: the service validates the \
             image after it answers a write, so a rewrite could report success while the \
             description stays unchanged; refused."
        )));
    }

    let parameter_type = overwrite_type(metadata.r#type(), name)?;
    let data_type = overwrite_data_type(metadata.data_type(), name)?;
    let tier = metadata.tier().cloned().ok_or_else(|| {
        DbError::query_failed(format!(
            "Parameter Store did not report the tier of \"{name}\"; \
             rewriting it could not send the parameter's tier explicitly."
        ))
    })?;
    let key_id = if parameter_type == ParameterType::SecureString {
        Some(existing_key_id(metadata, name)?.to_string())
    } else {
        None
    };

    Ok(DescriptionWritePlan {
        description: description.to_string(),
        parameter_type,
        data_type,
        tier,
        key_id,
        allowed_pattern: metadata.allowed_pattern().map(ToString::to_string),
        read_version: metadata.version(),
    })
}

/// Refuses a description write whose value was read at a different version
/// than the metadata described: the parameter changed between the two
/// reads, and writing the value that was read would replace a newer one.
pub(crate) fn ensure_unchanged_while_reading(
    described: i64,
    read: i64,
    name: &str,
) -> Result<(), DbError> {
    if described != read {
        return Err(DbError::query_failed(format!(
            "Parameter \"{name}\" changed while its value was being read (version {described} \
             became {read}); nothing was written. Try again."
        )));
    }

    Ok(())
}

/// Refuses a description write when the parameter's version, read again
/// right before the write, is not the one the value was read at. This
/// narrows the window `PutParameter`'s lack of a compare-and-swap leaves
/// open to the time between this read and the write; it cannot close it.
pub(crate) fn ensure_unchanged_before_writing(
    read: i64,
    current: Option<i64>,
    name: &str,
) -> Result<(), DbError> {
    match current {
        Some(current) if current == read => Ok(()),
        Some(current) => Err(DbError::query_failed(format!(
            "Parameter \"{name}\" changed just before it was rewritten (version {read} became \
             {current}); nothing was written. Try again."
        ))),
        None => Err(DbError::query_failed(format!(
            "Parameter Store returned no parameter for \"{name}\" when its version was checked \
             before the write; nothing was written."
        ))),
    }
}

/// Checks that a description write produced the very next version. Any
/// other number means a version landed between the read and the write, so
/// the value sent replaced a newer one; the error points to the history.
pub(crate) fn ensure_written_after(read: i64, written: i64, name: &str) -> Result<(), DbError> {
    if written != read + 1 {
        return Err(DbError::query_failed(format!(
            "Parameter \"{name}\" was rewritten as version {written}, but version {read} was the \
             one read: another version was written in between and the value sent replaced it. \
             Its earlier values are in the history of \"{name}\"."
        )));
    }

    Ok(())
}

/// The tags of a request as the SSM SDK takes them.
pub(crate) fn ssm_tags(tags: &[SecretTag]) -> Result<Vec<aws_sdk_ssm::types::Tag>, DbError> {
    tags.iter()
        .map(|tag| {
            aws_sdk_ssm::types::Tag::builder()
                .key(&tag.key)
                .value(&tag.value)
                .build()
                .map_err(|error| DbError::query_failed(format!("Invalid tag: {error}")))
        })
        .collect()
}

/// The parameter a `DeleteParameter` call deletes, once the request is one
/// Parameter Store can honor: it has no recovery window, so a scheduled
/// deletion is refused, and the irreversible one is refused unless the name
/// was typed back exactly. Both refusals come before any call.
pub(crate) fn confirmed_deletion_name(request: &SecretDeleteRequest) -> Result<&str, DbError> {
    match request {
        SecretDeleteRequest::Scheduled { name, .. } => Err(DbError::NotSupported(format!(
            "Parameter Store has no recovery window; refused to schedule the deletion of \"{name}\"."
        ))),
        SecretDeleteRequest::Irreversible { name, .. } => {
            request.validate()?;
            Ok(name)
        }
    }
}

/// The request fields a `PutParameter` overwrite sends, decided from the
/// parameter's metadata and the put request alone. Deciding them here keeps
/// the type and key rules unit-testable without an endpoint.
#[derive(Debug)]
pub(crate) struct PutParameterCall {
    pub value: String,

    /// The parameter's existing type, sent explicitly on every overwrite.
    pub parameter_type: ParameterType,

    /// The KMS key a `SecureString` re-encrypts with, and `None` for every
    /// other type, which takes none.
    pub key_id: Option<String>,

    /// The parameter's existing data type, sent explicitly on every
    /// overwrite, so the service never applies its own default.
    pub data_type: String,
}

/// Builds the `PutParameter` overwrite fields from the parameter's metadata
/// and the put request, refusing what cannot be sent honestly: a staged or
/// binary write has no parameter-store shape, an unknown type or data type
/// refuses rather than guessing, and a `SecureString` without a reported key
/// refuses rather than re-encrypting under the account default.
pub(crate) fn put_parameter_call(
    metadata: &ParameterMetadata,
    request: &SecretPutRequest,
) -> Result<PutParameterCall, DbError> {
    let value = put_text(request)?;
    let name = &request.name;
    let parameter_type = overwrite_type(metadata.r#type(), name)?;
    let data_type = overwrite_data_type(metadata.data_type(), name)?;

    let key_id = if parameter_type == ParameterType::SecureString {
        Some(existing_key_id(metadata, name)?.to_string())
    } else {
        None
    };

    Ok(PutParameterCall {
        value,
        parameter_type,
        data_type,
        key_id,
    })
}

/// The text a `PutParameter` overwrite carries, or the refusal that stops
/// the write before any call. A staged write has no parameter-store shape,
/// because the store versions by number instead of staging labels, and a
/// binary payload has none either, because values are text only. Both
/// refusals name the store and the parameter, never the value.
pub(crate) fn put_text(request: &SecretPutRequest) -> Result<String, DbError> {
    let name = &request.name;

    if request.stage == Some(SecretVersionStage::Pending) {
        return Err(DbError::NotSupported(format!(
            "Parameter Store has no staging labels; the write to \"{name}\" cannot be staged."
        )));
    }

    match &request.payload {
        SecretPayload::Text(text) => Ok(text.expose_secret().to_owned()),
        SecretPayload::Binary(_) => Err(DbError::NotSupported(format!(
            "Parameter Store stores values as text only; the write to \"{name}\" was refused."
        ))),
    }
}

/// The type an overwrite passes to `PutParameter`: the parameter's existing
/// type, sent explicitly on every overwrite, so `String`, `StringList` and
/// `SecureString` all reach the service by name. The SDK's `PutParameter`
/// model makes the type optional when updating; DBFlux does not rely on how
/// the service resolves an omitted one. An unknown type refuses rather than
/// guessing. The refusal names the store and the parameter.
pub(crate) fn overwrite_type(
    existing: Option<&ParameterType>,
    name: &str,
) -> Result<ParameterType, DbError> {
    existing.cloned().ok_or_else(|| {
        DbError::query_failed(format!(
            "Parameter Store did not report a value type for \"{name}\"; \
             overwriting it could not send the parameter's type explicitly."
        ))
    })
}

/// The data type an overwrite passes to `PutParameter`: the parameter's
/// existing data type, sent explicitly on every overwrite, so the service
/// never applies its own default to a value that arrives without one. A
/// parameter whose metadata reports no data type refuses rather than letting
/// the service default it. The refusal names the store and the parameter.
pub(crate) fn overwrite_data_type(existing: Option<&str>, name: &str) -> Result<String, DbError> {
    existing.map(ToString::to_string).ok_or_else(|| {
        DbError::query_failed(format!(
            "Parameter Store did not report a data type for \"{name}\"; \
             overwriting it could not send the parameter's data type explicitly."
        ))
    })
}

/// The KMS key a `SecureString` overwrite re-encrypts with.
///
/// A `SecureString` write re-encrypts the value, so the parameter keeps the
/// key it is already encrypted with. A parameter whose metadata reports no
/// key refuses instead: without the key the update would silently move the
/// value to the account's default key, and changing how a value is
/// protected is not part of writing it. The refusal names the store and the
/// parameter.
pub(crate) fn existing_key_id<'a>(
    metadata: &'a ParameterMetadata,
    name: &'a str,
) -> Result<&'a str, DbError> {
    metadata.key_id().ok_or_else(|| {
        DbError::query_failed(format!(
            "Parameter Store did not report the encryption key of \"{name}\"; \
             the SecureString's key is unknown and overwriting it would \
             re-encrypt the value with the account's default key."
        ))
    })
}

/// Walks the pages a `DescribeParameters` listing is split into, looking for
/// the parameter named `name`. The fetcher sends one page: it receives the
/// page token to ask with (`None` for the first page) and answers with that
/// page's parameters and the token of the next page, or `None` when the
/// listing ended. A filtered listing is paged by the service too, so a
/// parameter that is not on the first page is still on a later one; the walk
/// ends only when a page without the parameter also ends the listing, and
/// then the parameter is absent.
pub(crate) fn find_parameter_on_pages<F>(
    name: &str,
    mut fetch_page: F,
) -> Result<Option<ParameterMetadata>, DbError>
where
    F: FnMut(Option<String>) -> Result<(Vec<ParameterMetadata>, Option<String>), DbError>,
{
    let mut token = None;
    loop {
        let (parameters, next_token) = fetch_page(token)?;

        if let Some(found) = parameters
            .iter()
            .find(|parameter| parameter.name() == Some(name))
            .cloned()
        {
            return Ok(Some(found));
        }

        match next_token {
            Some(next) => token = Some(next),
            None => return Ok(None),
        }
    }
}

/// Maps one history page field by field. `ParameterHistory::value` is never
/// read here, so a history row cannot carry the value it was built from.
pub(crate) fn page_versions(
    entries: &[ParameterHistory],
) -> impl Iterator<Item = SecretVersionInfo> + '_ {
    entries.iter().map(|entry| SecretVersionInfo {
        id: entry.version().to_string(),
        labels: entry.labels().to_vec(),
        created: entry
            .last_modified_date()
            .and_then(smithy_datetime_to_chrono),
        modified_by: entry.last_modified_user().map(ToString::to_string),
        is_current: false,
        is_pending: false,
    })
}

/// Walks every page of a `GetParameterHistory` listing. The fetcher sends
/// one page: it receives the token to ask with (`None` for the first page)
/// and answers with that page's rows, already mapped to metadata, and the
/// next token, or `None` when the listing ended. Rows are returned newest
/// version first.
pub(crate) fn list_versions_on_pages<F>(
    mut fetch_page: F,
) -> Result<Vec<SecretVersionInfo>, DbError>
where
    F: FnMut(Option<String>) -> Result<(Vec<SecretVersionInfo>, Option<String>), DbError>,
{
    let mut versions = Vec::new();
    let mut token = None;

    loop {
        let (page, next_token) = fetch_page(token)?;
        versions.extend(page);

        match next_token {
            Some(next) => token = Some(next),
            None => {
                sort_newest_first(&mut versions);
                return Ok(versions);
            }
        }
    }
}

/// Parameter versions are numbered, so the highest number is the newest, and
/// the newest is the one a read without a version serves: it alone is marked
/// current, whatever a partial page marked before.
pub(crate) fn sort_newest_first(versions: &mut [SecretVersionInfo]) {
    versions.sort_by_key(|version| std::cmp::Reverse(version.id.parse::<u64>().unwrap_or(0)));

    for (index, version) in versions.iter_mut().enumerate() {
        version.is_current = index == 0;
    }
}

fn clamp_page_size(page_size: u32) -> i32 {
    i32::try_from(page_size.clamp(1, MAX_PAGE_SIZE)).unwrap_or(1)
}

fn string_filter(key: &str, option: &str, value: &str) -> Result<ParameterStringFilter, DbError> {
    ParameterStringFilter::builder()
        .key(key)
        .option(option)
        .values(value)
        .build()
        .map_err(|error| DbError::query_failed(format!("Invalid Parameter Store filter: {error}")))
}

fn summary_from_metadata(metadata: &ParameterMetadata) -> SecretSummary {
    SecretSummary {
        name: metadata.name().unwrap_or_default().to_string(),
        description: metadata.description().map(ToString::to_string),
        value_type: metadata
            .r#type()
            .map(|value_type| value_type.as_str().to_string()),
        tier: metadata.tier().map(|tier| tier.as_str().to_string()),
        version: Some(metadata.version().to_string()),
        last_changed: metadata
            .last_modified_date()
            .and_then(smithy_datetime_to_chrono),
        rotation: None,
        owning_service: None,
        deletion_scheduled_for: None,
    }
}

#[cfg(test)]
mod tests;
