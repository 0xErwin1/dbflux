mod hierarchy;

use aws_sdk_ssm::Client;
use aws_sdk_ssm::types::{
    ParameterMetadata, ParameterStringFilter, ParameterType, ResourceTypeForTagging,
};
use dbflux_core::secrecy::ExposeSecret;
use dbflux_core::{
    DbError, FormattedError, SecretDetails, SecretListPage, SecretListRequest, SecretPayload,
    SecretPutRequest, SecretStoreCapabilities, SecretStoreDescriptor, SecretStoreSource,
    SecretSummary, SecretTag, SecretValue, SecretVersionStage,
};

use crate::driver::{AwsSecretsProfileConfig, runtime, smithy_datetime_to_chrono};
use crate::error_formatter::{
    ErrorTarget, classify_connection_error, classify_operation_error, format_service_error,
    scrub_submitted_value,
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
                    | SecretStoreCapabilities::WRITE,
            ),
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
            scrub_submitted_value(
                self.operation_error(&error, Some(&request.name)),
                &request.payload,
            )
        })?;

        Ok(SecretValue {
            version: Some(output.version().to_string()),
            payload: request.payload.clone(),
        })
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
