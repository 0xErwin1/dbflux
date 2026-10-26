mod hierarchy;

use aws_sdk_ssm::Client;
use aws_sdk_ssm::types::{ParameterMetadata, ParameterStringFilter, ResourceTypeForTagging};
use dbflux_core::{
    DbError, FormattedError, SecretDetails, SecretListPage, SecretListRequest,
    SecretStoreCapabilities, SecretStoreDescriptor, SecretStoreSource, SecretSummary, SecretTag,
    SecretValue,
};

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
                    | SecretStoreCapabilities::TYPED_VALUES,
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
        let output = runtime()
            .block_on(
                self.client
                    .describe_parameters()
                    .parameter_filters(string_filter("Name", "Equals", name)?)
                    .send(),
            )
            .map_err(|error| self.operation_error(&error, Some(name)))?;

        output
            .parameters()
            .iter()
            .find(|parameter| parameter.name() == Some(name))
            .cloned()
            .ok_or_else(|| {
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
