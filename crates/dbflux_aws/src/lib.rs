mod accounts;
mod auth;
mod config;
mod parameters;
mod secrets;

use dbflux_core::DbError;
use dbflux_core::auth::{AuthProfile, AuthProfileConfig};

pub use accounts::{
    AwsSsoAccount, list_sso_account_roles, list_sso_account_roles_blocking, list_sso_accounts,
    list_sso_accounts_blocking,
};
pub use auth::{
    AwsAuthProvider, SsoLoginHandle, login_sso_blocking, start_sso_login_blocking,
    wait_for_sso_session_blocking,
};
pub use config::{AwsProfileInfo, CachedAwsConfig};
pub use parameters::AwsSsmParameterProvider;
pub use secrets::AwsSecretsManagerProvider;

/// Build AWS value providers (Secrets Manager + SSM Parameter Store)
/// for a given auth profile.
///
/// Requires a Tokio runtime. Use `value_providers_for_auth_profile_blocking`
/// when calling from contexts without one (e.g. GPUI background executor).
pub async fn value_providers_for_auth_profile(
    profile: &AuthProfile,
) -> Result<(AwsSecretsManagerProvider, AwsSsmParameterProvider), DbError> {
    let (profile_name, region) = aws_profile_name_and_region(profile);

    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_string()));

    if let Some(profile_name) = profile_name {
        loader = loader.profile_name(profile_name);
    }

    let sdk_config = loader.load().await;

    Ok((
        AwsSecretsManagerProvider::new(sdk_config.clone()),
        AwsSsmParameterProvider::new(sdk_config),
    ))
}

/// Build AWS value providers using a blocking Tokio runtime.
///
/// Safe to call from GPUI background executor tasks and other non-async
/// contexts. Creates its own single-threaded Tokio runtime internally.
pub fn value_providers_for_auth_profile_blocking(
    profile: &AuthProfile,
) -> Result<(AwsSecretsManagerProvider, AwsSsmParameterProvider), DbError> {
    let (profile_name, region) = aws_profile_name_and_region(profile);
    let profile_name = profile_name.map(|s| s.to_string());
    let region = region.to_string();

    // The AWS SDK spawns internal tasks and uses timers that require a
    // multi-threaded runtime with a reactor.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|err| {
            DbError::ValueResolutionFailed(format!(
                "Failed to create Tokio runtime for AWS provider init: {}",
                err
            ))
        })?;

    runtime.block_on(async move {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region));

        if let Some(name) = profile_name {
            loader = loader.profile_name(name);
        }

        let sdk_config = loader.load().await;

        Ok((
            AwsSecretsManagerProvider::new(sdk_config.clone()),
            AwsSsmParameterProvider::new(sdk_config),
        ))
    })
}

fn aws_profile_name_and_region(profile: &AuthProfile) -> (Option<&str>, &str) {
    match &profile.config {
        AuthProfileConfig::AwsSso {
            profile_name,
            region,
            ..
        }
        | AuthProfileConfig::AwsSharedCredentials {
            profile_name,
            region,
        } => (Some(profile_name.as_str()), region.as_str()),
        AuthProfileConfig::AwsStaticCredentials { region } => (None, region.as_str()),
    }
}
