use std::path::Path;

use chrono::{DateTime, Utc};
use dbflux_core::DbError;

use crate::auth::{parse_sso_expiry, sso_cache_path};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsSsoAccount {
    pub account_id: String,
    pub account_name: String,
    pub email_address: Option<String>,
}

pub async fn list_sso_accounts(
    profile_name: &str,
    region: &str,
    sso_start_url: &str,
) -> Result<Vec<AwsSsoAccount>, DbError> {
    validate_list_inputs(profile_name, region, sso_start_url)?;

    let access_token = load_valid_access_token(profile_name, sso_start_url)?;
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_string()))
        .profile_name(profile_name)
        .load()
        .await;

    let client = aws_sdk_sso::Client::new(&sdk_config);
    let mut accounts = Vec::new();
    let mut next_token: Option<String> = None;

    loop {
        let output = client
            .list_accounts()
            .access_token(access_token.clone())
            .set_next_token(next_token.clone())
            .send()
            .await
            .map_err(|err| map_sso_error("ListAccounts", profile_name, err.to_string()))?;

        for account in output.account_list() {
            let account_id = account.account_id().unwrap_or_default().to_string();
            if account_id.is_empty() {
                continue;
            }

            accounts.push(AwsSsoAccount {
                account_id,
                account_name: account.account_name().unwrap_or_default().to_string(),
                email_address: account.email_address().map(ToString::to_string),
            });
        }

        next_token = output.next_token().map(ToString::to_string);
        if next_token.is_none() {
            break;
        }
    }

    accounts.sort_by(|left, right| {
        left.account_name
            .cmp(&right.account_name)
            .then_with(|| left.account_id.cmp(&right.account_id))
    });
    accounts.dedup_by(|left, right| left.account_id == right.account_id);

    Ok(accounts)
}

pub fn list_sso_accounts_blocking(
    profile_name: &str,
    region: &str,
    sso_start_url: &str,
) -> Result<Vec<AwsSsoAccount>, DbError> {
    run_with_local_runtime(list_sso_accounts(profile_name, region, sso_start_url))
}

pub async fn list_sso_account_roles(
    profile_name: &str,
    region: &str,
    sso_start_url: &str,
    account_id: &str,
) -> Result<Vec<String>, DbError> {
    validate_list_inputs(profile_name, region, sso_start_url)?;

    if account_id.trim().is_empty() {
        return Err(DbError::InvalidProfile(
            "AWS SSO account ID is required to list roles".to_string(),
        ));
    }

    let access_token = load_valid_access_token(profile_name, sso_start_url)?;
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_string()))
        .profile_name(profile_name)
        .load()
        .await;

    let client = aws_sdk_sso::Client::new(&sdk_config);
    let mut roles = Vec::new();
    let mut next_token: Option<String> = None;

    loop {
        let output = client
            .list_account_roles()
            .access_token(access_token.clone())
            .account_id(account_id)
            .set_next_token(next_token.clone())
            .send()
            .await
            .map_err(|err| map_sso_error("ListAccountRoles", profile_name, err.to_string()))?;

        for role in output.role_list() {
            if let Some(role_name) = role.role_name()
                && !role_name.is_empty()
            {
                roles.push(role_name.to_string());
            }
        }

        next_token = output.next_token().map(ToString::to_string);
        if next_token.is_none() {
            break;
        }
    }

    roles.sort();
    roles.dedup();

    Ok(roles)
}

pub fn list_sso_account_roles_blocking(
    profile_name: &str,
    region: &str,
    sso_start_url: &str,
    account_id: &str,
) -> Result<Vec<String>, DbError> {
    run_with_local_runtime(list_sso_account_roles(
        profile_name,
        region,
        sso_start_url,
        account_id,
    ))
}

fn validate_list_inputs(
    profile_name: &str,
    region: &str,
    sso_start_url: &str,
) -> Result<(), DbError> {
    if profile_name.trim().is_empty() {
        return Err(DbError::InvalidProfile(
            "AWS profile name is required for SSO account listing".to_string(),
        ));
    }

    if region.trim().is_empty() {
        return Err(DbError::InvalidProfile(
            "AWS region is required for SSO account listing".to_string(),
        ));
    }

    if sso_start_url.trim().is_empty() {
        return Err(DbError::InvalidProfile(
            "AWS SSO start URL is required for SSO account listing".to_string(),
        ));
    }

    Ok(())
}

fn load_valid_access_token(profile_name: &str, sso_start_url: &str) -> Result<String, DbError> {
    let target_start_url = normalize_start_url(sso_start_url);
    let cache_path = sso_cache_path(sso_start_url);

    if cache_path.exists() {
        let cache_entry = load_cache_entry(&cache_path)?;

        if !start_url_matches(&cache_entry.start_url, target_start_url.as_str()) {
            return Err(DbError::ValueResolutionFailed(format!(
                "Login required: AWS SSO cache '{}' does not match start URL '{}'; run 'aws sso login --profile {}'",
                cache_path.display(),
                sso_start_url,
                profile_name
            )));
        }

        if Utc::now() < cache_entry.expires_at {
            return Ok(cache_entry.access_token);
        }

        return Err(DbError::ValueResolutionFailed(format!(
            "Login required: AWS SSO session expired for profile '{}'; run 'aws sso login --profile {}'",
            profile_name, profile_name
        )));
    }

    let cache_dir = cache_path.parent().map(Path::to_path_buf).ok_or_else(|| {
        DbError::ValueResolutionFailed(format!(
            "Login required: invalid AWS SSO cache path '{}'; run 'aws sso login --profile {}'",
            cache_path.display(),
            profile_name
        ))
    })?;

    let cache_entries = std::fs::read_dir(&cache_dir).map_err(|err| {
        DbError::ValueResolutionFailed(format!(
            "Login required: failed to read AWS SSO cache directory '{}': {}. Run 'aws sso login --profile {}'",
            cache_dir.display(),
            err,
            profile_name
        ))
    })?;

    let mut freshest_match: Option<CacheEntry> = None;
    let mut saw_matching_entry = false;

    for entry_result in cache_entries {
        let entry = match entry_result {
            Ok(value) => value,
            Err(err) => {
                log::debug!("Skipping unreadable AWS SSO cache entry: {}", err);
                continue;
            }
        };

        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let Some(cache_entry) = load_cache_entry_for_scan(&path) else {
            continue;
        };

        if !start_url_matches(&cache_entry.start_url, target_start_url.as_str()) {
            continue;
        }

        saw_matching_entry = true;

        if Utc::now() >= cache_entry.expires_at {
            continue;
        }

        let should_replace = freshest_match
            .as_ref()
            .map(|current| cache_entry.expires_at > current.expires_at)
            .unwrap_or(true);

        if should_replace {
            freshest_match = Some(cache_entry);
        }
    }

    if let Some(entry) = freshest_match {
        return Ok(entry.access_token);
    }

    if saw_matching_entry {
        return Err(DbError::ValueResolutionFailed(format!(
            "Login required: AWS SSO session expired for profile '{}'; run 'aws sso login --profile {}'",
            profile_name, profile_name
        )));
    }

    Err(DbError::ValueResolutionFailed(format!(
        "Login required: run 'aws sso login --profile {}'",
        profile_name
    )))
}

#[derive(Debug)]
struct CacheEntry {
    start_url: String,
    access_token: String,
    expires_at: DateTime<Utc>,
}

fn load_cache_entry(path: &Path) -> Result<CacheEntry, DbError> {
    let contents = std::fs::read_to_string(path).map_err(|err| {
        DbError::ValueResolutionFailed(format!(
            "failed to read AWS SSO cache '{}': {}",
            path.display(),
            err
        ))
    })?;

    let parsed: serde_json::Value = serde_json::from_str(&contents).map_err(|err| {
        DbError::ValueResolutionFailed(format!(
            "invalid AWS SSO cache '{}': {}",
            path.display(),
            err
        ))
    })?;

    let start_url = parsed
        .get("startUrl")
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            DbError::ValueResolutionFailed(format!(
                "AWS SSO cache '{}' missing startUrl",
                path.display()
            ))
        })?;

    let access_token = parsed
        .get("accessToken")
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            DbError::ValueResolutionFailed(format!(
                "AWS SSO cache '{}' missing access token",
                path.display()
            ))
        })?;

    let expires_at_str = parsed
        .get("expiresAt")
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            DbError::ValueResolutionFailed(format!(
                "AWS SSO cache '{}' missing expiry",
                path.display()
            ))
        })?;

    let expires_at = parse_sso_expiry(expires_at_str).ok_or_else(|| {
        DbError::ValueResolutionFailed(format!(
            "AWS SSO cache '{}' has invalid expiry",
            path.display()
        ))
    })?;

    Ok(CacheEntry {
        start_url,
        access_token,
        expires_at,
    })
}

fn load_cache_entry_for_scan(path: &Path) -> Option<CacheEntry> {
    let contents = match std::fs::read_to_string(path) {
        Ok(value) => value,
        Err(err) => {
            log::debug!(
                "Skipping unreadable AWS SSO cache entry '{}': {}",
                path.display(),
                err
            );
            return None;
        }
    };

    let parsed: serde_json::Value = match serde_json::from_str(&contents) {
        Ok(value) => value,
        Err(err) => {
            log::debug!(
                "Skipping invalid AWS SSO cache entry '{}': {}",
                path.display(),
                err
            );
            return None;
        }
    };

    let start_url = match parsed
        .get("startUrl")
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
        .filter(|value| !value.trim().is_empty())
    {
        Some(value) => value,
        None => {
            log::debug!(
                "Skipping AWS SSO cache entry '{}' without startUrl",
                path.display()
            );
            return None;
        }
    };

    let access_token = match parsed
        .get("accessToken")
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
        .filter(|value| !value.is_empty())
    {
        Some(value) => value,
        None => {
            log::debug!(
                "Skipping AWS SSO cache entry '{}' without access token",
                path.display()
            );
            return None;
        }
    };

    let expires_at = match parsed
        .get("expiresAt")
        .and_then(|value| value.as_str())
        .and_then(parse_sso_expiry)
    {
        Some(value) => value,
        None => {
            log::debug!(
                "Skipping AWS SSO cache entry '{}' with invalid expiry",
                path.display()
            );
            return None;
        }
    };

    Some(CacheEntry {
        start_url,
        access_token,
        expires_at,
    })
}

fn normalize_start_url(value: &str) -> String {
    value.trim().trim_end_matches('/').to_string()
}

fn start_url_matches(left: &str, right: &str) -> bool {
    normalize_start_url(left) == normalize_start_url(right)
}

fn map_sso_error(operation: &str, profile_name: &str, message: String) -> DbError {
    let lower = message.to_lowercase();
    if lower.contains("unauthorized")
        || lower.contains("expiredtoken")
        || lower.contains("invalidtoken")
    {
        return DbError::ValueResolutionFailed(format!(
            "Login required: run 'aws sso login --profile {}'",
            profile_name
        ));
    }

    DbError::ValueResolutionFailed(format!("AWS SSO {} failed: {}", operation, message))
}

fn run_with_local_runtime<F, T>(future: F) -> Result<T, DbError>
where
    F: std::future::Future<Output = Result<T, DbError>>,
{
    // The AWS SDK spawns internal tasks and uses timers that require a
    // multi-threaded runtime with a reactor.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|err| {
            DbError::ValueResolutionFailed(format!("Failed to create Tokio runtime: {}", err))
        })?;

    runtime.block_on(future)
}
