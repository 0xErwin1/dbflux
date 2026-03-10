/// AWS authentication provider implementing `AuthProvider` for SSO,
/// shared credentials, and static credentials.
///
/// SSO session validation reads cached tokens from `~/.aws/sso/cache/`
/// using the SHA-1 hash of the `sso_start_url` as the filename.
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use secrecy::SecretString;
use sha1::{Digest, Sha1};

use aws_sdk_sts::config::ProvideCredentials;

use dbflux_core::DbError;
use dbflux_core::auth::{
    AuthProfile, AuthProfileConfig, AuthSession, AuthSessionState, ResolvedCredentials, UrlCallback,
};

use crate::config::CachedAwsConfig;

const SSO_EXPIRY_BUFFER_SECS: i64 = 300;
const SSO_LOGIN_POLL_INTERVAL: Duration = Duration::from_secs(2);
const SSO_LOGIN_TIMEOUT: Duration = Duration::from_secs(300);

/// Result of launching the SSO login process before the session is confirmed.
///
/// The `verification_url` is extracted from `aws sso login` stdout and is
/// ready to be surfaced in the UI. The login is not yet complete — the caller
/// must still wait (poll the SSO cache) for the session to appear.
/// All SSO fields needed to write a complete `[profile X]` block in `~/.aws/config`.
pub struct SsoProfileConfig {
    pub profile_name: String,
    pub region: String,
    pub sso_start_url: String,
    pub sso_account_id: String,
    pub sso_role_name: String,
}

/// Writes (or updates) a `[profile <name>]` block in `~/.aws/config` with
/// all SSO fields so that `aws sso login --profile <name>` runs non-interactively.
///
/// Only writes fields that are non-empty. Existing lines for the profile are
/// replaced; unrelated parts of the file are left untouched.
pub fn ensure_aws_profile_configured(config: &SsoProfileConfig) -> Result<(), DbError> {
    // Skip if we don't have the minimum required fields to make the profile useful.
    if config.sso_start_url.trim().is_empty()
        || config.sso_account_id.trim().is_empty()
        || config.sso_role_name.trim().is_empty()
    {
        return Ok(());
    }

    let config_path = aws_config_path();
    let existing = std::fs::read_to_string(&config_path).unwrap_or_default();

    let profile_header = format!("[profile {}]", config.profile_name);
    let new_block = build_sso_profile_block(config);

    let updated = replace_or_append_profile_block(&existing, &profile_header, &new_block);

    // Ensure the directory exists before writing.
    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            DbError::ValueResolutionFailed(format!(
                "Could not create AWS config directory: {}",
                err
            ))
        })?;
    }

    std::fs::write(&config_path, updated).map_err(|err| {
        DbError::ValueResolutionFailed(format!("Could not write ~/.aws/config: {}", err))
    })
}

fn aws_config_path() -> std::path::PathBuf {
    // AWS_CONFIG_FILE env var overrides the default location.
    if let Ok(path) = std::env::var("AWS_CONFIG_FILE") {
        return std::path::PathBuf::from(path);
    }
    dirs::home_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("~"))
        .join(".aws")
        .join("config")
}

fn build_sso_profile_block(config: &SsoProfileConfig) -> String {
    let mut lines = vec![
        format!("[profile {}]", config.profile_name),
        format!("sso_start_url = {}", config.sso_start_url),
        format!("sso_region = {}", config.region),
        format!("sso_account_id = {}", config.sso_account_id),
        format!("sso_role_name = {}", config.sso_role_name),
    ];

    if !config.region.is_empty() {
        lines.push(format!("region = {}", config.region));
    }

    lines.push("output = json".to_string());
    lines.push(String::new()); // trailing newline after block

    lines.join("\n")
}

/// Replaces an existing `[profile X]` block in the INI content with
/// `new_block`, or appends it if the profile does not exist yet.
///
/// A profile block spans from its header line until the next `[` section
/// header (or end of file).
fn replace_or_append_profile_block(content: &str, header: &str, new_block: &str) -> String {
    let mut result = String::with_capacity(content.len() + new_block.len());
    let mut inside_target = false;
    let mut replaced = false;

    for line in content.lines() {
        let trimmed = line.trim();

        if trimmed == header {
            // Start of the target profile — emit the new block instead.
            result.push_str(new_block);
            inside_target = true;
            replaced = true;
            continue;
        }

        if inside_target {
            // Skip lines belonging to the old profile block.
            if trimmed.starts_with('[') {
                // Next section starts — stop skipping and emit this line.
                inside_target = false;
                result.push_str(line);
                result.push('\n');
            }
            // else: still inside old block, discard.
            continue;
        }

        result.push_str(line);
        result.push('\n');
    }

    if !replaced {
        // Profile didn't exist — append with a blank line separator.
        if !result.ends_with("\n\n") {
            if !result.ends_with('\n') {
                result.push('\n');
            }
            result.push('\n');
        }
        result.push_str(new_block);
    }

    result
}

pub struct SsoLoginHandle {
    /// The device-verification URL from `aws sso login` (e.g.
    /// `https://device.sso.us-east-1.amazonaws.com/?user_code=XXXX-XXXX`).
    /// `None` if the process started but did not emit a recognisable URL within
    /// the stdout-scan window.
    pub verification_url: Option<String>,

    /// Sender used to signal the background login thread to abort early.
    /// Sending any value sets `abort_flag` to `true`, which causes the drain
    /// thread to kill the CLI process and `wait_for_sso_session_blocking` to
    /// return an error immediately.
    pub abort_tx: std::sync::mpsc::SyncSender<()>,

    /// Shared abort flag, checked by the session-polling loop.
    /// Also accessible directly for callers that hold an `Arc` reference.
    pub(crate) abort_flag: Arc<std::sync::atomic::AtomicBool>,
}

/// Starts `aws sso login --profile <name>`, reads stdout until the
/// verification URL appears, and returns an `SsoLoginHandle`.
///
/// This is a **blocking** function intended to be called inside a thread
/// (not on the GPUI background executor, which has no Tokio runtime).
/// After getting the handle, the caller must separately wait for the
/// SSO session to appear in the token cache via `wait_for_sso_session_blocking`.
pub fn start_sso_login_blocking(profile_name: &str) -> Result<SsoLoginHandle, DbError> {
    use std::io::BufRead;
    use std::process::{Command, Stdio};

    log::debug!(
        "Spawning 'aws sso login --no-browser --profile {}'",
        profile_name
    );

    let mut child = Command::new("aws")
        .args(["sso", "login", "--no-browser", "--profile", profile_name])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| {
            DbError::ValueResolutionFailed(format!(
                "Failed to spawn 'aws sso login': {}. Is the AWS CLI installed?",
                err
            ))
        })?;

    let stdout = child.stdout.take().expect("stdout was piped");
    let stderr = child.stderr.take().expect("stderr was piped");

    // Shared abort flag. The drain thread and the session-polling loop both
    // check this flag; the caller signals abort by calling `abort_tx.send(())`.
    let abort_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let abort_flag_for_drain = Arc::clone(&abort_flag);
    let abort_flag_for_poll = Arc::clone(&abort_flag);

    let (abort_tx, abort_rx) = std::sync::mpsc::sync_channel::<()>(1);

    // Share the child handle so the drain thread can kill the process on abort.
    let child_handle = Arc::new(std::sync::Mutex::new(Some(child)));
    let child_for_drain = Arc::clone(&child_handle);

    // Forward abort channel signals to the shared flag.
    std::thread::spawn(move || {
        if abort_rx.recv().is_ok() {
            abort_flag.store(true, std::sync::atomic::Ordering::Release);
        }
    });

    // Drain stderr in a background thread so the process does not block on
    // its stderr buffer.
    std::thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            log::debug!("[aws sso login stderr] {}", line);
        }
    });

    // Scan stdout for the device-verification URL, then hand the reader to a
    // drain thread that keeps the pipe open until the process exits.
    //
    // `--no-browser` makes the AWS CLI print something like:
    //
    //   Please visit the following URL:
    //   https://example.awsapps.com/start/#/device
    //
    //   Then enter the code: XXXX-YYYY
    //
    //   Alternatively, you may visit the following URL which will autofill the code:
    //   https://example.awsapps.com/start/#/device?user_code=XXXX-YYYY
    //
    // We prefer the autofill URL (contains `user_code=`) because it is a
    // single click for the user. Fall back to any https:// URL if that line
    // is not found.
    //
    // IMPORTANT: we must NOT drop stdout before the process exits. Closing the
    // read end of the pipe sends SIGPIPE to the aws CLI process, killing it
    // before the user can complete the browser flow. We hand the BufReader to
    // a drain thread once the URL is found so the pipe stays open.
    let verification_url = {
        let mut reader = std::io::BufReader::new(stdout);
        let mut found_url: Option<String> = None;
        let mut fallback_url: Option<String> = None;

        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => break, // EOF — process exited before printing a URL
                Ok(_) => {}
                Err(_) => break,
            }

            let trimmed = line.trim().to_string();
            log::debug!("[aws sso login stdout] {}", trimmed);

            if trimmed.starts_with("https://") {
                if trimmed.contains("user_code=") {
                    found_url = Some(trimmed);
                    break; // Best URL found — drain the rest in a thread
                } else if fallback_url.is_none() {
                    fallback_url = Some(trimmed);
                    // Keep scanning — the autofill URL may be on a later line
                }
            }
        }

        // Hand the reader to a drain thread that keeps the pipe open until
        // the aws CLI process exits naturally, or until the abort flag fires.
        //
        // Dropping stdout here would close the read-end of the pipe and send
        // SIGPIPE to the CLI process, killing it before the user can approve.
        std::thread::spawn(move || {
            use std::io::BufRead;
            let mut line = String::new();
            loop {
                // Check for abort signal before each read.
                if abort_flag_for_drain.load(std::sync::atomic::Ordering::Acquire) {
                    log::debug!("[aws sso login drain] abort signalled, killing process");
                    if let Ok(mut guard) = child_for_drain.lock() {
                        if let Some(mut child) = guard.take() {
                            let _ = child.kill();
                        }
                    }
                    return;
                }

                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        log::debug!("[aws sso login stdout drain] {}", line.trim());
                        line.clear();
                    }
                }
            }
        });

        found_url.or(fallback_url)
    };

    log::debug!("AWS SSO login verification URL: {:?}", verification_url);

    // Release our copy of the child handle. The drain thread holds the other
    // Arc and will kill + drop the child if abort fires, or let it run until
    // it exits naturally when the user completes the SSO flow.
    drop(child_handle);

    Ok(SsoLoginHandle {
        verification_url,
        abort_tx,
        abort_flag: abort_flag_for_poll,
    })
}

/// Polls the SSO token cache until a valid session appears for `sso_start_url`,
/// the timeout is reached, or `abort_flag` is set to `true`.
///
/// Blocking. Call from a dedicated thread or a blocking-capable runtime.
pub fn wait_for_sso_session_blocking(
    profile_id: uuid::Uuid,
    provider_id: &str,
    sso_start_url: &str,
    abort_flag: &std::sync::atomic::AtomicBool,
) -> Result<AuthSession, DbError> {
    use std::time::Instant;

    let deadline = Instant::now() + SSO_LOGIN_TIMEOUT;

    loop {
        std::thread::sleep(SSO_LOGIN_POLL_INTERVAL);

        if abort_flag.load(std::sync::atomic::Ordering::Acquire) {
            return Err(DbError::ValueResolutionFailed(
                "AWS SSO login was cancelled".to_string(),
            ));
        }

        match validate_sso_session(sso_start_url) {
            Ok(AuthSessionState::Valid { expires_at }) => {
                return Ok(AuthSession {
                    provider_id: provider_id.to_string(),
                    profile_id,
                    expires_at,
                    data: None,
                });
            }
            Ok(_) => {}
            Err(err) => {
                log::warn!("Error during SSO session polling: {}", err);
            }
        }

        if Instant::now() >= deadline {
            return Err(DbError::ValueResolutionFailed(
                "AWS SSO login timed out after 5 minutes".to_string(),
            ));
        }
    }
}

pub struct AwsAuthProvider {
    config_cache: std::sync::Mutex<CachedAwsConfig>,
}

impl AwsAuthProvider {
    pub fn new() -> Self {
        Self {
            config_cache: std::sync::Mutex::new(CachedAwsConfig::new()),
        }
    }

    /// Returns discovered AWS profiles from `~/.aws/config`, using the
    /// mtime-based cache to avoid re-parsing every time.
    pub fn list_profiles(&self) -> Vec<crate::config::AwsProfileInfo> {
        let mut cache = self.config_cache.lock().unwrap_or_else(|e| e.into_inner());
        cache.profiles().to_vec()
    }
}

impl Default for AwsAuthProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl dbflux_core::auth::DynAuthProvider for AwsAuthProvider {
    fn provider_id(&self) -> &'static str {
        "aws"
    }

    fn display_name(&self) -> &'static str {
        "AWS"
    }

    async fn validate_session(&self, profile: &AuthProfile) -> Result<AuthSessionState, DbError> {
        match &profile.config {
            AuthProfileConfig::AwsSso {
                profile_name,
                sso_start_url,
                ..
            } => {
                let Some(url) = resolve_sso_start_url(profile_name, sso_start_url) else {
                    // No URL available — we have no way to check the cache.
                    // Treat as LoginRequired so the user gets prompted.
                    log::debug!(
                        "No sso_start_url for profile '{}', treating as LoginRequired",
                        profile_name
                    );
                    return Ok(AuthSessionState::LoginRequired);
                };

                validate_sso_session(&url)
            }
            AuthProfileConfig::AwsSharedCredentials { .. } => {
                Ok(AuthSessionState::Valid { expires_at: None })
            }
            AuthProfileConfig::AwsStaticCredentials { .. } => {
                Ok(AuthSessionState::Valid { expires_at: None })
            }
        }
    }

    async fn login(
        &self,
        profile: &AuthProfile,
        url_callback: UrlCallback,
    ) -> Result<AuthSession, DbError> {
        match &profile.config {
            AuthProfileConfig::AwsSso {
                profile_name,
                region,
                sso_start_url,
                sso_account_id,
                sso_role_name,
            } => {
                // Ensure ~/.aws/config has a complete profile block so that
                // `aws sso login` doesn't prompt the user to pick an account/role.
                let sso_config = SsoProfileConfig {
                    profile_name: profile_name.clone(),
                    region: region.clone(),
                    sso_start_url: sso_start_url.clone(),
                    sso_account_id: sso_account_id.clone(),
                    sso_role_name: sso_role_name.clone(),
                };
                if let Err(err) = ensure_aws_profile_configured(&sso_config) {
                    log::warn!("Could not write AWS profile config: {}", err);
                }
                sso_login_with_url(profile, profile_name, sso_start_url, url_callback).await
            }
            AuthProfileConfig::AwsSharedCredentials { .. }
            | AuthProfileConfig::AwsStaticCredentials { .. } => {
                url_callback(None);
                Ok(AuthSession {
                    provider_id: "aws".to_string(),
                    profile_id: profile.id,
                    expires_at: None,
                    data: None,
                })
            }
        }
    }

    async fn resolve_credentials(
        &self,
        profile: &AuthProfile,
    ) -> Result<ResolvedCredentials, DbError> {
        resolve_aws_credentials(profile).await
    }
}

/// Resolves the effective SSO start URL for a profile.
///
/// If `sso_start_url` is non-empty it is used as-is (normalized). Otherwise
/// the value is looked up from `~/.aws/config` using the profile name.
/// Returns `None` when no URL can be found.
fn resolve_sso_start_url(profile_name: &str, sso_start_url: &str) -> Option<String> {
    let url = sso_start_url.trim();

    if !url.is_empty() {
        return Some(url.to_string());
    }

    // Fall back to ~/.aws/config
    let config_path = aws_config_path();
    let contents = std::fs::read_to_string(&config_path).ok()?;
    let profiles = crate::config::parse_aws_config_str(&contents);

    profiles
        .into_iter()
        .find(|p| p.name.eq_ignore_ascii_case(profile_name))
        .and_then(|p| p.sso_start_url)
        .map(|u| u.trim().to_string())
        .filter(|u| !u.is_empty())
}

/// Checks the SSO token cache for a valid, non-expired token.
///
/// Searches by `startUrl` field inside each cache JSON rather than relying
/// solely on the filename hash. This handles mismatches caused by trailing
/// slashes — e.g. the profile stores `".../start/"` but the CLI created the
/// cache file using `".../start"` (or vice versa).
#[allow(clippy::result_large_err)]
pub(crate) fn validate_sso_session(sso_start_url: &str) -> Result<AuthSessionState, DbError> {
    let normalized_url = sso_start_url.trim_end_matches('/');

    // First try the hash-based path (fast path, works when URL is exact match).
    // Then fall back to scanning all cache files by startUrl content.
    let contents = find_sso_cache_contents(normalized_url);

    let contents = match contents {
        Some(c) => c,
        None => return Ok(AuthSessionState::LoginRequired),
    };

    let parsed: serde_json::Value = match serde_json::from_str(&contents) {
        Ok(v) => v,
        Err(err) => {
            log::warn!("Malformed SSO cache entry for '{}': {}", sso_start_url, err);
            return Ok(AuthSessionState::LoginRequired);
        }
    };

    let expires_at_str = match parsed.get("expiresAt").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return Ok(AuthSessionState::LoginRequired),
    };

    let expires_at = match parse_sso_expiry(expires_at_str) {
        Some(dt) => dt,
        None => {
            log::warn!("Unparseable expiresAt in SSO cache: {}", expires_at_str);
            return Ok(AuthSessionState::LoginRequired);
        }
    };

    let buffered_expiry = expires_at - chrono::Duration::seconds(SSO_EXPIRY_BUFFER_SECS);

    if Utc::now() >= buffered_expiry {
        Ok(AuthSessionState::Expired)
    } else {
        Ok(AuthSessionState::Valid {
            expires_at: Some(expires_at),
        })
    }
}

/// Builds an AWS `SdkConfig` for the given profile and extracts the
/// resolved credentials. The `SdkConfig` is stored in the returned
/// `ResolvedCredentials.extra` as type-erased data so that downstream
/// providers (Secrets Manager, SSM) can reuse the same session.
///
/// Spawns a dedicated OS thread that creates its own Tokio runtime, so this
/// is safe to call from async contexts without an active Tokio reactor
/// (e.g. the GPUI background executor).
async fn resolve_aws_credentials(profile: &AuthProfile) -> Result<ResolvedCredentials, DbError> {
    let profile_name = match &profile.config {
        AuthProfileConfig::AwsSso { profile_name, .. }
        | AuthProfileConfig::AwsSharedCredentials { profile_name, .. } => {
            Some(profile_name.clone())
        }
        AuthProfileConfig::AwsStaticCredentials { .. } => None,
    };
    let region = match &profile.config {
        AuthProfileConfig::AwsSso { region, .. }
        | AuthProfileConfig::AwsSharedCredentials { region, .. }
        | AuthProfileConfig::AwsStaticCredentials { region } => region.clone(),
    };

    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);

    std::thread::spawn(move || {
        let _ = result_tx.send(resolve_aws_credentials_blocking(profile_name, region));
    });

    // Non-blocking poll — yields to the executor between checks so GPUI
    // can continue processing events while credentials are being resolved.
    loop {
        match result_rx.try_recv() {
            Ok(result) => return result,
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                async_sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                return Err(DbError::ValueResolutionFailed(
                    "AWS credential resolution thread terminated unexpectedly".to_string(),
                ));
            }
        }
    }
}

/// Blocking implementation of AWS credential resolution.
/// Creates its own single-threaded Tokio runtime internally.
fn resolve_aws_credentials_blocking(
    profile_name: Option<String>,
    region: String,
) -> Result<ResolvedCredentials, DbError> {
    // The AWS SDK internally spawns tasks and uses timers that require a
    // multi-threaded Tokio runtime with a reactor. `new_current_thread` is
    // insufficient here — use `new_multi_thread` with a small thread pool.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|err| {
            DbError::ValueResolutionFailed(format!(
                "Failed to create Tokio runtime for AWS credential resolution: {}",
                err
            ))
        })?;

    runtime.block_on(async move {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.clone()));

        if let Some(name) = profile_name {
            config_loader = config_loader.profile_name(name);
        }

        let sdk_config = config_loader.load().await;

        let creds = sdk_config
            .credentials_provider()
            .ok_or_else(|| {
                DbError::ValueResolutionFailed(
                    "No credentials provider found in AWS SDK config".to_string(),
                )
            })?
            .provide_credentials()
            .await
            .map_err(|err| {
                DbError::ValueResolutionFailed(format!(
                    "Failed to resolve AWS credentials: {}",
                    err
                ))
            })?;

        let mut resolved = ResolvedCredentials {
            access_key_id: Some(creds.access_key_id().to_string()),
            secret_access_key: Some(SecretString::from(creds.secret_access_key().to_string())),
            session_token: creds
                .session_token()
                .map(|t| SecretString::from(t.to_string())),
            region: Some(region),
            extra: std::collections::HashMap::new(),
            provider_data: Some(Arc::new(sdk_config)),
        };

        if let Some(expiry) = creds.expiry() {
            let dt = chrono::DateTime::<Utc>::from(expiry);
            resolved
                .extra
                .insert("expires_at".to_string(), dt.to_rfc3339());
        }

        Ok(resolved)
    })
}

/// AWS SSO cache filenames are the SHA-1 hex digest of the start URL,
/// located at `~/.aws/sso/cache/<hash>.json`.
pub(crate) fn sso_cache_path(sso_start_url: &str) -> PathBuf {
    // Normalize by stripping trailing slashes so that
    // "https://example.awsapps.com/start" and ".../start/" hash identically.
    let normalized = sso_start_url.trim_end_matches('/');
    let hash = Sha1::digest(normalized.as_bytes());
    let hex = format!("{:x}", hash);

    sso_cache_dir().join(format!("{}.json", hex))
}

fn sso_cache_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("~"))
        .join(".aws")
        .join("sso")
        .join("cache")
}

/// Reads the SSO cache file for the given start URL.
///
/// Tries the hash-based filename first (fast path). If not found, scans all
/// `.json` files in the cache directory and returns the first one whose
/// `startUrl` field matches the normalized URL (ignoring trailing slashes).
/// Returns `None` if no matching file exists or it cannot be read.
fn find_sso_cache_contents(normalized_url: &str) -> Option<String> {
    // Fast path: exact hash match (both with and without trailing slash).
    for candidate in [normalized_url, &format!("{}/", normalized_url)] {
        let hash = Sha1::digest(candidate.as_bytes());
        let path = sso_cache_dir().join(format!("{:x}.json", hash));
        if let Ok(contents) = std::fs::read_to_string(&path) {
            log::debug!("SSO cache hit (hash match): {}", path.display());
            return Some(contents);
        }
    }

    // Slow path: scan all files and compare by startUrl field value.
    let dir = sso_cache_dir();
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(_) => return None,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }

        let contents = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let parsed: serde_json::Value = match serde_json::from_str(&contents) {
            Ok(v) => v,
            Err(_) => continue,
        };

        if let Some(url) = parsed.get("startUrl").and_then(|v| v.as_str()) {
            if url.trim_end_matches('/') == normalized_url {
                log::debug!("SSO cache hit (startUrl scan): {}", path.display());
                return Some(contents);
            }
        }
    }

    log::debug!("SSO cache miss for URL: {}", normalized_url);
    None
}

/// Runs the full AWS SSO login flow for a given profile, delivering the
/// device-verification URL to `url_callback` as soon as it is available.
///
/// The login spawns `aws sso login` in a dedicated OS thread (to avoid the
/// Tokio reactor requirement of the GPUI background executor). Once the CLI
/// prints the verification URL to stdout, `url_callback` is called **from
/// that same OS thread** so the UI state channel is updated without blocking
/// the async executor.
///
/// The async executor then polls the result channel in a non-blocking loop
/// with short sleeps so that GPUI can still process other events (including
/// delivering the updated `WaitingForLogin { url: Some(...) }` state to the
/// login modal) while the user completes the SSO flow in their browser.
async fn sso_login_with_url(
    profile: &AuthProfile,
    profile_name: &str,
    sso_start_url: &str,
    url_callback: UrlCallback,
) -> Result<AuthSession, DbError> {
    let profile_name = profile_name.to_string();
    let start_url = sso_start_url.to_string();
    let profile_id = profile.id;

    let (result_tx, result_rx) = std::sync::mpsc::sync_channel::<Result<AuthSession, DbError>>(1);

    // Spawn a dedicated OS thread for all blocking work.
    // The `url_callback` is passed into the thread and called as soon as the
    // verification URL is known, so the state channel receives the URL update
    // without any blocking on the async side.
    std::thread::spawn(move || {
        let handle = match start_sso_login_blocking(&profile_name) {
            Ok(h) => h,
            Err(err) => {
                url_callback(None);
                let _ = result_tx.send(Err(err));
                return;
            }
        };

        // Fire the callback now — the URL is known, the user may still be
        // completing the browser flow.
        url_callback(handle.verification_url);

        // Poll the token cache until the session appears, times out, or is aborted.
        let session =
            wait_for_sso_session_blocking(profile_id, "aws", &start_url, &handle.abort_flag);
        let _ = result_tx.send(session);
    });

    // Poll the result channel without blocking the async executor.
    //
    // We use a non-blocking try_recv + an async sleep so that the GPUI
    // executor can continue processing other events (including delivering
    // the WaitingForLogin URL update to the login modal) while the user
    // completes the browser flow.
    //
    // async_sleep spawns a thread to perform the std::thread::sleep and
    // signals completion through a oneshot so the executor is not blocked.
    loop {
        match result_rx.try_recv() {
            Ok(result) => return result,
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                async_sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                return Err(DbError::ValueResolutionFailed(
                    "AWS SSO login thread terminated unexpectedly".to_string(),
                ));
            }
        }
    }
}

/// Async-compatible sleep that does not block the calling executor thread.
///
/// Spawns a separate OS thread that sleeps for `duration`, then wakes the
/// async task exactly once via its `Waker`. The future returns `Pending`
/// until the thread fires, at which point it returns `Ready` on the very
/// next poll — no busy-loop, no continuous re-scheduling.
///
/// Safe to use from executors without a Tokio or async-std runtime (e.g. GPUI).
fn async_sleep(duration: std::time::Duration) -> impl std::future::Future<Output = ()> {
    SleepFuture {
        duration,
        state: SleepState::NotStarted,
    }
}

enum SleepState {
    NotStarted,
    Sleeping(std::sync::mpsc::Receiver<()>),
    Done,
}

struct SleepFuture {
    duration: std::time::Duration,
    state: SleepState,
}

impl std::future::Future for SleepFuture {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        loop {
            match &self.state {
                SleepState::NotStarted => {
                    let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);
                    let waker = cx.waker().clone();
                    let duration = self.duration;

                    std::thread::spawn(move || {
                        std::thread::sleep(duration);
                        let _ = tx.send(());
                        waker.wake();
                    });

                    self.state = SleepState::Sleeping(rx);
                    return std::task::Poll::Pending;
                }
                SleepState::Sleeping(rx) => {
                    if rx.try_recv().is_ok() {
                        self.state = SleepState::Done;
                        return std::task::Poll::Ready(());
                    }
                    // Not ready yet — remain pending; the waker will re-poll us.
                    return std::task::Poll::Pending;
                }
                SleepState::Done => {
                    return std::task::Poll::Ready(());
                }
            }
        }
    }
}

/// Fully blocking SSO login: spawns the AWS CLI, reads the URL from stdout,
/// then polls the token cache until the session appears or times out.
///
/// Safe to call from a plain OS thread with no async runtime. Used by the
/// Settings UI login button which runs on the GPUI background executor
/// (which has no Tokio reactor).
pub fn login_sso_blocking(
    profile_id: uuid::Uuid,
    profile_name: &str,
    sso_start_url: &str,
    sso_region: &str,
    sso_account_id: &str,
    sso_role_name: &str,
) -> Result<AuthSession, DbError> {
    ensure_aws_profile_configured(&SsoProfileConfig {
        profile_name: profile_name.to_string(),
        region: sso_region.to_string(),
        sso_start_url: sso_start_url.to_string(),
        sso_account_id: sso_account_id.to_string(),
        sso_role_name: sso_role_name.to_string(),
    })?;

    let handle = start_sso_login_blocking(profile_name)?;
    log::debug!(
        "AWS SSO login started for profile '{}', verification URL: {:?}",
        profile_name,
        handle.verification_url
    );

    // No external abort signal for the Settings UI path — use a flag that
    // is never set so the poll runs to completion or timeout.
    wait_for_sso_session_blocking(profile_id, "aws", sso_start_url, &handle.abort_flag)
}

/// AWS SSO tokens use ISO 8601 / RFC 3339 format for `expiresAt`, but
/// some versions omit the timezone suffix. We try multiple formats.
pub(crate) fn parse_sso_expiry(s: &str) -> Option<DateTime<Utc>> {
    // Try RFC 3339 first (has timezone)
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }

    // AWS sometimes uses format without timezone, assume UTC
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.and_utc());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_sso_cache(dir: &std::path::Path, start_url: &str, json: &str) {
        let hash = Sha1::digest(start_url.as_bytes());
        let hex = format!("{:x}", hash);
        let path = dir.join(format!("{}.json", hex));
        let mut file = std::fs::File::create(path).unwrap();
        file.write_all(json.as_bytes()).unwrap();
    }

    #[test]
    fn valid_sso_token_returns_valid() {
        let tmp = tempfile::tempdir().unwrap();
        let start_url = "https://test-valid.awsapps.com/start";
        let future_time = (Utc::now() + chrono::Duration::hours(1))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();

        let json = format!(
            r#"{{"startUrl":"{}","accessToken":"token123","expiresAt":"{}"}}"#,
            start_url, future_time
        );
        write_sso_cache(tmp.path(), start_url, &json);

        // Override the cache dir by testing the underlying function with a
        // constructed path
        let hash = Sha1::digest(start_url.as_bytes());
        let hex = format!("{:x}", hash);
        let cache_file = tmp.path().join(format!("{}.json", hex));
        let contents = std::fs::read_to_string(&cache_file).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
        let expires_str = parsed["expiresAt"].as_str().unwrap();
        let expires_at = parse_sso_expiry(expires_str).unwrap();

        assert!(Utc::now() < expires_at);
    }

    #[test]
    fn expired_sso_token_is_detected() {
        let past_time = (Utc::now() - chrono::Duration::hours(1))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();

        let expires_at = parse_sso_expiry(&past_time).unwrap();
        let buffered = expires_at - chrono::Duration::seconds(SSO_EXPIRY_BUFFER_SECS);
        assert!(Utc::now() >= buffered);
    }

    #[test]
    fn malformed_json_returns_login_required() {
        let result = validate_sso_from_str("not valid json {{{");
        assert!(matches!(result, AuthSessionState::LoginRequired));
    }

    #[test]
    fn missing_expires_at_returns_login_required() {
        let result = validate_sso_from_str(r#"{"startUrl":"https://test.com","accessToken":"x"}"#);
        assert!(matches!(result, AuthSessionState::LoginRequired));
    }

    #[test]
    fn shared_credentials_always_valid() {
        let profile = AuthProfile::new(
            "test-shared",
            "aws",
            AuthProfileConfig::AwsSharedCredentials {
                profile_name: "default".to_string(),
                region: "us-east-1".to_string(),
            },
        );

        let provider = AwsAuthProvider::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let result = rt.block_on(async {
            dbflux_core::auth::DynAuthProvider::validate_session(&provider, &profile).await
        });

        let state = result.unwrap();
        assert!(matches!(
            state,
            AuthSessionState::Valid { expires_at: None }
        ));
    }

    #[test]
    fn sso_cache_path_uses_sha1() {
        let url = "https://my-sso.awsapps.com/start";
        let path = sso_cache_path(url);

        let expected_hash = format!("{:x}", Sha1::digest(url.as_bytes()));
        assert!(path.to_string_lossy().contains(&expected_hash));
        assert!(path.to_string_lossy().ends_with(".json"));
    }

    #[test]
    fn parse_expiry_with_and_without_timezone() {
        let with_tz = parse_sso_expiry("2025-06-15T14:30:25Z");
        assert!(with_tz.is_some());

        let without_tz = parse_sso_expiry("2025-06-15T14:30:25");
        assert!(without_tz.is_some());

        let invalid = parse_sso_expiry("not-a-date");
        assert!(invalid.is_none());
    }

    /// Helper: validates SSO session from a raw JSON string, bypassing
    /// the filesystem cache path lookup.
    fn validate_sso_from_str(json: &str) -> AuthSessionState {
        let parsed: serde_json::Value = match serde_json::from_str(json) {
            Ok(v) => v,
            Err(_) => return AuthSessionState::LoginRequired,
        };

        let expires_at_str = match parsed.get("expiresAt").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return AuthSessionState::LoginRequired,
        };

        let expires_at = match parse_sso_expiry(expires_at_str) {
            Some(dt) => dt,
            None => return AuthSessionState::LoginRequired,
        };

        let buffered_expiry = expires_at - chrono::Duration::seconds(SSO_EXPIRY_BUFFER_SECS);

        if Utc::now() >= buffered_expiry {
            AuthSessionState::Expired
        } else {
            AuthSessionState::Valid {
                expires_at: Some(expires_at),
            }
        }
    }
}
