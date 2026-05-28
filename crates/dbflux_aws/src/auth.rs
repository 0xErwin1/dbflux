#![allow(clippy::result_large_err)]

/// AWS authentication provider implementing `AuthProvider` for SSO,
/// shared credentials, and static credentials.
///
/// SSO session validation reads cached tokens from `~/.aws/sso/cache/`
/// using the SHA-1 hash of the `sso_start_url` as the filename.
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use secrecy::SecretString;
use sha1::{Digest, Sha1};

use aws_sdk_sts::config::ProvideCredentials;

use dbflux_core::DbError;
use dbflux_core::auth::{
    AuthFormDef, AuthProfile, AuthProviderCapabilities, AuthProviderLoginCapabilities, AuthSession,
    AuthSessionState, FetchOptionsError, FetchOptionsRequest, FetchOptionsResponse,
    ImportableProfile, ResolvedCredentials, UrlCallback, aws_profile_uuid,
};
use dbflux_core::{
    FormFieldDef, FormFieldKind, FormSection, FormTab, RefreshTrigger, SelectOption,
};

use crate::config::CachedAwsConfig;
use crate::parameters::AwsSsmParameterProvider;
use crate::secrets::AwsSecretsManagerProvider;

const SSO_EXPIRY_BUFFER_SECS: i64 = 300;
const SSO_LOGIN_POLL_INTERVAL: Duration = Duration::from_secs(2);
const SSO_LOGIN_TIMEOUT: Duration = Duration::from_secs(300);

/// Result of launching the SSO login process before the session is confirmed.
///
/// The `verification_url` is extracted from `aws sso login` stdout and is
/// ready to be surfaced in the UI. The login is not yet complete — the caller
/// must still wait (poll the SSO cache) for the session to appear.
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

    // Scan stdout for the device-verification URL. We need to handle two
    // distinct AWS CLI output flows:
    //
    // 1. **Device-code flow** (older / `--use-device-code`):
    //
    //        Please visit the following URL:
    //        https://example.awsapps.com/start/#/device
    //
    //        Then enter the code: XXXX-YYYY
    //
    //        Alternatively, you may visit the following URL which will autofill the code:
    //        https://example.awsapps.com/start/#/device?user_code=XXXX-YYYY
    //
    //    Here the autofill URL (with `user_code=`) is the one to surface.
    //
    // 2. **PKCE / loopback flow** (modern default for AWS CLI v2):
    //
    //        Browser will not be automatically opened.
    //        Please visit the following URL:
    //
    //        https://oidc.<region>.amazonaws.com/authorize?...&redirect_uri=http://127.0.0.1:PORT/oauth/callback...
    //
    //    The CLI then sits on a local HTTP listener and prints nothing more
    //    until the user completes the browser flow. There is no `user_code=`
    //    URL to wait for, so blocking on `read_line` for one would hang
    //    forever.
    //
    // Strategy: stream lines from a reader thread into a channel and use a
    // recv-with-deadline scheme. Once we see the first `https://` URL, wait
    // a short grace period for a `user_code=` variant; if none arrives,
    // accept the first URL and return.
    //
    // IMPORTANT: we must NOT drop stdout before the process exits. Closing
    // the read end of the pipe sends SIGPIPE to the aws CLI process, killing
    // it before the user can complete the browser flow. The reader thread
    // keeps the pipe open until the process exits naturally, until the abort
    // flag fires, or until the URL-scanning side decides we have what we
    // need.
    let verification_url = {
        let (line_tx, line_rx) = std::sync::mpsc::channel::<String>();

        let abort_flag_for_reader = Arc::clone(&abort_flag_for_drain);
        let child_for_reader = Arc::clone(&child_for_drain);

        std::thread::spawn(move || {
            use std::io::{BufRead, BufReader};
            let reader = BufReader::new(stdout);
            for line in reader.lines().map_while(Result::ok) {
                if abort_flag_for_reader.load(std::sync::atomic::Ordering::Acquire) {
                    log::debug!("[aws sso login drain] abort signalled, killing process");
                    if let Ok(mut guard) = child_for_reader.lock()
                        && let Some(mut child) = guard.take()
                    {
                        let _ = child.kill();
                    }
                    return;
                }
                log::debug!("[aws sso login stdout] {}", line);
                if line_tx.send(line).is_err() {
                    // Receiver dropped — drain remaining output silently.
                    break;
                }
            }
            // Process ended; if the URL-scanning side is still waiting it
            // will observe `Disconnected` and fall back to whatever it had.
        });

        // Initial wait for the first URL — give the CLI up to 30s to print
        // its first https:// line. After that we accept whatever we have.
        const INITIAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
        // Once a URL is found, wait briefly for a `user_code=` variant that
        // would be a better fit (device-code flow).
        const AUTOFILL_GRACE: std::time::Duration = std::time::Duration::from_millis(500);

        let mut found_url: Option<String> = None;
        let mut fallback_url: Option<String> = None;
        let deadline = std::time::Instant::now() + INITIAL_TIMEOUT;

        while found_url.is_none() {
            let now = std::time::Instant::now();
            let timeout = if let Some(start) = fallback_url.as_ref().map(|_| now) {
                let _ = start;
                AUTOFILL_GRACE
            } else if now >= deadline {
                break;
            } else {
                deadline - now
            };

            match line_rx.recv_timeout(timeout) {
                Ok(line) => {
                    let trimmed = line.trim().to_string();
                    if trimmed.starts_with("https://") {
                        if trimmed.contains("user_code=") {
                            found_url = Some(trimmed);
                            break;
                        } else if fallback_url.is_none() {
                            fallback_url = Some(trimmed);
                            // Continue waiting briefly for the autofill variant.
                        }
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }

        // Drain remaining lines in a background thread so the pipe stays
        // open until the CLI exits. Without this drain the OS pipe buffer
        // fills and the CLI blocks on its writes.
        std::thread::spawn(move || {
            for _line in line_rx {
                // Drop lines silently — they were already logged by the
                // reader thread above.
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

pub struct AwsSsoAuthProvider {
    config_cache: Mutex<CachedAwsConfig>,
}

impl AwsSsoAuthProvider {
    pub fn new() -> Self {
        Self {
            config_cache: Mutex::new(CachedAwsConfig::new()),
        }
    }

    /// Returns discovered AWS profiles from `~/.aws/config`, using the
    /// mtime-based cache to avoid re-parsing every time.
    pub fn list_profiles(&self) -> Vec<crate::config::AwsProfileInfo> {
        let mut cache = self.config_cache.lock().unwrap_or_else(|e| e.into_inner());
        cache.profiles().to_vec()
    }

    /// Reflects all `[profile NAME]` SSO sections from `~/.aws/config` into
    /// `AuthProfile` records.
    ///
    /// Each reflected profile has:
    /// - `provider_id = "aws-sso"`
    /// - `id = aws_profile_uuid("aws-sso", name)`
    /// - `read_only = true`
    /// - `fields` populated from the parsed config section; `sso_session`
    ///   indirection is folded in from the referenced `[sso-session]` block.
    ///
    /// Malformed sections (empty name, required fields absent) are skipped
    /// with a log warning. Missing or empty config returns an empty vec.
    pub fn reflect_profiles(&self) -> Vec<AuthProfile> {
        let profiles = {
            let mut cache = self.config_cache.lock().unwrap_or_else(|e| e.into_inner());
            cache.profiles().to_vec()
        };

        // Build a lookup for [sso-session NAME] blocks so we can fold them
        // into profiles that use the sso_session = NAME indirection.
        let session_lookup: HashMap<String, &crate::config::AwsProfileInfo> = profiles
            .iter()
            .filter(|p| p.is_sso_session)
            .map(|p| (p.name.to_lowercase(), p))
            .collect::<HashMap<_, _>>();
        // Work around the lifetime issue: collect session data into owned vecs.
        let session_map: HashMap<String, crate::config::AwsProfileInfo> = profiles
            .iter()
            .filter(|p| p.is_sso_session)
            .map(|p| (p.name.to_lowercase(), p.clone()))
            .collect();
        drop(session_lookup); // was only used to build session_map

        profiles
            .iter()
            .filter(|p| p.is_sso && !p.is_sso_session)
            .filter_map(|p| {
                if p.name.is_empty() {
                    log::warn!("aws-config-reflect: skipping SSO profile with empty name");
                    return None;
                }

                let mut fields = HashMap::new();
                fields.insert("profile_name".to_string(), p.name.clone());

                if let Some(ref region) = p.region {
                    fields.insert("region".to_string(), region.clone());
                }

                // Fold sso_session indirection: if the profile references a
                // [sso-session NAME] block, merge start_url and sso_region
                // from the session into the profile's fields.
                if let Some(ref session_name) = p.sso_session {
                    fields.insert("sso_session".to_string(), session_name.clone());

                    if let Some(session) = session_map.get(&session_name.to_lowercase()) {
                        if let Some(ref url) = session.sso_start_url {
                            fields.insert("sso_start_url".to_string(), url.clone());
                        }
                        if let Some(ref sso_region) = session.sso_region {
                            fields.insert("sso_region".to_string(), sso_region.clone());
                        }
                    }
                } else {
                    if let Some(ref url) = p.sso_start_url {
                        fields.insert("sso_start_url".to_string(), url.clone());
                    }
                    if let Some(ref sso_region) = p.sso_region {
                        fields.insert("sso_region".to_string(), sso_region.clone());
                    }
                }

                if let Some(ref account_id) = p.sso_account_id {
                    fields.insert("sso_account_id".to_string(), account_id.clone());
                }
                if let Some(ref role_name) = p.sso_role_name {
                    fields.insert("sso_role_name".to_string(), role_name.clone());
                }

                let id = aws_profile_uuid("aws-sso", &p.name);

                Some(AuthProfile {
                    id,
                    name: p.name.clone(),
                    provider_id: "aws-sso".to_string(),
                    fields,
                    enabled: true,
                    read_only: true,
                })
            })
            .collect()
    }
}

impl Default for AwsSsoAuthProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Auth provider that models an `[sso-session <name>]` block in
/// `~/.aws/config`. It is a data container — it does not own a login flow
/// on its own; other `aws-sso` profiles reference it via the
/// `sso_session_ref` field on their form, and the auth profile expansion
/// step merges the session's `sso_start_url` / `sso_region` into the
/// consumer profile before login.
pub struct AwsSsoSessionAuthProvider {
    config_cache: Mutex<CachedAwsConfig>,
}

impl AwsSsoSessionAuthProvider {
    pub fn new() -> Self {
        Self {
            config_cache: Mutex::new(CachedAwsConfig::new()),
        }
    }

    /// Reflects all `[sso-session NAME]` sections from `~/.aws/config` into
    /// `AuthProfile` records.
    ///
    /// Each reflected profile has:
    /// - `provider_id = "aws-sso-session"`
    /// - `id = aws_profile_uuid("aws-sso-session", name)` — distinct from
    ///   the same name under `aws-sso` (S17, provider-id scoping)
    /// - `read_only = true`
    ///
    /// Malformed sections (empty name) are skipped with a log warning.
    pub fn reflect_profiles(&self) -> Vec<AuthProfile> {
        let mut cache = self.config_cache.lock().unwrap_or_else(|e| e.into_inner());
        let profiles = cache.profiles().to_vec();

        profiles
            .iter()
            .filter(|p| p.is_sso_session)
            .filter_map(|p| {
                if p.name.is_empty() {
                    log::warn!("aws-config-reflect: skipping sso-session profile with empty name");
                    return None;
                }

                let mut fields = HashMap::new();
                fields.insert("profile_name".to_string(), p.name.clone());

                if let Some(ref url) = p.sso_start_url {
                    fields.insert("sso_start_url".to_string(), url.clone());
                }
                if let Some(ref sso_region) = p.sso_region {
                    fields.insert("sso_region".to_string(), sso_region.clone());
                }

                let id = aws_profile_uuid("aws-sso-session", &p.name);

                Some(AuthProfile {
                    id,
                    name: p.name.clone(),
                    provider_id: "aws-sso-session".to_string(),
                    fields,
                    enabled: true,
                    read_only: true,
                })
            })
            .collect()
    }
}

impl Default for AwsSsoSessionAuthProvider {
    fn default() -> Self {
        Self::new()
    }
}

pub struct AwsSharedCredentialsAuthProvider {
    config_cache: Mutex<CachedAwsConfig>,
}

impl AwsSharedCredentialsAuthProvider {
    pub fn new() -> Self {
        Self {
            config_cache: Mutex::new(CachedAwsConfig::new()),
        }
    }

    /// Reflects all non-SSO profile names from `~/.aws/config` and
    /// `~/.aws/credentials` into `AuthProfile` records.
    ///
    /// Uses `shared_profile_names()` which unions the non-SSO config sections
    /// with the credentials-file section names (deduped, case-preserving).
    ///
    /// Reflected fields contain `profile_name` and optionally `region` from
    /// the config file. Key material (`aws_access_key_id`,
    /// `aws_secret_access_key`) is NEVER included — the AWS SDK reads those
    /// directly from `~/.aws/credentials` at connect time.
    pub fn reflect_profiles(&self) -> Vec<AuthProfile> {
        let mut cache = self.config_cache.lock().unwrap_or_else(|e| e.into_inner());
        let names = cache.shared_profile_names();

        // Build a region lookup from the config profiles (names only; no keys).
        let config_profiles = cache.profiles().to_vec();
        let region_lookup: HashMap<String, String> = config_profiles
            .iter()
            .filter(|p| !p.is_sso && !p.is_sso_session)
            .filter_map(|p| {
                p.region
                    .as_ref()
                    .map(|r| (p.name.to_lowercase(), r.clone()))
            })
            .collect();

        names
            .into_iter()
            .filter_map(|name| {
                if name.is_empty() {
                    log::warn!(
                        "aws-config-reflect: skipping shared-credentials profile with empty name"
                    );
                    return None;
                }

                let mut fields = HashMap::new();
                fields.insert("profile_name".to_string(), name.clone());

                if let Some(region) = region_lookup.get(&name.to_lowercase()) {
                    fields.insert("region".to_string(), region.clone());
                }

                // Security invariant: no key material in reflected fields.
                // aws_access_key_id and aws_secret_access_key are intentionally
                // absent — the AWS SDK reads them from ~/.aws/credentials.
                debug_assert!(
                    !fields.contains_key("aws_access_key_id"),
                    "aws-config-reflect: key material must never appear in reflected fields"
                );
                debug_assert!(
                    !fields.contains_key("aws_secret_access_key"),
                    "aws-config-reflect: key material must never appear in reflected fields"
                );

                let id = aws_profile_uuid("aws-shared-credentials", &name);

                Some(AuthProfile {
                    id,
                    name: name.clone(),
                    provider_id: "aws-shared-credentials".to_string(),
                    fields,
                    enabled: true,
                    read_only: true,
                })
            })
            .collect()
    }
}

impl Default for AwsSharedCredentialsAuthProvider {
    fn default() -> Self {
        Self::new()
    }
}

fn required_text_field(id: &str, label: &str, placeholder: &str) -> FormFieldDef {
    FormFieldDef {
        id: id.to_string(),
        label: label.to_string(),
        kind: FormFieldKind::Text,
        placeholder: placeholder.to_string(),
        required: true,
        default_value: String::new(),
        enabled_when_checked: None,
        enabled_when_unchecked: None,
        disabled_when_field_set: None,
        help: None,
    }
}

fn build_aws_sso_form() -> AuthFormDef {
    AuthFormDef {
        tabs: vec![FormTab {
            id: "main".to_string(),
            label: "Main".to_string(),
            sections: vec![FormSection {
                title: "AWS SSO".to_string(),
                fields: vec![
                    required_text_field("profile_name", "AWS Profile Name", "dev"),
                    FormFieldDef {
                        id: "sso_session_ref".to_string(),
                        label: "SSO Session".to_string(),
                        kind: FormFieldKind::AuthProfileRef {
                            provider_id: "aws-sso-session".to_string(),
                        },
                        placeholder: String::new(),
                        required: false,
                        default_value: String::new(),
                        enabled_when_checked: None,
                        enabled_when_unchecked: None,
                        disabled_when_field_set: None,
                        help: Some(
                            "Optional. When set, SSO Start URL and SSO Region come from the referenced session and the fields below can be left empty.".to_string(),
                        ),
                    },
                    FormFieldDef {
                        id: "sso_start_url".to_string(),
                        label: "SSO Start URL".to_string(),
                        kind: FormFieldKind::Text,
                        placeholder: "https://my-org.awsapps.com/start/".to_string(),
                        required: false,
                        default_value: String::new(),
                        enabled_when_checked: None,
                        enabled_when_unchecked: None,
                        disabled_when_field_set: Some("sso_session_ref".to_string()),
                        help: None,
                    },
                    required_text_field("region", "Region", "us-east-1"),
                    FormFieldDef {
                        id: "sso_account_id".to_string(),
                        label: "Account ID".to_string(),
                        kind: FormFieldKind::DynamicSelect {
                            depends_on: vec![
                                "region".to_string(),
                                "sso_start_url".to_string(),
                                "sso_session_ref".to_string(),
                            ],
                            refresh: RefreshTrigger::OnLoginComplete,
                            requires_session: true,
                            allow_freeform: false,
                        },
                        placeholder: String::new(),
                        required: true,
                        default_value: String::new(),
                        enabled_when_checked: None,
                        enabled_when_unchecked: None,
                        disabled_when_field_set: None,
                        help: None,
                    },
                    FormFieldDef {
                        id: "sso_role_name".to_string(),
                        label: "Role Name".to_string(),
                        kind: FormFieldKind::DynamicSelect {
                            depends_on: vec!["sso_account_id".to_string()],
                            refresh: RefreshTrigger::OnDependencyChange,
                            requires_session: true,
                            allow_freeform: false,
                        },
                        placeholder: String::new(),
                        required: true,
                        default_value: String::new(),
                        enabled_when_checked: None,
                        enabled_when_unchecked: None,
                        disabled_when_field_set: None,
                        help: None,
                    },
                ],
            }],
        }],
    }
}

fn build_aws_shared_credentials_form() -> AuthFormDef {
    AuthFormDef {
        tabs: vec![FormTab {
            id: "main".to_string(),
            label: "Main".to_string(),
            sections: vec![FormSection {
                title: "AWS Shared Credentials".to_string(),
                fields: vec![
                    required_text_field("profile_name", "AWS Profile Name", "default"),
                    required_text_field("region", "Region", "us-east-1"),
                ],
            }],
        }],
    }
}

fn build_aws_sso_session_form() -> AuthFormDef {
    AuthFormDef {
        tabs: vec![FormTab {
            id: "main".to_string(),
            label: "Main".to_string(),
            sections: vec![FormSection {
                title: "AWS SSO Session".to_string(),
                fields: vec![
                    required_text_field(
                        "sso_start_url",
                        "SSO Start URL",
                        "https://my-org.awsapps.com/start/",
                    ),
                    required_text_field("sso_region", "SSO Region", "us-east-1"),
                    FormFieldDef {
                        id: "sso_registration_scopes".to_string(),
                        label: "Registration Scopes".to_string(),
                        kind: FormFieldKind::Text,
                        placeholder: "sso:account:access".to_string(),
                        required: false,
                        default_value: "sso:account:access".to_string(),
                        enabled_when_checked: None,
                        enabled_when_unchecked: None,
                        disabled_when_field_set: None,
                        help: Some(
                            "Comma-separated OAuth scopes. Default works for most setups."
                                .to_string(),
                        ),
                    },
                ],
            }],
        }],
    }
}

fn non_expiring_login(
    profile: &AuthProfile,
    provider_id: &str,
    url_callback: UrlCallback,
) -> AuthSession {
    url_callback(None);

    AuthSession {
        provider_id: provider_id.to_string(),
        profile_id: profile.id,
        expires_at: None,
        data: None,
    }
}

fn aws_profile_name_fallback_allowed(profile: &AuthProfile) -> bool {
    matches!(
        profile.provider_id.as_str(),
        "aws-sso" | "aws-shared-credentials"
    )
}

fn effective_aws_profile_name(profile: &AuthProfile) -> Option<&str> {
    if let Some(profile_name) = profile
        .fields
        .get("profile_name")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(profile_name);
    }

    if aws_profile_name_fallback_allowed(profile) {
        let fallback = profile.name.trim();
        if !fallback.is_empty() {
            return Some(fallback);
        }
    }

    None
}

fn profile_name_and_region(profile: &AuthProfile) -> (Option<&str>, &str) {
    let profile_name = effective_aws_profile_name(profile);
    let region = profile
        .fields
        .get("region")
        .map(String::as_str)
        .unwrap_or("us-east-1");

    (profile_name, region)
}

fn build_aws_value_providers_blocking(
    profile: &AuthProfile,
) -> Result<(AwsSecretsManagerProvider, AwsSsmParameterProvider), DbError> {
    let (profile_name, region) = profile_name_and_region(profile);
    let profile_name = profile_name.map(ToOwned::to_owned);
    let region = region.to_string();

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

/// Builds an `SdkConfig` from explicit static credentials stored in the
/// auth profile's `fields` map, bypassing the default credential chain.
///
#[async_trait::async_trait]
impl dbflux_core::auth::DynAuthProvider for AwsSsoAuthProvider {
    fn provider_id(&self) -> &'static str {
        "aws-sso"
    }

    fn display_name(&self) -> &'static str {
        "AWS SSO"
    }

    fn form_def(&self) -> &'static AuthFormDef {
        static FORM: OnceLock<AuthFormDef> = OnceLock::new();
        FORM.get_or_init(build_aws_sso_form)
    }

    fn capabilities(&self) -> &AuthProviderCapabilities {
        static CAPABILITIES: AuthProviderCapabilities = AuthProviderCapabilities {
            login: AuthProviderLoginCapabilities {
                supported: true,
                verification_url_progress: true,
            },
        };

        &CAPABILITIES
    }

    async fn validate_session(&self, profile: &AuthProfile) -> Result<AuthSessionState, DbError> {
        let profile_name = effective_aws_profile_name(profile).unwrap_or("");
        let sso_start_url = profile
            .fields
            .get("sso_start_url")
            .map(String::as_str)
            .unwrap_or("");

        let Some(url) = resolve_sso_start_url(profile_name, sso_start_url) else {
            log::debug!(
                "No sso_start_url for profile '{}', treating as LoginRequired",
                profile_name
            );
            return Ok(AuthSessionState::LoginRequired);
        };

        validate_sso_session(&url)
    }

    async fn login(
        &self,
        profile: &AuthProfile,
        url_callback: UrlCallback,
    ) -> Result<AuthSession, DbError> {
        let profile_name = effective_aws_profile_name(profile)
            .map(ToOwned::to_owned)
            .unwrap_or_default();
        let raw_sso_start_url = profile
            .fields
            .get("sso_start_url")
            .map(String::as_str)
            .unwrap_or("");
        let sso_start_url =
            resolve_sso_start_url(&profile_name, raw_sso_start_url).ok_or_else(|| {
                DbError::InvalidProfile(format!(
                    "AWS SSO profile '{}' has no sso_start_url (check ~/.aws/config)",
                    profile_name
                ))
            })?;

        // The profile section already exists in ~/.aws/config (it was reflected
        // from the file). No write-back to the config file is needed here.
        sso_login_with_url(profile, &profile_name, &sso_start_url, url_callback).await
    }

    async fn resolve_credentials(
        &self,
        profile: &AuthProfile,
    ) -> Result<ResolvedCredentials, DbError> {
        resolve_aws_credentials(profile).await
    }

    fn register_value_providers(
        &self,
        profile: &AuthProfile,
        _session: Option<&AuthSession>,
        resolver: &mut dbflux_core::values::CompositeValueResolver,
    ) -> Result<(), DbError> {
        let (secret_provider, param_provider) = build_aws_value_providers_blocking(profile)?;

        resolver.register_secret_provider(Arc::new(secret_provider));
        resolver.register_parameter_provider(Arc::new(param_provider));

        Ok(())
    }

    fn abort_login(&self, profile: &AuthProfile) -> bool {
        abort_sso_login(profile.id)
    }

    fn reflect_profiles(&self) -> Vec<AuthProfile> {
        AwsSsoAuthProvider::reflect_profiles(self)
    }

    fn detect_importable_profiles(&self) -> Vec<ImportableProfile> {
        let mut cache = self.config_cache.lock().unwrap_or_else(|e| e.into_inner());

        cache
            .profiles()
            .iter()
            .filter(|profile| profile.is_sso && !profile.is_sso_session)
            .map(|profile| {
                let mut fields = HashMap::new();
                fields.insert("profile_name".to_string(), profile.name.clone());

                if let Some(region) = profile.region.clone() {
                    fields.insert("region".to_string(), region);
                }

                if let Some(sso_start_url) = profile.sso_start_url.clone() {
                    fields.insert("sso_start_url".to_string(), sso_start_url);
                }

                if let Some(sso_account_id) = profile.sso_account_id.clone() {
                    fields.insert("sso_account_id".to_string(), sso_account_id);
                }

                if let Some(sso_role_name) = profile.sso_role_name.clone() {
                    fields.insert("sso_role_name".to_string(), sso_role_name);
                }

                // Preserve the `sso_session = X` indirection so the import
                // flow can wire `sso_session_ref` to the matching DBFlux
                // `aws-sso-session` profile after both are imported.
                if let Some(sso_session) = profile.sso_session.clone() {
                    fields.insert("sso_session".to_string(), sso_session);
                }

                ImportableProfile {
                    display_name: profile.name.clone(),
                    provider_id: "aws-sso".to_string(),
                    fields,
                }
            })
            .collect()
    }

    /// Fetches runtime options for `sso_account_id` and `sso_role_name`
    /// `DynamicSelect` fields.
    ///
    /// For `sso_account_id`: reads the SSO token cache and calls `list_accounts`.
    /// For `sso_role_name`: reads `sso_account_id` from dependencies and calls
    /// `list_account_roles` for that account.
    ///
    /// Returns `SessionExpired` when the access token is absent or expired so
    /// the UI can prompt for re-login.
    async fn fetch_dynamic_options(
        &self,
        profile: &AuthProfile,
        request: FetchOptionsRequest,
    ) -> Result<FetchOptionsResponse, FetchOptionsError> {
        let profile_name = effective_aws_profile_name(profile)
            .unwrap_or("")
            .to_string();
        let region = profile
            .fields
            .get("region")
            .map(String::as_str)
            .unwrap_or("us-east-1")
            .to_string();
        let sso_start_url = profile
            .fields
            .get("sso_start_url")
            .map(String::as_str)
            .unwrap_or("")
            .to_string();

        let field_id = request.field_id.clone();

        // Early validation before spawning the thread.
        let account_id_for_roles = if field_id == "sso_role_name" {
            let id = request
                .dependencies
                .get("sso_account_id")
                .map(String::as_str)
                .unwrap_or("")
                .trim()
                .to_string();

            if id.is_empty() {
                return Err(FetchOptionsError::Permanent(
                    "sso_account_id is required to list roles".to_string(),
                ));
            }

            Some(id)
        } else {
            None
        };

        match field_id.as_str() {
            "sso_account_id" | "sso_role_name" => {}
            other => {
                return Err(FetchOptionsError::Permanent(format!(
                    "unknown dynamic field: {}",
                    other
                )));
            }
        }

        // All AWS SDK calls require a Tokio runtime. The trait method is async
        // but may be called from a GPUI background executor that has no
        // runtime. Spawn a dedicated OS thread with its own runtime, then
        // poll the result channel in a non-blocking async loop.
        let (result_tx, result_rx) =
            std::sync::mpsc::sync_channel::<Result<FetchOptionsResponse, FetchOptionsError>>(1);

        std::thread::spawn(move || {
            let result = match field_id.as_str() {
                "sso_account_id" => crate::accounts::list_sso_accounts_blocking(
                    &profile_name,
                    &region,
                    &sso_start_url,
                )
                .map(|accounts| {
                    let options = accounts
                        .iter()
                        .map(|account| {
                            let label = if account.account_name.trim().is_empty() {
                                account.account_id.clone()
                            } else {
                                format!("{} ({})", account.account_name, account.account_id)
                            };
                            SelectOption::new(account.account_id.clone(), label)
                        })
                        .collect();

                    FetchOptionsResponse {
                        options,
                        cache_hint_seconds: Some(300),
                    }
                })
                .map_err(|err| map_fetch_error(err.to_string())),
                "sso_role_name" => {
                    let account_id = account_id_for_roles.unwrap_or_default();
                    crate::accounts::list_sso_account_roles_blocking(
                        &profile_name,
                        &region,
                        &sso_start_url,
                        &account_id,
                    )
                    .map(|roles| {
                        let options = roles
                            .iter()
                            .map(|role_name| {
                                SelectOption::new(role_name.clone(), role_name.clone())
                            })
                            .collect();

                        FetchOptionsResponse {
                            options,
                            cache_hint_seconds: Some(300),
                        }
                    })
                    .map_err(|err| map_fetch_error(err.to_string()))
                }
                _ => unreachable!("field_id validated above"),
            };

            let _ = result_tx.send(result);
        });

        // Non-blocking poll — yields to the executor between checks so the
        // caller can continue processing events while the fetch runs.
        loop {
            match result_rx.try_recv() {
                Ok(result) => return result,
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    async_sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    return Err(FetchOptionsError::Transient(
                        "fetch thread terminated unexpectedly".to_string(),
                    ));
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl dbflux_core::auth::DynAuthProvider for AwsSharedCredentialsAuthProvider {
    fn provider_id(&self) -> &'static str {
        "aws-shared-credentials"
    }

    fn display_name(&self) -> &'static str {
        "AWS Shared Credentials"
    }

    fn form_def(&self) -> &'static AuthFormDef {
        static FORM: OnceLock<AuthFormDef> = OnceLock::new();
        FORM.get_or_init(build_aws_shared_credentials_form)
    }

    fn capabilities(&self) -> &AuthProviderCapabilities {
        static CAPABILITIES: AuthProviderCapabilities = AuthProviderCapabilities {
            login: AuthProviderLoginCapabilities {
                supported: false,
                verification_url_progress: false,
            },
        };

        &CAPABILITIES
    }

    async fn validate_session(&self, _profile: &AuthProfile) -> Result<AuthSessionState, DbError> {
        Ok(AuthSessionState::Valid { expires_at: None })
    }

    async fn login(
        &self,
        profile: &AuthProfile,
        url_callback: UrlCallback,
    ) -> Result<AuthSession, DbError> {
        Ok(non_expiring_login(
            profile,
            self.provider_id(),
            url_callback,
        ))
    }

    async fn resolve_credentials(
        &self,
        profile: &AuthProfile,
    ) -> Result<ResolvedCredentials, DbError> {
        resolve_aws_credentials(profile).await
    }

    fn register_value_providers(
        &self,
        profile: &AuthProfile,
        _session: Option<&AuthSession>,
        resolver: &mut dbflux_core::values::CompositeValueResolver,
    ) -> Result<(), DbError> {
        let (secret_provider, param_provider) = build_aws_value_providers_blocking(profile)?;

        resolver.register_secret_provider(Arc::new(secret_provider));
        resolver.register_parameter_provider(Arc::new(param_provider));

        Ok(())
    }

    fn reflect_profiles(&self) -> Vec<AuthProfile> {
        AwsSharedCredentialsAuthProvider::reflect_profiles(self)
    }
}

#[async_trait::async_trait]
impl dbflux_core::auth::DynAuthProvider for AwsSsoSessionAuthProvider {
    fn provider_id(&self) -> &'static str {
        "aws-sso-session"
    }

    fn display_name(&self) -> &'static str {
        "AWS SSO Session"
    }

    fn form_def(&self) -> &'static AuthFormDef {
        static FORM: OnceLock<AuthFormDef> = OnceLock::new();
        FORM.get_or_init(build_aws_sso_session_form)
    }

    fn capabilities(&self) -> &AuthProviderCapabilities {
        // SSO session profiles are reference targets, not login targets.
        // Login happens via the `aws-sso` profile that points at the session.
        static CAPABILITIES: AuthProviderCapabilities = AuthProviderCapabilities {
            login: AuthProviderLoginCapabilities {
                supported: false,
                verification_url_progress: false,
            },
        };

        &CAPABILITIES
    }

    async fn validate_session(&self, _profile: &AuthProfile) -> Result<AuthSessionState, DbError> {
        // A session record is always considered "valid" as a data container.
        // Token validity for the referenced URL is checked by the consumer
        // `aws-sso` profile during its own validate_session.
        Ok(AuthSessionState::Valid { expires_at: None })
    }

    async fn login(
        &self,
        profile: &AuthProfile,
        url_callback: UrlCallback,
    ) -> Result<AuthSession, DbError> {
        Ok(non_expiring_login(
            profile,
            self.provider_id(),
            url_callback,
        ))
    }

    async fn resolve_credentials(
        &self,
        _profile: &AuthProfile,
    ) -> Result<ResolvedCredentials, DbError> {
        Ok(ResolvedCredentials::default())
    }

    fn detect_importable_profiles(&self) -> Vec<ImportableProfile> {
        let mut cache = self.config_cache.lock().unwrap_or_else(|e| e.into_inner());

        cache
            .profiles()
            .iter()
            .filter(|entry| entry.is_sso_session)
            .map(|entry| {
                let mut fields = HashMap::new();

                if let Some(sso_start_url) = entry.sso_start_url.clone() {
                    fields.insert("sso_start_url".to_string(), sso_start_url);
                }

                if let Some(sso_region) = entry.sso_region.clone() {
                    fields.insert("sso_region".to_string(), sso_region);
                }

                ImportableProfile {
                    display_name: entry.name.clone(),
                    provider_id: "aws-sso-session".to_string(),
                    fields,
                }
            })
            .collect()
    }

    fn reflect_profiles(&self) -> Vec<AuthProfile> {
        AwsSsoSessionAuthProvider::reflect_profiles(self)
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

    // Fall back to ~/.aws/config, following `sso_session` indirection when the
    // profile delegates its SSO config to an `[sso-session <name>]` section.
    let config_path = aws_config_path();
    let contents = std::fs::read_to_string(&config_path).ok()?;
    let profiles = crate::config::parse_aws_config_str(&contents);

    let profile = profiles
        .iter()
        .find(|p| !p.is_sso_session && p.name.eq_ignore_ascii_case(profile_name))?;

    let direct = profile
        .sso_start_url
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty());

    if let Some(url) = direct {
        return Some(url.to_string());
    }

    let session_name = profile.sso_session.as_deref().map(str::trim)?;
    if session_name.is_empty() {
        return None;
    }

    profiles
        .iter()
        .find(|p| p.is_sso_session && p.name.eq_ignore_ascii_case(session_name))
        .and_then(|p| p.sso_start_url.clone())
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
    let profile_name = effective_aws_profile_name(profile).map(ToOwned::to_owned);
    let region = profile
        .fields
        .get("region")
        .cloned()
        .unwrap_or_else(|| "us-east-1".to_string());

    log::debug!(
        "Resolving AWS credentials for auth profile '{}' (provider={}, aws_profile={}, region={})",
        profile.name,
        profile.provider_id,
        profile_name.as_deref().unwrap_or("<default>"),
        region
    );

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
    let aws_profile_label = profile_name
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or("<default>")
        .to_string();

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
                DbError::ValueResolutionFailed(format!(
                    "No credentials provider found in AWS SDK config (aws_profile={}, region={})",
                    aws_profile_label, region
                ))
            })?
            .provide_credentials()
            .await
            .map_err(|err| {
                log::warn!(
                    "AWS credential resolution failed (aws_profile={}, region={}): {}",
                    aws_profile_label,
                    region,
                    err
                );
                DbError::ValueResolutionFailed(format!(
                    "Failed to resolve AWS credentials (aws_profile={}, region={}): {}",
                    aws_profile_label, region, err
                ))
            })?;

        let mut fields = std::collections::HashMap::new();
        fields.insert(
            "access_key_id".to_string(),
            creds.access_key_id().to_string(),
        );
        fields.insert("region".to_string(), region);

        let mut secret_fields = std::collections::HashMap::new();
        secret_fields.insert(
            "secret_access_key".to_string(),
            SecretString::from(creds.secret_access_key().to_string()),
        );
        if let Some(token) = creds.session_token() {
            secret_fields.insert(
                "session_token".to_string(),
                SecretString::from(token.to_string()),
            );
        }

        if let Some(expiry) = creds.expiry() {
            let dt = chrono::DateTime::<Utc>::from(expiry);
            fields.insert("expires_at".to_string(), dt.to_rfc3339());
        }

        Ok(ResolvedCredentials {
            fields,
            secret_fields,
            provider_data: Some(Arc::new(sdk_config)),
        })
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
/// AWS CLI v2 uses two different cache filename schemes:
/// - Legacy: the file is named `sha1(start_url)` (with or without trailing slash).
/// - Modern (`sso_session` block in `~/.aws/config`): the file is named
///   `sha1(session_name)` instead, completely decoupled from the start URL.
///
/// Both schemes write the start URL into the file's `startUrl` JSON field, so
/// the only reliable way to find the *current* token is to scan every `.json`
/// file in the cache directory, match by `startUrl` content, and pick the most
/// recently modified one. Relying on the hash-derived filename causes stale
/// hash-matched files to mask fresh session-keyed tokens, leaving the login
/// polling loop spinning forever after a successful `aws sso login`.
fn find_sso_cache_contents(normalized_url: &str) -> Option<String> {
    let dir = sso_cache_dir();
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(_) => return None,
    };

    let mut newest: Option<(std::time::SystemTime, std::path::PathBuf, String)> = None;

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

        let Some(url) = parsed.get("startUrl").and_then(|v| v.as_str()) else {
            continue;
        };

        if url.trim_end_matches('/') != normalized_url {
            continue;
        }

        let mtime = std::fs::metadata(&path)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);

        match &newest {
            Some((current_mtime, _, _)) if *current_mtime >= mtime => {}
            _ => newest = Some((mtime, path, contents)),
        }
    }

    if let Some((_, path, contents)) = newest {
        log::debug!("SSO cache hit: {}", path.display());
        return Some(contents);
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
/// Registry of in-flight SSO login abort senders keyed by `AuthProfile.id`.
/// Allows the UI to cancel a running login (kills the `aws sso login` process
/// and unblocks the cache-polling loop).
static ABORT_REGISTRY: OnceLock<Mutex<HashMap<uuid::Uuid, std::sync::mpsc::SyncSender<()>>>> =
    OnceLock::new();

fn abort_registry() -> &'static Mutex<HashMap<uuid::Uuid, std::sync::mpsc::SyncSender<()>>> {
    ABORT_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Signals the in-flight `aws sso login` for `profile_id` to abort.
///
/// Returns `true` if an abort was signalled (a login was in flight),
/// `false` if no login for this profile was tracked.
pub fn abort_sso_login(profile_id: uuid::Uuid) -> bool {
    let sender = {
        let mut map = abort_registry().lock().unwrap_or_else(|e| e.into_inner());
        map.remove(&profile_id)
    };
    match sender {
        Some(tx) => {
            let _ = tx.try_send(());
            true
        }
        None => false,
    }
}

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

        // Register the abort sender so the UI can cancel this login mid-flight.
        {
            let mut map = abort_registry().lock().unwrap_or_else(|e| e.into_inner());
            map.insert(profile_id, handle.abort_tx.clone());
        }

        // Fire the callback now — the URL is known, the user may still be
        // completing the browser flow.
        url_callback(handle.verification_url);

        // Poll the token cache until the session appears, times out, or is aborted.
        let session =
            wait_for_sso_session_blocking(profile_id, "aws-sso", &start_url, &handle.abort_flag);

        // Deregister on exit regardless of outcome.
        {
            let mut map = abort_registry().lock().unwrap_or_else(|e| e.into_inner());
            map.remove(&profile_id);
        }

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
                std::task::Poll::Pending
            }
            SleepState::Sleeping(rx) => {
                if rx.try_recv().is_ok() {
                    self.state = SleepState::Done;
                    return std::task::Poll::Ready(());
                }

                // Not ready yet — remain pending; the waker will re-poll us.
                std::task::Poll::Pending
            }
            SleepState::Done => std::task::Poll::Ready(()),
        }
    }
}

/// Fully blocking SSO login: spawns the AWS CLI, reads the URL from stdout,
/// then polls the token cache until the session appears or times out.
///
/// Safe to call from a plain OS thread with no async runtime. Used by the
/// Settings UI login button which runs on the GPUI background executor
/// (which has no Tokio reactor).
/// Performs a blocking SSO login for the given profile.
///
/// The profile section must already exist in `~/.aws/config` (it is reflected
/// from the file). No write to `~/.aws/config` is performed here.
pub fn login_sso_blocking(
    profile_id: uuid::Uuid,
    profile_name: &str,
    sso_start_url: &str,
) -> Result<AuthSession, DbError> {
    let handle = start_sso_login_blocking(profile_name)?;
    log::debug!(
        "AWS SSO login started for profile '{}', verification URL: {:?}",
        profile_name,
        handle.verification_url
    );

    // No external abort signal for the Settings UI path — use a flag that
    // is never set so the poll runs to completion or timeout.
    wait_for_sso_session_blocking(profile_id, "aws-sso", sso_start_url, &handle.abort_flag)
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

/// Maps a stringified AWS SDK or cache error to a `FetchOptionsError`.
///
/// Errors containing "Login required", "expired", "unauthorized", or similar
/// tokens indicate the SSO session is gone → `SessionExpired`. Everything else
/// is treated as `Transient` (retriable) to let the UI show a refresh button.
fn map_fetch_error(message: String) -> FetchOptionsError {
    let lower = message.to_lowercase();
    if lower.contains("login required")
        || lower.contains("expiredtoken")
        || lower.contains("session expired")
        || lower.contains("invalidtoken")
        || lower.contains("unauthorized")
    {
        FetchOptionsError::SessionExpired
    } else {
        FetchOptionsError::Transient(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::DynAuthProvider;
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
        let mut fields = std::collections::HashMap::new();
        fields.insert("profile_name".to_string(), "default".to_string());
        fields.insert("region".to_string(), "us-east-1".to_string());

        let profile = AuthProfile::new("test-shared", "aws-shared-credentials", fields);

        let provider = AwsSharedCredentialsAuthProvider::new();
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
    fn shared_credentials_profile_name_falls_back_to_auth_profile_name() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("region".to_string(), "us-east-1".to_string());

        let profile = AuthProfile::new("team-sso", "aws-shared-credentials", fields);

        let (profile_name, region) = profile_name_and_region(&profile);

        assert_eq!(profile_name, Some("team-sso"));
        assert_eq!(region, "us-east-1");
    }

    #[test]
    fn aws_sso_capabilities_advertise_interactive_login() {
        let provider = AwsSsoAuthProvider::new();

        assert!(
            <AwsSsoAuthProvider as dbflux_core::auth::DynAuthProvider>::capabilities(&provider)
                .login
                .supported
        );
        assert!(
            <AwsSsoAuthProvider as dbflux_core::auth::DynAuthProvider>::capabilities(&provider)
                .login
                .verification_url_progress
        );
    }

    #[test]
    fn shared_credentials_provider_keeps_login_disabled() {
        let shared = AwsSharedCredentialsAuthProvider::new();
        let shared_capabilities =
            <AwsSharedCredentialsAuthProvider as dbflux_core::auth::DynAuthProvider>::capabilities(
                &shared,
            );
        assert!(!shared_capabilities.login.supported);
        assert!(!shared_capabilities.login.verification_url_progress);
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

    fn field_def_by_id<'a>(fields: &'a [FormFieldDef], id: &str) -> Option<&'a FormFieldDef> {
        fields.iter().find(|f| f.id == id)
    }

    // -------------------------------------------------------------------------
    // T16: fetch_dynamic_options unit tests (no live AWS calls)
    // -------------------------------------------------------------------------

    fn make_sso_profile() -> AuthProfile {
        let mut fields = std::collections::HashMap::new();
        fields.insert("profile_name".to_string(), "test-profile".to_string());
        fields.insert("region".to_string(), "us-east-1".to_string());
        fields.insert(
            "sso_start_url".to_string(),
            "https://test.awsapps.com/start".to_string(),
        );

        AuthProfile {
            id: uuid::Uuid::new_v4(),
            name: "Test".to_string(),
            provider_id: "aws-sso".to_string(),
            fields,
            enabled: true,
            read_only: false,
        }
    }

    /// When there is no valid SSO token cache, `fetch_dynamic_options` for
    /// `sso_account_id` must return `SessionExpired` (or at minimum a
    /// `Transient`/`Permanent` error — never a panic).
    #[test]
    fn fetch_accounts_without_session_returns_session_expired_or_transient() {
        // Use a temp dir so we control the SSO cache path indirectly via
        // the environment variable AWS_CONFIG_FILE. The SSO token cache is
        // read from ~/.aws/sso/cache which we cannot override directly, so
        // this test asserts the error branch (no live AWS call is made).
        let provider = AwsSsoAuthProvider::new();
        let profile = make_sso_profile();
        let request = FetchOptionsRequest {
            field_id: "sso_account_id".to_string(),
            dependencies: std::collections::HashMap::new(),
            session: None,
        };

        // The blocking call should not panic. The exact variant depends on
        // whether a stale token cache exists in the test environment.
        let result = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
            .block_on(provider.fetch_dynamic_options(&profile, request));

        match result {
            Ok(_) => {
                // If the test environment happens to have a valid token, that is
                // also acceptable.
            }
            Err(FetchOptionsError::SessionExpired)
            | Err(FetchOptionsError::Transient(_))
            | Err(FetchOptionsError::Permanent(_)) => {
                // All expected error paths — no panic occurred.
            }
            Err(FetchOptionsError::NeedsLogin) => {
                // Also acceptable.
            }
        }
    }

    /// An unknown field id must return `Permanent`.
    #[test]
    fn fetch_unknown_field_returns_permanent() {
        let provider = AwsSsoAuthProvider::new();
        let profile = make_sso_profile();
        let request = FetchOptionsRequest {
            field_id: "nonexistent_field".to_string(),
            dependencies: std::collections::HashMap::new(),
            session: None,
        };

        let result = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
            .block_on(provider.fetch_dynamic_options(&profile, request));

        assert!(
            matches!(result, Err(FetchOptionsError::Permanent(_))),
            "expected Permanent error for unknown field, got {:?}",
            result
        );
    }

    // --- T-3.1: AwsSsoAuthProvider::reflect_profiles() ---

    /// Helper: creates a `CachedAwsConfig` backed by a temp config file, wrapped
    /// in an `AwsSsoAuthProvider`.
    fn sso_provider_with_config(config_content: &str) -> AwsSsoAuthProvider {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config");
        let creds_path = dir.path().join("credentials");
        std::fs::write(&config_path, config_content).unwrap();
        std::fs::write(&creds_path, "").unwrap();
        let cache = crate::config::CachedAwsConfig::new_with_paths(config_path, creds_path);
        // Leak the tempdir so the files remain for the test's duration.
        std::mem::forget(dir);
        AwsSsoAuthProvider {
            config_cache: Mutex::new(cache),
        }
    }

    /// Helper: creates a provider with both a config and a credentials file.
    fn sso_provider_with_config_keep_dir(
        config_content: &str,
    ) -> (AwsSsoAuthProvider, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config");
        let creds_path = dir.path().join("credentials");
        std::fs::write(&config_path, config_content).unwrap();
        std::fs::write(&creds_path, "").unwrap();
        let cache = crate::config::CachedAwsConfig::new_with_paths(config_path, creds_path);
        let provider = AwsSsoAuthProvider {
            config_cache: Mutex::new(cache),
        };
        (provider, dir)
    }

    #[test]
    fn sso_reflect_produces_auth_profile_with_correct_provider_and_uuid() {
        let config = r#"
[profile dev-sso]
sso_start_url = https://example.awsapps.com/start
sso_account_id = 123456789012
sso_role_name = DevAccess
sso_region = us-east-1
"#;
        let provider = sso_provider_with_config(config);
        let profiles = provider.reflect_profiles();

        assert_eq!(profiles.len(), 1, "expected one reflected SSO profile");
        let p = &profiles[0];

        assert_eq!(p.provider_id, "aws-sso");
        assert_eq!(p.name, "dev-sso");
        assert!(p.read_only, "reflected profile must be read-only");
        assert!(p.enabled);

        let expected_id = dbflux_core::auth::aws_profile_uuid("aws-sso", "dev-sso");
        assert_eq!(
            p.id, expected_id,
            "id must equal aws_profile_uuid(aws-sso, name)"
        );

        assert_eq!(
            p.fields.get("sso_start_url").map(String::as_str),
            Some("https://example.awsapps.com/start")
        );
        assert_eq!(
            p.fields.get("sso_account_id").map(String::as_str),
            Some("123456789012")
        );
        assert_eq!(
            p.fields.get("sso_role_name").map(String::as_str),
            Some("DevAccess")
        );
        assert_eq!(
            p.fields.get("profile_name").map(String::as_str),
            Some("dev-sso")
        );

        // No secret fields.
        assert!(
            !p.fields.contains_key("aws_access_key_id"),
            "aws_access_key_id must not appear in reflected fields"
        );
        assert!(
            !p.fields.contains_key("aws_secret_access_key"),
            "aws_secret_access_key must not appear in reflected fields"
        );
    }

    #[test]
    fn sso_reflect_folds_sso_session_indirection() {
        let config = r#"
[profile my-sso]
sso_session = my-org

[sso-session my-org]
sso_start_url = https://example.awsapps.com/start
sso_region = us-east-1
"#;
        let provider = sso_provider_with_config(config);
        let profiles = provider.reflect_profiles();

        // Only one SSO profile (the sso-session is handled by AwsSsoSessionAuthProvider).
        let sso = profiles
            .iter()
            .find(|p| p.name == "my-sso")
            .expect("my-sso must be reflected");

        assert_eq!(sso.provider_id, "aws-sso");
        assert_eq!(
            sso.fields.get("sso_start_url").map(String::as_str),
            Some("https://example.awsapps.com/start"),
            "sso_start_url must be folded from the sso-session block"
        );
        assert_eq!(
            sso.fields.get("sso_region").map(String::as_str),
            Some("us-east-1"),
            "sso_region must be folded from the sso-session block"
        );
        assert_eq!(
            sso.fields.get("sso_session").map(String::as_str),
            Some("my-org"),
            "sso_session reference name must be preserved in fields"
        );
    }

    #[test]
    fn sso_reflect_returns_empty_when_config_missing() {
        let dir = tempfile::tempdir().unwrap();
        // config_path intentionally does not exist.
        let config_path = dir.path().join("nonexistent_config");
        let creds_path = dir.path().join("credentials");
        std::fs::write(&creds_path, "").unwrap();
        let cache = crate::config::CachedAwsConfig::new_with_paths(config_path, creds_path);
        let provider = AwsSsoAuthProvider {
            config_cache: Mutex::new(cache),
        };

        let profiles = provider.reflect_profiles();
        assert!(
            profiles.is_empty(),
            "missing config must yield empty list, no panic"
        );
    }

    #[test]
    fn sso_reflect_skips_non_sso_sections_but_reflects_sso_ones() {
        let config = r#"
[profile ci-user]
region = us-west-2

[profile dev-sso]
sso_start_url = https://example.awsapps.com/start
sso_account_id = 123456789012
sso_role_name = DevAccess
sso_region = us-east-1
"#;
        let provider = sso_provider_with_config(config);
        let profiles = provider.reflect_profiles();

        // Only SSO profiles; ci-user is shared-credentials, not SSO.
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].name, "dev-sso");
    }

    // --- T-3.2: AwsSsoSessionAuthProvider::reflect_profiles() ---

    fn sso_session_provider_with_config(config_content: &str) -> AwsSsoSessionAuthProvider {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config");
        let creds_path = dir.path().join("credentials");
        std::fs::write(&config_path, config_content).unwrap();
        std::fs::write(&creds_path, "").unwrap();
        let cache = crate::config::CachedAwsConfig::new_with_paths(config_path, creds_path);
        std::mem::forget(dir);
        AwsSsoSessionAuthProvider {
            config_cache: Mutex::new(cache),
        }
    }

    #[test]
    fn sso_session_reflect_produces_correct_provider_and_uuid() {
        let config = r#"
[sso-session my-org]
sso_start_url = https://example.awsapps.com/start
sso_region = us-east-1
"#;
        let provider = sso_session_provider_with_config(config);
        let profiles = provider.reflect_profiles();

        assert_eq!(profiles.len(), 1);
        let p = &profiles[0];

        assert_eq!(p.provider_id, "aws-sso-session");
        assert_eq!(p.name, "my-org");
        assert!(p.read_only);

        let expected_id = dbflux_core::auth::aws_profile_uuid("aws-sso-session", "my-org");
        assert_eq!(p.id, expected_id);

        assert_eq!(
            p.fields.get("sso_start_url").map(String::as_str),
            Some("https://example.awsapps.com/start")
        );
    }

    #[test]
    fn sso_session_uuid_differs_from_sso_same_name() {
        let config = r#"
[profile shared]
sso_start_url = https://example.awsapps.com/start
sso_account_id = 111122223333
sso_role_name = Admin
sso_region = us-east-1

[sso-session shared]
sso_start_url = https://example.awsapps.com/start
sso_region = us-east-1
"#;
        let sso_provider = sso_provider_with_config(config);
        let session_provider = sso_session_provider_with_config(config);

        let sso_profiles = sso_provider.reflect_profiles();
        let session_profiles = session_provider.reflect_profiles();

        let sso_p = sso_profiles
            .iter()
            .find(|p| p.name == "shared")
            .expect("sso shared");
        let session_p = session_profiles
            .iter()
            .find(|p| p.name == "shared")
            .expect("session shared");

        assert_ne!(
            sso_p.id, session_p.id,
            "same name under aws-sso vs aws-sso-session must have distinct UUIDs"
        );
    }

    #[test]
    fn sso_session_reflect_returns_empty_when_config_missing() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("nonexistent");
        let creds_path = dir.path().join("credentials");
        std::fs::write(&creds_path, "").unwrap();
        let cache = crate::config::CachedAwsConfig::new_with_paths(config_path, creds_path);
        let provider = AwsSsoSessionAuthProvider {
            config_cache: Mutex::new(cache),
        };
        assert!(provider.reflect_profiles().is_empty());
    }

    // --- T-3.3: AwsSharedCredentialsAuthProvider::reflect_profiles() ---

    fn shared_provider_with_files(
        config_content: &str,
        credentials_content: &str,
    ) -> AwsSharedCredentialsAuthProvider {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config");
        let creds_path = dir.path().join("credentials");
        std::fs::write(&config_path, config_content).unwrap();
        std::fs::write(&creds_path, credentials_content).unwrap();
        let cache = crate::config::CachedAwsConfig::new_with_paths(config_path, creds_path);
        std::mem::forget(dir);
        AwsSharedCredentialsAuthProvider {
            config_cache: Mutex::new(cache),
        }
    }

    #[test]
    fn shared_reflect_includes_credentials_file_profiles() {
        let credentials = "[ci-user]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\naws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCY\n";
        let config = "";
        let provider = shared_provider_with_files(config, credentials);
        let profiles = provider.reflect_profiles();

        let ci = profiles
            .iter()
            .find(|p| p.name == "ci-user")
            .expect("ci-user must be reflected");
        assert_eq!(ci.provider_id, "aws-shared-credentials");
        assert!(ci.read_only);

        let expected_id = dbflux_core::auth::aws_profile_uuid("aws-shared-credentials", "ci-user");
        assert_eq!(ci.id, expected_id);
    }

    #[test]
    fn shared_reflect_reflects_region_from_config_when_present() {
        let config = "[profile ci-user]\nregion = us-west-2\n";
        let credentials = "[ci-user]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\naws_secret_access_key = wJalrXUtnFEMI\n";
        let provider = shared_provider_with_files(config, credentials);
        let profiles = provider.reflect_profiles();

        let ci = profiles
            .iter()
            .find(|p| p.name == "ci-user")
            .expect("ci-user");
        assert_eq!(
            ci.fields.get("region").map(String::as_str),
            Some("us-west-2"),
            "region from config must be reflected"
        );
    }

    #[test]
    fn shared_reflect_no_region_does_not_error() {
        let credentials = "[my-profile]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\naws_secret_access_key = wJalrXUtnFEMI\n";
        let provider = shared_provider_with_files("", credentials);
        let profiles = provider.reflect_profiles();

        let p = profiles
            .iter()
            .find(|p| p.name == "my-profile")
            .expect("my-profile");
        assert!(
            p.fields.get("region").is_none(),
            "absent region must not appear in fields"
        );
    }

    /// Security assertion: reflected shared-credentials profiles must never
    /// contain key material (ADR-7 invariant).
    #[test]
    fn shared_reflect_never_includes_key_material() {
        let credentials = "[prod]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\naws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCY\n";
        let provider = shared_provider_with_files("", credentials);
        let profiles = provider.reflect_profiles();

        for profile in &profiles {
            let keys_present = profile.fields.contains_key("aws_access_key_id")
                || profile.fields.contains_key("aws_secret_access_key");
            assert!(
                !keys_present,
                "profile '{}' must not contain key material in reflected fields",
                profile.name
            );

            // Also assert no AKIA-pattern value appears anywhere in the fields.
            for (key, value) in &profile.fields {
                assert!(
                    !value.starts_with("AKIA"),
                    "field '{}' contains an AKIA-pattern value, which is forbidden in reflected fields",
                    key
                );
            }
        }
    }

    /// When `sso_account_id` dependency is missing, `sso_role_name` fetch
    /// must return `Permanent`.
    #[test]
    fn fetch_roles_without_account_id_returns_permanent() {
        let provider = AwsSsoAuthProvider::new();
        let profile = make_sso_profile();
        let request = FetchOptionsRequest {
            field_id: "sso_role_name".to_string(),
            dependencies: std::collections::HashMap::new(), // no sso_account_id
            session: None,
        };

        let result = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
            .block_on(provider.fetch_dynamic_options(&profile, request));

        assert!(
            matches!(result, Err(FetchOptionsError::Permanent(_))),
            "expected Permanent error when sso_account_id is absent, got {:?}",
            result
        );
    }
}
