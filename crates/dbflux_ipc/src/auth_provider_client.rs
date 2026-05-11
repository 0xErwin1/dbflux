use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::process::{Child, Command};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use dbflux_core::DbError;
use dbflux_core::FormFieldKind;
use dbflux_core::auth::{
    AuthFormDef, AuthProfile, AuthProviderCapabilities, AuthSession, AuthSessionState,
    DynAuthProvider, ResolvedCredentials, UrlCallback,
};
use interprocess::local_socket::{Stream as IpcStream, prelude::*};

use crate::auth::AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV;
use crate::auth_provider_protocol::{
    AuthProviderHelloRequest, AuthProviderRequestBody, AuthProviderRequestEnvelope,
    AuthProviderResponseBody, AuthProviderResponseEnvelope, FetchFieldOptionsError,
    FetchFieldOptionsRequest, LoginRequest, ResolveCredentialsRequest, ValidateSessionRequest,
};
use crate::envelope::{AUTH_PROVIDER_RPC_API_CONTRACT, AUTH_PROVIDER_RPC_V1_2, ProtocolVersion};
use crate::framing;
use crate::socket::auth_provider_socket_name;
use crate::{RpcApiFamily, auth_provider_rpc_supported_versions, negotiate_highest_mutual_version};

const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 5_000;
const MIN_STARTUP_TIMEOUT_MS: u64 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IpcServiceLaunchConfig {
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub startup_timeout: Duration,
}

pub struct RpcAuthProvider {
    socket_id: String,
    provider_id: String,
    display_name: String,
    form_definition: AuthFormDef,
    capabilities: AuthProviderCapabilities,
    selected_version: ProtocolVersion,
    /// Whether the provider has opted in to receiving `Password`-kind field
    /// values in `FetchFieldOptions` requests (v1.2+ only).
    secret_dependency_opt_in: bool,
    launch: Option<IpcServiceLaunchConfig>,
}

#[derive(Debug)]
struct NormalizedAuthProviderHelloResponse {
    provider_id: String,
    display_name: String,
    form_definition: AuthFormDef,
    capabilities: AuthProviderCapabilities,
    selected_version: ProtocolVersion,
    secret_dependency_opt_in: bool,
}

impl RpcAuthProvider {
    #[allow(clippy::result_large_err)]
    pub fn build_launch_config(
        socket_id: &str,
        command: Option<&str>,
        args: &[String],
        env: &HashMap<String, String>,
        startup_timeout_ms: Option<u64>,
    ) -> Result<Option<IpcServiceLaunchConfig>, DbError> {
        validate_socket_id(socket_id)?;

        let program = match command.map(str::trim).filter(|value| !value.is_empty()) {
            Some(program) => Some(program.to_string()),
            None if args.is_empty() => None,
            None => {
                return Err(DbError::connection_failed(format!(
                    "Managed auth-provider service '{}' must set an explicit command; default driver-host launch is driver-only",
                    socket_id
                )));
            }
        };

        let Some(program) = program else {
            return Ok(None);
        };

        let startup_timeout_ms = match startup_timeout_ms {
            Some(0) => {
                return Err(DbError::connection_failed(format!(
                    "Startup timeout for service '{}' must be at least {} ms",
                    socket_id, MIN_STARTUP_TIMEOUT_MS
                )));
            }
            Some(timeout) => timeout,
            None => DEFAULT_STARTUP_TIMEOUT_MS,
        };

        let mut env_pairs = env
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        env_pairs.sort_by(|left, right| left.0.cmp(&right.0));

        Ok(Some(IpcServiceLaunchConfig {
            program,
            args: args.to_vec(),
            env: env_pairs,
            startup_timeout: Duration::from_millis(startup_timeout_ms),
        }))
    }

    #[allow(clippy::result_large_err)]
    pub fn probe(socket_id: &str, launch: Option<IpcServiceLaunchConfig>) -> Result<Self, DbError> {
        validate_socket_id(socket_id)?;

        let hello = Self::perform_hello(socket_id, launch.as_ref())?;

        Ok(Self {
            socket_id: socket_id.to_string(),
            provider_id: hello.provider_id,
            display_name: hello.display_name,
            form_definition: hello.form_definition,
            capabilities: hello.capabilities,
            selected_version: hello.selected_version,
            secret_dependency_opt_in: hello.secret_dependency_opt_in,
            launch,
        })
    }

    #[allow(clippy::result_large_err)]
    fn connect_stream(&self) -> Result<IpcStream, DbError> {
        ensure_host_running_for(&self.socket_id, self.launch.as_ref())?;

        let name = auth_provider_socket_name(&self.socket_id)
            .map_err(|error| DbError::connection_failed(error.to_string()))?;

        IpcStream::connect(name).map_err(|error| DbError::connection_failed(error.to_string()))
    }

    #[allow(clippy::result_large_err)]
    fn perform_hello(
        socket_id: &str,
        launch: Option<&IpcServiceLaunchConfig>,
    ) -> Result<NormalizedAuthProviderHelloResponse, DbError> {
        ensure_host_running_for(socket_id, launch)?;

        let name = auth_provider_socket_name(socket_id)
            .map_err(|error| DbError::connection_failed(error.to_string()))?;

        let mut stream = IpcStream::connect(name)
            .map_err(|error| DbError::connection_failed(error.to_string()))?;

        let auth_token = std::env::var(AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV)
            .ok()
            .filter(|token| !token.is_empty());

        let request = AuthProviderRequestEnvelope::new(
            AUTH_PROVIDER_RPC_API_CONTRACT.version,
            0,
            AuthProviderRequestBody::Hello(AuthProviderHelloRequest {
                client_name: "dbflux_ipc".to_string(),
                client_version: env!("CARGO_PKG_VERSION").to_string(),
                supported_versions: auth_provider_rpc_supported_versions().to_vec(),
                auth_token,
            }),
        );

        framing::send_msg(&mut stream, &request)?;
        let response: AuthProviderResponseEnvelope = framing::recv_msg(&mut stream)?;

        if response.request_id != request.request_id {
            return Err(DbError::connection_failed(format!(
                "Request ID mismatch: sent {}, got {}",
                request.request_id, response.request_id
            )));
        }

        match response.body {
            AuthProviderResponseBody::Hello(_)
            | AuthProviderResponseBody::HelloV1_1(_)
            | AuthProviderResponseBody::HelloV1_2(_) => {
                let hello = normalize_hello_response(response.body)?;

                validate_hello_selected_version(
                    hello.selected_version,
                    auth_provider_rpc_supported_versions(),
                )?;

                Ok(hello)
            }
            AuthProviderResponseBody::Error(error) => Err(error.into_db_error()),
            _ => Err(DbError::connection_failed(
                "Unexpected response to auth-provider Hello".to_string(),
            )),
        }
    }

    #[allow(clippy::result_large_err)]
    fn send_request(
        &self,
        body: AuthProviderRequestBody,
    ) -> Result<Vec<AuthProviderResponseEnvelope>, DbError> {
        let mut stream = self.connect_stream()?;
        let request = AuthProviderRequestEnvelope::new(self.selected_version, 1, body);

        framing::send_msg(&mut stream, &request)?;

        let mut responses = Vec::new();
        loop {
            let response: AuthProviderResponseEnvelope = framing::recv_msg(&mut stream)?;

            if response.request_id != request.request_id {
                return Err(DbError::connection_failed(format!(
                    "Request ID mismatch: sent {}, got {}",
                    request.request_id, response.request_id
                )));
            }

            let done = response.done;
            responses.push(response);

            if done {
                return Ok(responses);
            }
        }
    }

    /// Fetch the available options for a `DynamicSelect` field.
    ///
    /// Returns `FetchFieldOptionsError::SessionExpired` if the provider
    /// response cannot be deserialized or is missing. Password-kind field
    /// values are stripped from `dependencies` unless `secret_dependency_opt_in`
    /// is true on this provider AND the target field lists that field in its
    /// `depends_on`.
    pub async fn fetch_dynamic_options(
        &self,
        profile: &AuthProfile,
        field_id: &str,
        raw_dependencies: HashMap<String, String>,
        session: Option<serde_json::Value>,
    ) -> Result<crate::auth_provider_protocol::FetchFieldOptionsResponse, FetchFieldOptionsError>
    {
        // Only v1.2+ providers support FetchDynamicOptions.
        let supports_fetch = self.selected_version.major > AUTH_PROVIDER_RPC_V1_2.major
            || (self.selected_version.major == AUTH_PROVIDER_RPC_V1_2.major
                && self.selected_version.minor >= AUTH_PROVIDER_RPC_V1_2.minor);
        if !supports_fetch {
            return Err(FetchFieldOptionsError::Permanent(format!(
                "auth-provider '{}' does not support FetchDynamicOptions (selected protocol {}.{})",
                self.provider_id, self.selected_version.major, self.selected_version.minor
            )));
        }

        // Build the filtered dependencies map, stripping Password fields when
        // the provider has not opted in to receiving them.
        let dependencies = build_fetch_dependencies(
            &self.form_definition,
            field_id,
            &raw_dependencies,
            self.secret_dependency_opt_in,
        );

        let profile_json = serde_json::to_string(profile).map_err(|error| {
            FetchFieldOptionsError::Permanent(format!("could not serialize profile: {error}"))
        })?;

        let request_body = AuthProviderRequestBody::FetchDynamicOptions(FetchFieldOptionsRequest {
            profile_json,
            field_id: field_id.to_string(),
            dependencies,
            session,
        });

        let responses = self
            .send_request(request_body)
            .map_err(|error| FetchFieldOptionsError::Transient(error.to_string()))?;

        let last = responses
            .last()
            .ok_or(FetchFieldOptionsError::SessionExpired)?;

        match &last.body {
            AuthProviderResponseBody::DynamicOptions(response) => Ok(response.clone()),
            AuthProviderResponseBody::Error(error) => {
                Err(FetchFieldOptionsError::Permanent(error.message.clone()))
            }
            _ => Err(FetchFieldOptionsError::SessionExpired),
        }
    }
}

/// Build the `dependencies` map to include in a `FetchFieldOptionsRequest`.
///
/// Password-kind field values are excluded unless both conditions hold:
/// 1. `secret_dependency_opt_in` is `true` (provider opted in).
/// 2. The Password field's id is listed in the target field's `depends_on`.
///
/// Exposed as a free function so unit tests can exercise the logic without
/// spinning up an RPC socket.
pub fn build_fetch_dependencies(
    form_def: &AuthFormDef,
    field_id: &str,
    raw_dependencies: &HashMap<String, String>,
    secret_dependency_opt_in: bool,
) -> HashMap<String, String> {
    // Collect the target field's `depends_on` list, if known.
    let depends_on: std::collections::HashSet<&str> = form_def
        .tabs
        .iter()
        .flat_map(|tab| tab.sections.iter())
        .flat_map(|section| section.fields.iter())
        .find(|field| field.id == field_id)
        .and_then(|field| {
            if let FormFieldKind::DynamicSelect { depends_on, .. } = &field.kind {
                Some(depends_on.iter().map(String::as_str).collect())
            } else {
                None
            }
        })
        .unwrap_or_default();

    // Collect a set of field ids that have Password kind.
    let password_field_ids: std::collections::HashSet<&str> = form_def
        .tabs
        .iter()
        .flat_map(|tab| tab.sections.iter())
        .flat_map(|section| section.fields.iter())
        .filter(|field| field.kind == FormFieldKind::Password)
        .map(|field| field.id.as_str())
        .collect();

    raw_dependencies
        .iter()
        .filter(|(dep_field_id, _)| {
            let is_password = password_field_ids.contains(dep_field_id.as_str());

            if !is_password {
                return true;
            }

            // For Password fields: include only when the provider opted in
            // AND the field is listed in the target field's depends_on.
            secret_dependency_opt_in && depends_on.contains(dep_field_id.as_str())
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Compute a stable hash of the dependencies map for cache keying.
///
/// The hash is derived from the sorted (key, value) pairs so it is
/// deterministic regardless of HashMap iteration order.
pub fn hash_dependencies(dependencies: &HashMap<String, String>) -> u64 {
    let mut pairs: Vec<(&str, &str)> = dependencies
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    pairs.sort_unstable();

    let mut hasher = DefaultHasher::new();
    for (key, value) in pairs {
        key.hash(&mut hasher);
        value.hash(&mut hasher);
    }
    hasher.finish()
}

#[async_trait::async_trait]
impl DynAuthProvider for RpcAuthProvider {
    fn provider_id(&self) -> &str {
        &self.provider_id
    }

    fn display_name(&self) -> &str {
        &self.display_name
    }

    fn form_def(&self) -> &AuthFormDef {
        &self.form_definition
    }

    fn capabilities(&self) -> &AuthProviderCapabilities {
        &self.capabilities
    }

    async fn validate_session(&self, profile: &AuthProfile) -> Result<AuthSessionState, DbError> {
        let profile_json = serde_json::to_string(profile)
            .map_err(|error| DbError::QueryFailed(error.to_string().into()))?;

        let responses = self.send_request(AuthProviderRequestBody::ValidateSession(
            ValidateSessionRequest { profile_json },
        ))?;

        let response = responses.last().ok_or_else(|| {
            DbError::connection_failed("Auth-provider returned no response".to_string())
        })?;

        match &response.body {
            AuthProviderResponseBody::SessionState { state } => Ok(state.clone().into()),
            AuthProviderResponseBody::Error(error) => Err(error.clone().into_db_error()),
            _ => Err(DbError::connection_failed(
                "Unexpected response to ValidateSession".to_string(),
            )),
        }
    }

    async fn login(
        &self,
        profile: &AuthProfile,
        url_callback: UrlCallback,
    ) -> Result<AuthSession, DbError> {
        let profile_json = serde_json::to_string(profile)
            .map_err(|error| DbError::QueryFailed(error.to_string().into()))?;

        let responses = self.send_request(AuthProviderRequestBody::Login(LoginRequest {
            profile_json,
        }))?;

        let mut url_callback = Some(url_callback);
        let mut saw_progress = false;

        for response in responses {
            match response.body {
                AuthProviderResponseBody::LoginUrlProgress(progress) => {
                    saw_progress = true;

                    if let Some(callback) = url_callback.take() {
                        callback(progress.verification_url);
                    }
                }
                AuthProviderResponseBody::LoginResult { session } => {
                    if !saw_progress && let Some(callback) = url_callback.take() {
                        callback(None);
                    }

                    return Ok(session.into());
                }
                AuthProviderResponseBody::Error(error) => return Err(error.into_db_error()),
                _ => {
                    return Err(DbError::connection_failed(
                        "Unexpected response to Login".to_string(),
                    ));
                }
            }
        }

        Err(DbError::connection_failed(
            "Auth-provider login did not return a terminal result".to_string(),
        ))
    }

    async fn resolve_credentials(
        &self,
        profile: &AuthProfile,
    ) -> Result<ResolvedCredentials, DbError> {
        let profile_json = serde_json::to_string(profile)
            .map_err(|error| DbError::QueryFailed(error.to_string().into()))?;

        let responses = self.send_request(AuthProviderRequestBody::ResolveCredentials(
            ResolveCredentialsRequest { profile_json },
        ))?;

        let response = responses.last().ok_or_else(|| {
            DbError::connection_failed("Auth-provider returned no response".to_string())
        })?;

        match &response.body {
            AuthProviderResponseBody::Credentials { credentials } => Ok(credentials.clone().into()),
            AuthProviderResponseBody::Error(error) => Err(error.clone().into_db_error()),
            _ => Err(DbError::connection_failed(
                "Unexpected response to ResolveCredentials".to_string(),
            )),
        }
    }
}

#[allow(clippy::result_large_err)]
fn validate_socket_id(socket_id: &str) -> Result<(), DbError> {
    if socket_id.is_empty()
        || !socket_id
            .chars()
            .all(|char| char.is_ascii_alphanumeric() || matches!(char, '.' | '_' | '-'))
    {
        return Err(DbError::connection_failed(format!(
            "Invalid socket ID '{}': use only letters, numbers, '.', '_' or '-'",
            socket_id
        )));
    }

    auth_provider_socket_name(socket_id)
        .map(|_| ())
        .map_err(|error| DbError::connection_failed(error.to_string()))
}

#[allow(clippy::result_large_err)]
fn validate_hello_selected_version(
    selected_version: ProtocolVersion,
    supported_versions: &[ProtocolVersion],
) -> Result<(), DbError> {
    if supported_versions.contains(&selected_version) {
        return Ok(());
    }

    Err(DbError::connection_failed(format!(
        "Auth-provider selected unsupported protocol version {}.{}",
        selected_version.major, selected_version.minor
    )))
}

#[allow(clippy::result_large_err)]
fn normalize_hello_response(
    body: AuthProviderResponseBody,
) -> Result<NormalizedAuthProviderHelloResponse, DbError> {
    match body {
        AuthProviderResponseBody::Hello(hello) => Ok(NormalizedAuthProviderHelloResponse {
            provider_id: hello.provider_id,
            display_name: hello.display_name,
            form_definition: hello.form_definition,
            capabilities: AuthProviderCapabilities::default(),
            selected_version: hello.selected_version,
            secret_dependency_opt_in: false,
        }),
        AuthProviderResponseBody::HelloV1_1(hello) => Ok(NormalizedAuthProviderHelloResponse {
            provider_id: hello.provider_id,
            display_name: hello.display_name,
            form_definition: hello.form_definition,
            capabilities: hello.capabilities,
            selected_version: hello.selected_version,
            secret_dependency_opt_in: false,
        }),
        AuthProviderResponseBody::HelloV1_2(hello) => Ok(NormalizedAuthProviderHelloResponse {
            provider_id: hello.provider_id,
            display_name: hello.display_name,
            form_definition: hello.form_definition,
            capabilities: hello.capabilities,
            selected_version: hello.selected_version,
            secret_dependency_opt_in: hello.secret_dependency_opt_in,
        }),
        _ => Err(DbError::connection_failed(
            "Unexpected response to auth-provider Hello".to_string(),
        )),
    }
}

fn managed_hosts() -> &'static Mutex<HashMap<String, Child>> {
    static MANAGED_HOSTS: OnceLock<Mutex<HashMap<String, Child>>> = OnceLock::new();
    MANAGED_HOSTS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[allow(clippy::result_large_err)]
fn socket_is_live_for(socket_id: &str) -> Result<bool, DbError> {
    let name = auth_provider_socket_name(socket_id)
        .map_err(|error| DbError::connection_failed(error.to_string()))?;

    match IpcStream::connect(name) {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

#[allow(clippy::result_large_err)]
fn managed_host_is_running(socket_id: &str) -> Result<bool, DbError> {
    let mut hosts = managed_hosts().lock().map_err(|_| {
        DbError::connection_failed("Managed auth-provider host registry is poisoned".to_string())
    })?;

    let mut should_remove = false;
    let is_running = if let Some(child) = hosts.get_mut(socket_id) {
        match child.try_wait().map_err(DbError::IoError)? {
            Some(_) => {
                should_remove = true;
                false
            }
            None => true,
        }
    } else {
        false
    };

    if should_remove {
        hosts.remove(socket_id);
    }

    Ok(is_running)
}

#[allow(clippy::result_large_err)]
fn register_managed_host(socket_id: &str, mut child: Child) -> Result<(), DbError> {
    let mut hosts = match managed_hosts().lock() {
        Ok(hosts) => hosts,
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(DbError::connection_failed(
                "Managed auth-provider host registry is poisoned".to_string(),
            ));
        }
    };

    if let Some(mut previous) = hosts.insert(socket_id.to_string(), child)
        && let Ok(None) = previous.try_wait()
    {
        let _ = previous.kill();
        let _ = previous.wait();
    }

    Ok(())
}

#[allow(clippy::result_large_err)]
fn ensure_host_running_for(
    socket_id: &str,
    launch: Option<&IpcServiceLaunchConfig>,
) -> Result<(), DbError> {
    if socket_is_live_for(socket_id)? {
        return Ok(());
    }

    if managed_host_is_running(socket_id)? {
        let startup_timeout = launch
            .map(|config| config.startup_timeout)
            .unwrap_or_else(|| Duration::from_millis(2_000));
        let deadline = Instant::now() + startup_timeout;

        while Instant::now() < deadline {
            if socket_is_live_for(socket_id)? {
                return Ok(());
            }

            if !managed_host_is_running(socket_id)? {
                break;
            }

            thread::sleep(Duration::from_millis(75));
        }

        if managed_host_is_running(socket_id)? {
            return Err(DbError::connection_failed(format!(
                "Managed auth-provider host for '{}' is running but socket is unavailable",
                socket_id
            )));
        }
    }

    let Some(launch) = launch else {
        return Err(DbError::connection_failed(format!(
            "Auth-provider socket '{}' is not available",
            socket_id
        )));
    };

    let mut command = Command::new(&launch.program);
    command.args(&launch.args);
    command.envs(launch.env.iter().cloned());

    let child = command.spawn().map_err(DbError::IoError)?;
    register_managed_host(socket_id, child)?;

    let deadline = Instant::now() + launch.startup_timeout;
    while Instant::now() < deadline {
        if socket_is_live_for(socket_id)? {
            return Ok(());
        }

        if !managed_host_is_running(socket_id)? {
            return Err(DbError::connection_failed(format!(
                "Managed auth-provider host for '{}' exited before socket was ready",
                socket_id
            )));
        }

        thread::sleep(Duration::from_millis(75));
    }

    Err(DbError::connection_failed(format!(
        "Managed auth-provider host for '{}' did not become ready within {} ms",
        socket_id,
        launch.startup_timeout.as_millis()
    )))
}

pub fn negotiate_auth_provider_version(
    remote_versions: &[ProtocolVersion],
) -> Option<ProtocolVersion> {
    negotiate_highest_mutual_version(
        RpcApiFamily::AuthProviderRpc,
        auth_provider_rpc_supported_versions(),
        remote_versions,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth_provider_protocol::{
        AuthProviderHelloResponse, AuthProviderHelloResponseV1_1, AuthProviderHelloResponseV1_2,
    };
    use dbflux_core::auth::{AuthFormDef, AuthProviderCapabilities, AuthProviderLoginCapabilities};
    use dbflux_core::{FormFieldDef, FormFieldKind, FormSection, FormTab, RefreshTrigger};

    #[test]
    fn normalize_hello_response_defaults_v1_0_capabilities_to_disabled() {
        let normalized =
            normalize_hello_response(AuthProviderResponseBody::Hello(AuthProviderHelloResponse {
                server_name: "legacy-auth-provider".to_string(),
                server_version: "0.0.0-test".to_string(),
                selected_version: ProtocolVersion::new(1, 0),
                provider_id: "legacy-auth".to_string(),
                display_name: "Legacy Auth".to_string(),
                form_definition: AuthFormDef { tabs: vec![] },
            }))
            .expect("legacy hello should normalize");

        assert_eq!(normalized.capabilities, AuthProviderCapabilities::default());
    }

    #[test]
    fn normalize_hello_response_keeps_v1_1_capabilities() {
        let normalized = normalize_hello_response(AuthProviderResponseBody::HelloV1_1(
            AuthProviderHelloResponseV1_1 {
                server_name: "auth-provider".to_string(),
                server_version: "0.0.0-test".to_string(),
                selected_version: ProtocolVersion::new(1, 1),
                provider_id: "rpc-auth".to_string(),
                display_name: "RPC Auth".to_string(),
                form_definition: AuthFormDef { tabs: vec![] },
                capabilities: AuthProviderCapabilities {
                    login: AuthProviderLoginCapabilities {
                        supported: true,
                        verification_url_progress: true,
                    },
                },
            },
        ))
        .expect("v1.1 hello should normalize");

        assert!(normalized.capabilities.login.supported);
        assert!(normalized.capabilities.login.verification_url_progress);
    }

    #[test]
    fn normalize_hello_response_keeps_v1_2_secret_dependency_opt_in() {
        let normalized = normalize_hello_response(AuthProviderResponseBody::HelloV1_2(
            AuthProviderHelloResponseV1_2 {
                server_name: "auth-provider".to_string(),
                server_version: "0.0.0-test".to_string(),
                selected_version: ProtocolVersion::new(1, 2),
                provider_id: "rpc-auth".to_string(),
                display_name: "RPC Auth".to_string(),
                form_definition: AuthFormDef { tabs: vec![] },
                capabilities: AuthProviderCapabilities::default(),
                secret_dependency_opt_in: true,
            },
        ))
        .expect("v1.2 hello should normalize");

        assert!(normalized.secret_dependency_opt_in);
    }

    /// Builds a minimal `AuthFormDef` with a Password field and a DynamicSelect
    /// that declares a dependency on that Password field.
    fn form_with_password_dep() -> AuthFormDef {
        AuthFormDef {
            tabs: vec![FormTab {
                id: "main".to_string(),
                label: "Main".to_string(),
                sections: vec![FormSection {
                    title: "Fields".to_string(),
                    fields: vec![
                        FormFieldDef {
                            id: "api_key".to_string(),
                            label: "API Key".to_string(),
                            kind: FormFieldKind::Password,
                            placeholder: String::new(),
                            required: false,
                            default_value: String::new(),
                            enabled_when_checked: None,
                            enabled_when_unchecked: None,
                            help: None,
                        },
                        FormFieldDef {
                            id: "region".to_string(),
                            label: "Region".to_string(),
                            kind: FormFieldKind::Text,
                            placeholder: String::new(),
                            required: false,
                            default_value: String::new(),
                            enabled_when_checked: None,
                            enabled_when_unchecked: None,
                            help: None,
                        },
                        FormFieldDef {
                            id: "environment".to_string(),
                            label: "Environment".to_string(),
                            kind: FormFieldKind::DynamicSelect {
                                depends_on: vec!["region".to_string(), "api_key".to_string()],
                                refresh: RefreshTrigger::OnDependencyChange,
                                requires_session: false,
                                allow_freeform: false,
                            },
                            placeholder: String::new(),
                            required: false,
                            default_value: String::new(),
                            enabled_when_checked: None,
                            enabled_when_unchecked: None,
                            help: None,
                        },
                    ],
                }],
            }],
        }
    }

    /// FR-SEC-01: Password fields are stripped from `dependencies` unless the
    /// provider has opted in AND the field is listed in the target's `depends_on`.
    #[test]
    fn build_fetch_dependencies_strips_password_fields_when_not_opted_in() {
        let form_def = form_with_password_dep();

        let mut raw = HashMap::new();
        raw.insert("region".to_string(), "us-east-1".to_string());
        raw.insert("api_key".to_string(), "secret-value".to_string());

        let deps = build_fetch_dependencies(&form_def, "environment", &raw, false);

        assert_eq!(
            deps.get("region").map(String::as_str),
            Some("us-east-1"),
            "non-secret dep should be included"
        );
        assert!(
            deps.get("api_key").is_none(),
            "password dep must be stripped when opt-in is false"
        );
    }

    /// FR-SEC-02: When the provider has opted in, Password fields that appear
    /// in the target field's `depends_on` are forwarded.
    #[test]
    fn build_fetch_dependencies_includes_password_when_opted_in() {
        let form_def = form_with_password_dep();

        let mut raw = HashMap::new();
        raw.insert("region".to_string(), "us-east-1".to_string());
        raw.insert("api_key".to_string(), "secret-value".to_string());

        let deps = build_fetch_dependencies(&form_def, "environment", &raw, true);

        assert_eq!(
            deps.get("api_key").map(String::as_str),
            Some("secret-value"),
            "password dep must be included when provider opted in"
        );
    }

    /// A Password field NOT listed in `depends_on` must be stripped even when
    /// `secret_dependency_opt_in` is true.
    #[test]
    fn build_fetch_dependencies_strips_password_not_in_depends_on_even_when_opted_in() {
        // Build a form that has a second password field NOT listed in environment.depends_on.
        let form_def = AuthFormDef {
            tabs: vec![FormTab {
                id: "main".to_string(),
                label: "Main".to_string(),
                sections: vec![FormSection {
                    title: "Fields".to_string(),
                    fields: vec![
                        FormFieldDef {
                            id: "api_key".to_string(),
                            label: "API Key".to_string(),
                            kind: FormFieldKind::Password,
                            placeholder: String::new(),
                            required: false,
                            default_value: String::new(),
                            enabled_when_checked: None,
                            enabled_when_unchecked: None,
                            help: None,
                        },
                        // A second password field NOT referenced by environment.depends_on.
                        FormFieldDef {
                            id: "other_secret".to_string(),
                            label: "Other Secret".to_string(),
                            kind: FormFieldKind::Password,
                            placeholder: String::new(),
                            required: false,
                            default_value: String::new(),
                            enabled_when_checked: None,
                            enabled_when_unchecked: None,
                            help: None,
                        },
                        FormFieldDef {
                            id: "region".to_string(),
                            label: "Region".to_string(),
                            kind: FormFieldKind::Text,
                            placeholder: String::new(),
                            required: false,
                            default_value: String::new(),
                            enabled_when_checked: None,
                            enabled_when_unchecked: None,
                            help: None,
                        },
                        FormFieldDef {
                            id: "environment".to_string(),
                            label: "Environment".to_string(),
                            kind: FormFieldKind::DynamicSelect {
                                // Only api_key is in depends_on, not other_secret.
                                depends_on: vec!["region".to_string(), "api_key".to_string()],
                                refresh: RefreshTrigger::OnDependencyChange,
                                requires_session: false,
                                allow_freeform: false,
                            },
                            placeholder: String::new(),
                            required: false,
                            default_value: String::new(),
                            enabled_when_checked: None,
                            enabled_when_unchecked: None,
                            help: None,
                        },
                    ],
                }],
            }],
        };

        let mut raw = HashMap::new();
        raw.insert("region".to_string(), "us-east-1".to_string());
        raw.insert("api_key".to_string(), "allowed-secret".to_string());
        raw.insert("other_secret".to_string(), "do-not-send".to_string());

        let deps = build_fetch_dependencies(&form_def, "environment", &raw, true);

        assert_eq!(
            deps.get("api_key").map(String::as_str),
            Some("allowed-secret"),
            "api_key is in depends_on so must be included when opted in"
        );
        assert!(
            deps.get("other_secret").is_none(),
            "other_secret is a Password field not in depends_on — must be stripped even when opted in"
        );
    }
}
