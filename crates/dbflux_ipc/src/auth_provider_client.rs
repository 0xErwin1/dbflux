use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::process::{Child, Command};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use dbflux_core::DbError;
use dbflux_core::FormFieldKind;
use dbflux_core::auth::{
    AuthFormDef, AuthProfile, AuthProviderCapabilities, AuthSession, AuthSessionState,
    DynAuthProvider, ResolvedCredentials, UrlCallback,
};
use interprocess::local_socket::{Stream as IpcStream, prelude::*};

use crate::audit::{ExternalAuditEmitter, ExternalAuditSource};
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
    /// Whether the provider will emit `EmitAuditEvent` intermediate frames (v1.3+ only).
    audit_emit_opt_in: bool,
    /// Sanitizing sink for audit frames emitted by this provider.
    audit_emitter: Option<Arc<dyn ExternalAuditEmitter>>,
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
    /// Whether the provider opted in to emitting audit events (v1.3+).
    audit_emit_opt_in: bool,
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
            audit_emit_opt_in: hello.audit_emit_opt_in,
            audit_emitter: None,
            launch,
        })
    }

    /// Attaches an audit emitter for routing `EmitAuditEvent` intermediate frames.
    ///
    /// Must be called after `probe` and before any `DynAuthProvider` method is invoked.
    /// Has no effect unless the provider set `audit_emit_opt_in=true` in its hello.
    pub fn with_audit_emitter(mut self, emitter: Arc<dyn ExternalAuditEmitter>) -> Self {
        self.audit_emitter = Some(emitter);
        self
    }

    /// Constructs a minimal provider for testing the dispatch loop without a real socket.
    #[cfg(test)]
    fn new_for_test(
        socket_id: &str,
        provider_id: &str,
        audit_emit_opt_in: bool,
        audit_emitter: Option<Arc<dyn ExternalAuditEmitter>>,
    ) -> Self {
        use crate::envelope::AUTH_PROVIDER_RPC_VERSION;
        use dbflux_core::auth::AuthProviderCapabilities;

        Self {
            socket_id: socket_id.to_string(),
            provider_id: provider_id.to_string(),
            display_name: "Test Provider".to_string(),
            form_definition: dbflux_core::auth::AuthFormDef { tabs: vec![] },
            capabilities: AuthProviderCapabilities::default(),
            selected_version: AUTH_PROVIDER_RPC_VERSION,
            secret_dependency_opt_in: false,
            audit_emit_opt_in,
            audit_emitter,
            launch: None,
        }
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
            | AuthProviderResponseBody::HelloV1_2(_)
            | AuthProviderResponseBody::HelloV1_3(_) => {
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
        self.dispatch_request_loop(&mut stream, body)
    }

    /// Sends `body` to the provider over `stream` and collects all response frames.
    ///
    /// The loop exits on the first frame with `done = true`. To guard against misbehaving
    /// providers that send frames indefinitely, two circuit breakers are applied:
    /// - **Frame cap** (`MAX_INTERMEDIATE_FRAMES`): aborts after processing this many frames
    ///   without a terminal `done = true` frame.
    /// - **Per-request deadline** (`PER_REQUEST_DEADLINE`): aborts when the wall-clock time
    ///   since the request started exceeds the limit.
    ///
    /// Note: a `recv_msg` call that never returns cannot be interrupted by these guards —
    /// socket-level read timeouts are not portable for the `interprocess` stream type used here.
    #[allow(clippy::result_large_err)]
    fn dispatch_request_loop<S: std::io::Read + std::io::Write>(
        &self,
        stream: &mut S,
        body: AuthProviderRequestBody,
    ) -> Result<Vec<AuthProviderResponseEnvelope>, DbError> {
        const MAX_INTERMEDIATE_FRAMES: usize = 1000;
        const PER_REQUEST_DEADLINE: Duration = Duration::from_secs(60);

        let request = AuthProviderRequestEnvelope::new(self.selected_version, 1, body);
        let correlation_id = uuid::Uuid::new_v4().to_string();

        framing::send_msg(&mut *stream, &request)?;

        let deadline = Instant::now() + PER_REQUEST_DEADLINE;
        let mut frame_count: usize = 0;
        let mut responses = Vec::new();

        loop {
            let response: AuthProviderResponseEnvelope = framing::recv_msg(&mut *stream)?;

            frame_count += 1;

            if frame_count > MAX_INTERMEDIATE_FRAMES {
                log::warn!(
                    "auth-provider '{}' exceeded {} frames in one request; aborting",
                    self.provider_id,
                    MAX_INTERMEDIATE_FRAMES
                );
                return Err(DbError::connection_failed(format!(
                    "auth-provider '{}' exceeded frame budget ({} intermediate frames)",
                    self.provider_id, MAX_INTERMEDIATE_FRAMES
                )));
            }

            if Instant::now() > deadline {
                log::warn!(
                    "auth-provider '{}' request exceeded {:?} deadline; aborting",
                    self.provider_id,
                    PER_REQUEST_DEADLINE
                );
                return Err(DbError::connection_failed(format!(
                    "auth-provider '{}' request exceeded {:?}",
                    self.provider_id, PER_REQUEST_DEADLINE
                )));
            }

            if response.request_id != request.request_id {
                return Err(DbError::connection_failed(format!(
                    "Request ID mismatch: sent {}, got {}",
                    request.request_id, response.request_id
                )));
            }

            let done = response.done;

            match &response.body {
                AuthProviderResponseBody::EmitAuditEvent(dto) if !done => {
                    if self.audit_emit_opt_in
                        && let Some(sink) = &self.audit_emitter
                    {
                        let source = ExternalAuditSource::AuthProvider {
                            socket_id: self.socket_id.clone(),
                            provider_id: self.provider_id.clone(),
                            correlation_id: correlation_id.clone(),
                        };
                        sink.emit(source, dto.clone());
                    }
                    // Never push EmitAuditEvent frames to `responses` — keep it terminal-only.
                }
                _ => {
                    responses.push(response);
                }
            }

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

        let profile_json = profile_to_wire_json(profile).map_err(|error| {
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

    // Collect a set of field ids that have Password or WriteOnly kind.
    // Both carry secret values and are filtered from dependency maps sent to
    // external RPC providers unless the provider opts in via
    // `secret_dependency_opt_in`.
    let password_field_ids: std::collections::HashSet<&str> = form_def
        .tabs
        .iter()
        .flat_map(|tab| tab.sections.iter())
        .flat_map(|section| section.fields.iter())
        .filter(|field| {
            field.kind == FormFieldKind::Password || field.kind == FormFieldKind::WriteOnly
        })
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

/// Serialize an auth profile for the IPC wire, re-merging secret-kind field
/// values back into the flat `fields` map the external provider expects.
///
/// This is the single boundary where `AuthProfile::secret_fields` are exposed
/// in plaintext. The derived `Serialize` skips that map, so without this merge
/// an external provider would never receive the secret values it needs to
/// authenticate. The output shape is identical to the legacy
/// `serde_json::to_string(profile)` that kept secrets inline in `fields`.
fn profile_to_wire_json(profile: &AuthProfile) -> Result<String, serde_json::Error> {
    use secrecy::ExposeSecret;

    let mut value = serde_json::to_value(profile)?;

    if let Some(fields) = value.get_mut("fields").and_then(|f| f.as_object_mut()) {
        for (key, secret) in &profile.secret_fields {
            fields.insert(
                key.clone(),
                serde_json::Value::String(secret.expose_secret().to_string()),
            );
        }
    }

    serde_json::to_string(&value)
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
        let profile_json = profile_to_wire_json(profile)
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
        let profile_json = profile_to_wire_json(profile)
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
        let profile_json = profile_to_wire_json(profile)
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
            audit_emit_opt_in: false,
        }),
        AuthProviderResponseBody::HelloV1_1(hello) => Ok(NormalizedAuthProviderHelloResponse {
            provider_id: hello.provider_id,
            display_name: hello.display_name,
            form_definition: hello.form_definition,
            capabilities: hello.capabilities,
            selected_version: hello.selected_version,
            secret_dependency_opt_in: false,
            audit_emit_opt_in: false,
        }),
        AuthProviderResponseBody::HelloV1_2(hello) => Ok(NormalizedAuthProviderHelloResponse {
            provider_id: hello.provider_id,
            display_name: hello.display_name,
            form_definition: hello.form_definition,
            capabilities: hello.capabilities,
            selected_version: hello.selected_version,
            secret_dependency_opt_in: hello.secret_dependency_opt_in,
            audit_emit_opt_in: false,
        }),
        AuthProviderResponseBody::HelloV1_3(hello) => Ok(NormalizedAuthProviderHelloResponse {
            provider_id: hello.provider_id,
            display_name: hello.display_name,
            form_definition: hello.form_definition,
            capabilities: hello.capabilities,
            selected_version: hello.selected_version,
            secret_dependency_opt_in: hello.secret_dependency_opt_in,
            audit_emit_opt_in: hello.audit_emit_opt_in,
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

/// Stops all auth-provider host processes started by DBFlux. Returns count terminated.
pub fn shutdown_managed_auth_provider_hosts() -> usize {
    let mut children = {
        let Ok(mut hosts) = managed_hosts().lock() else {
            log::error!("Managed auth-provider host registry is poisoned");
            return 0;
        };
        std::mem::take(&mut *hosts)
    };

    let mut stopped = 0;

    for (socket_id, mut child) in children.drain() {
        match child.try_wait() {
            Ok(Some(status)) => {
                log::info!(
                    "Auth-provider host for '{}' already exited before shutdown ({})",
                    socket_id,
                    status
                );
            }
            Ok(None) => {
                if let Err(error) = child.kill() {
                    log::warn!(
                        "Failed to kill auth-provider host '{}': {}",
                        socket_id,
                        error
                    );
                    continue;
                }
                if let Err(error) = child.wait() {
                    log::warn!(
                        "Failed to wait for auth-provider host '{}' after kill: {}",
                        socket_id,
                        error
                    );
                }
                stopped += 1;
            }
            Err(error) => {
                log::warn!(
                    "Failed to inspect auth-provider host '{}': {}",
                    socket_id,
                    error
                );
            }
        }
    }

    stopped
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
        AuthProviderHelloResponseV1_3,
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
                    edit: None,
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
                            disabled_when_field_set: None,
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
                            disabled_when_field_set: None,
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
                            disabled_when_field_set: None,
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
            !deps.contains_key("api_key"),
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
                            disabled_when_field_set: None,
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
                            disabled_when_field_set: None,
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
                            disabled_when_field_set: None,
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
                            disabled_when_field_set: None,
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
            !deps.contains_key("other_secret"),
            "other_secret is a Password field not in depends_on — must be stripped even when opted in"
        );
    }

    #[test]
    fn test_auth_provider_hello_v1_3_serde() {
        let response = AuthProviderHelloResponseV1_3 {
            server_name: "test-provider".to_string(),
            server_version: "1.0.0".to_string(),
            selected_version: crate::ProtocolVersion::new(1, 3),
            provider_id: "test-auth".to_string(),
            display_name: "Test Auth".to_string(),
            form_definition: AuthFormDef { tabs: vec![] },
            capabilities: AuthProviderCapabilities::default(),
            secret_dependency_opt_in: false,
            audit_emit_opt_in: true,
        };

        let json = serde_json::to_string(&response).expect("serialize v1.3 hello");
        let restored: AuthProviderHelloResponseV1_3 =
            serde_json::from_str(&json).expect("deserialize v1.3 hello");

        assert!(restored.audit_emit_opt_in);
        assert_eq!(restored.provider_id, "test-auth");
    }

    #[test]
    fn test_normalize_hello_v1_3_has_audit_emit_opt_in() {
        let normalized = normalize_hello_response(AuthProviderResponseBody::HelloV1_3(
            AuthProviderHelloResponseV1_3 {
                server_name: "test-provider".to_string(),
                server_version: "1.0.0".to_string(),
                selected_version: crate::ProtocolVersion::new(1, 3),
                provider_id: "test-auth".to_string(),
                display_name: "Test Auth".to_string(),
                form_definition: AuthFormDef { tabs: vec![] },
                capabilities: AuthProviderCapabilities::default(),
                secret_dependency_opt_in: false,
                audit_emit_opt_in: true,
            },
        ))
        .expect("v1.3 hello should normalize");

        assert!(normalized.audit_emit_opt_in);
    }

    #[test]
    fn test_normalize_hello_v1_2_has_opt_in_false() {
        let normalized = normalize_hello_response(AuthProviderResponseBody::HelloV1_2(
            AuthProviderHelloResponseV1_2 {
                server_name: "test-provider".to_string(),
                server_version: "1.0.0".to_string(),
                selected_version: crate::ProtocolVersion::new(1, 2),
                provider_id: "test-auth".to_string(),
                display_name: "Test Auth".to_string(),
                form_definition: AuthFormDef { tabs: vec![] },
                capabilities: AuthProviderCapabilities::default(),
                secret_dependency_opt_in: false,
            },
        ))
        .expect("v1.2 hello should normalize");

        assert!(
            !normalized.audit_emit_opt_in,
            "v1.2 hello must have audit_emit_opt_in == false"
        );
    }

    // =========================================================================
    // Layer C: dispatch_request_loop — audit frame interception
    // =========================================================================

    use crate::audit::{
        AuditEventEmitDto, EventCategoryDto, EventOutcomeDto, EventSeverityDto,
        ExternalAuditEmitter, ExternalAuditSource,
    };
    use std::sync::{Arc, Mutex};

    /// In-memory stream that supplies pre-encoded response bytes and absorbs writes.
    struct MockStream {
        reader: std::io::Cursor<Vec<u8>>,
        writer: Vec<u8>,
    }

    impl MockStream {
        fn new(response_bytes: Vec<u8>) -> Self {
            Self {
                reader: std::io::Cursor::new(response_bytes),
                writer: Vec::new(),
            }
        }
    }

    impl std::io::Read for MockStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.reader.read(buf)
        }
    }

    impl std::io::Write for MockStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.writer.write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.writer.flush()
        }
    }

    /// Recording emitter that captures every `emit` call for test assertions.
    #[derive(Default)]
    struct RecordingEmitter {
        calls: Arc<Mutex<Vec<ExternalAuditSource>>>,
    }

    impl RecordingEmitter {
        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    impl ExternalAuditEmitter for RecordingEmitter {
        fn emit(&self, source: ExternalAuditSource, _dto: AuditEventEmitDto) {
            self.calls.lock().unwrap().push(source);
        }
    }

    fn minimal_emit_dto() -> AuditEventEmitDto {
        AuditEventEmitDto {
            ts_ms: 1_700_000_000_000,
            level: EventSeverityDto::Info,
            category: EventCategoryDto::Connection,
            action: "connect".to_string(),
            outcome: EventOutcomeDto::Success,
            summary: "provider connected".to_string(),
            object_type: None,
            object_id: None,
            duration_ms: None,
            error_code: None,
            error_message: None,
            details_json: None,
        }
    }

    fn encode_response(envelope: &AuthProviderResponseEnvelope) -> Vec<u8> {
        let mut buf = Vec::new();
        framing::send_msg(&mut buf, envelope).expect("encode response");
        buf
    }

    fn emit_audit_frame(request_id: u64, dto: AuditEventEmitDto) -> AuthProviderResponseEnvelope {
        AuthProviderResponseEnvelope {
            protocol_version: crate::AUTH_PROVIDER_RPC_VERSION,
            request_id,
            done: false,
            body: AuthProviderResponseBody::EmitAuditEvent(dto),
        }
    }

    fn login_result_frame(request_id: u64) -> AuthProviderResponseEnvelope {
        use crate::auth_provider_protocol::AuthSessionDto;
        AuthProviderResponseEnvelope {
            protocol_version: crate::AUTH_PROVIDER_RPC_VERSION,
            request_id,
            done: true,
            body: AuthProviderResponseBody::LoginResult {
                session: AuthSessionDto {
                    provider_id: "test-auth".to_string(),
                    profile_id: uuid::Uuid::nil(),
                    expires_at: None,
                    session_data: None,
                },
            },
        }
    }

    /// Scenario P-03-a: Provider has opt-in=true and emitter attached.
    /// One EmitAuditEvent frame (done=false) followed by a terminal LoginResult (done=true).
    /// Emitter should be called once; caller receives only the LoginResult.
    #[test]
    fn test_send_request_dispatches_emit_audit_frame() {
        let emitter = Arc::new(RecordingEmitter::default());
        let provider = RpcAuthProvider::new_for_test(
            "test-sock",
            "test-auth",
            true,
            Some(emitter.clone() as Arc<dyn ExternalAuditEmitter>),
        );

        let mut response_bytes = Vec::new();
        response_bytes.extend(encode_response(&emit_audit_frame(1, minimal_emit_dto())));
        response_bytes.extend(encode_response(&login_result_frame(1)));

        let mut stream = MockStream::new(response_bytes);
        let responses = provider
            .dispatch_request_loop(
                &mut stream,
                AuthProviderRequestBody::Login(crate::auth_provider_protocol::LoginRequest {
                    profile_json: "{}".to_string(),
                }),
            )
            .expect("dispatch should succeed");

        assert_eq!(
            emitter.call_count(),
            1,
            "emitter must be called once for the EmitAuditEvent frame"
        );
        assert_eq!(
            responses.len(),
            1,
            "caller must receive only the terminal LoginResult"
        );
        assert!(
            matches!(
                responses[0].body,
                AuthProviderResponseBody::LoginResult { .. }
            ),
            "terminal response must be LoginResult"
        );
    }

    // =========================================================================
    // PROC-5: dispatch_request_loop frame cap
    // =========================================================================

    /// Builds a stream containing `count` non-done EmitAuditEvent frames with no
    /// terminal frame. Used to verify the iteration cap aborts the loop.
    fn infinite_emit_stream(count: usize) -> MockStream {
        let mut response_bytes = Vec::new();
        for _ in 0..count {
            response_bytes.extend(encode_response(&emit_audit_frame(1, minimal_emit_dto())));
        }
        MockStream::new(response_bytes)
    }

    #[test]
    fn test_dispatch_loop_terminates_on_cap() {
        let provider = RpcAuthProvider::new_for_test("test-sock", "test-auth", true, None);

        // Feed 1001 non-done frames — should abort before processing the 1001st.
        let mut stream = infinite_emit_stream(1001);

        let result = provider.dispatch_request_loop(
            &mut stream,
            AuthProviderRequestBody::Login(crate::auth_provider_protocol::LoginRequest {
                profile_json: "{}".to_string(),
            }),
        );

        assert!(result.is_err(), "loop must abort after exceeding frame cap");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("frame budget") || err_msg.contains("exceeded"),
            "error must indicate frame cap reached, got: {err_msg}"
        );
    }

    #[test]
    fn test_dispatch_loop_terminates_normally() {
        let provider = RpcAuthProvider::new_for_test("test-sock", "test-auth", false, None);

        let mut response_bytes = Vec::new();
        response_bytes.extend(encode_response(&emit_audit_frame(1, minimal_emit_dto())));
        response_bytes.extend(encode_response(&emit_audit_frame(1, minimal_emit_dto())));
        response_bytes.extend(encode_response(&login_result_frame(1)));

        let mut stream = MockStream::new(response_bytes);
        let result = provider.dispatch_request_loop(
            &mut stream,
            AuthProviderRequestBody::Login(crate::auth_provider_protocol::LoginRequest {
                profile_json: "{}".to_string(),
            }),
        );

        assert!(
            result.is_ok(),
            "loop must terminate normally on done=true frame"
        );
        assert_eq!(
            result.unwrap().len(),
            1,
            "must return exactly one terminal frame"
        );
    }

    // =========================================================================
    // PROC-6: shutdown_managed_auth_provider_hosts
    // =========================================================================

    #[test]
    fn test_shutdown_kills_tracked_children() {
        use std::process::Command as StdCommand;

        // Register a sleeping child into the managed hosts registry
        let child = StdCommand::new("sleep")
            .arg("100")
            .spawn()
            .expect("spawn sleep");

        let child_id = format!("test-auth-provider-{}", child.id());
        {
            let mut hosts = managed_hosts().lock().unwrap();
            hosts.insert(child_id.clone(), child);
        }

        let stopped = shutdown_managed_auth_provider_hosts();

        assert_eq!(stopped, 1, "must report 1 stopped process");

        let hosts = managed_hosts().lock().unwrap();
        assert!(hosts.is_empty(), "registry must be empty after shutdown");
    }

    #[test]
    fn test_shutdown_returns_zero_on_empty() {
        // Ensure registry is empty first (may be populated by other tests)
        {
            let mut hosts = managed_hosts().lock().unwrap();
            hosts.clear();
        }

        let stopped = shutdown_managed_auth_provider_hosts();
        assert_eq!(stopped, 0, "must return 0 when no children tracked");
    }

    /// Scenario B-04-a: Provider has opt-in=false.
    /// EmitAuditEvent frame arrives; emitter must NOT be called.
    /// Caller receives only the terminal LoginResult.
    #[test]
    fn test_send_request_opt_in_false_drops_emit_frame() {
        let emitter = Arc::new(RecordingEmitter::default());
        let provider = RpcAuthProvider::new_for_test(
            "test-sock",
            "test-auth",
            false,
            Some(emitter.clone() as Arc<dyn ExternalAuditEmitter>),
        );

        let mut response_bytes = Vec::new();
        response_bytes.extend(encode_response(&emit_audit_frame(1, minimal_emit_dto())));
        response_bytes.extend(encode_response(&login_result_frame(1)));

        let mut stream = MockStream::new(response_bytes);
        let responses = provider
            .dispatch_request_loop(
                &mut stream,
                AuthProviderRequestBody::Login(crate::auth_provider_protocol::LoginRequest {
                    profile_json: "{}".to_string(),
                }),
            )
            .expect("dispatch should succeed even with opt-in=false");

        assert_eq!(
            emitter.call_count(),
            0,
            "emitter must NOT be called when audit_emit_opt_in=false"
        );
        assert_eq!(
            responses.len(),
            1,
            "caller must still receive the terminal LoginResult"
        );
        assert!(
            matches!(
                responses[0].body,
                AuthProviderResponseBody::LoginResult { .. }
            ),
            "terminal response must be LoginResult"
        );
    }
}
