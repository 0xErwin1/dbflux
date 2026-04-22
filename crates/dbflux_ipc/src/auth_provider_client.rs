use std::collections::HashMap;
use std::process::{Child, Command};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use dbflux_core::DbError;
use dbflux_core::auth::{AuthFormDef, AuthProfile, AuthSession, AuthSessionState, DynAuthProvider, ResolvedCredentials, UrlCallback};
use interprocess::local_socket::{Stream as IpcStream, prelude::*};

use crate::auth::AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV;
use crate::auth_provider_protocol::{
    AuthProviderHelloRequest, AuthProviderHelloResponse, AuthProviderRequestBody,
    AuthProviderRequestEnvelope, AuthProviderResponseBody, AuthProviderResponseEnvelope,
    LoginRequest, ResolveCredentialsRequest, ValidateSessionRequest,
};
use crate::envelope::{AUTH_PROVIDER_RPC_API_CONTRACT, ProtocolVersion};
use crate::framing;
use crate::socket::auth_provider_socket_name;
use crate::{auth_provider_rpc_supported_versions, negotiate_highest_mutual_version, RpcApiFamily};

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
    selected_version: ProtocolVersion,
    launch: Option<IpcServiceLaunchConfig>,
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
            selected_version: hello.selected_version,
            launch,
        })
    }

    fn connect_stream(&self) -> Result<IpcStream, DbError> {
        ensure_host_running_for(&self.socket_id, self.launch.as_ref())?;

        let name = auth_provider_socket_name(&self.socket_id)
            .map_err(|error| DbError::connection_failed(error.to_string()))?;

        IpcStream::connect(name).map_err(|error| DbError::connection_failed(error.to_string()))
    }

    fn perform_hello(
        socket_id: &str,
        launch: Option<&IpcServiceLaunchConfig>,
    ) -> Result<AuthProviderHelloResponse, DbError> {
        ensure_host_running_for(socket_id, launch)?;

        let name = auth_provider_socket_name(socket_id)
            .map_err(|error| DbError::connection_failed(error.to_string()))?;

        let mut stream =
            IpcStream::connect(name).map_err(|error| DbError::connection_failed(error.to_string()))?;

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
            AuthProviderResponseBody::Hello(hello) => {
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

    fn send_request(&self, body: AuthProviderRequestBody) -> Result<Vec<AuthProviderResponseEnvelope>, DbError> {
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

    async fn validate_session(&self, profile: &AuthProfile) -> Result<AuthSessionState, DbError> {
        let profile_json = serde_json::to_string(profile)
            .map_err(|error| DbError::QueryFailed(error.to_string().into()))?;

        let responses = self.send_request(AuthProviderRequestBody::ValidateSession(
            ValidateSessionRequest { profile_json },
        ))?;

        let response = responses
            .last()
            .ok_or_else(|| DbError::connection_failed("Auth-provider returned no response".to_string()))?;

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

        let response = responses
            .last()
            .ok_or_else(|| DbError::connection_failed("Auth-provider returned no response".to_string()))?;

        match &response.body {
            AuthProviderResponseBody::Credentials { credentials } => {
                Ok(credentials.clone().into())
            }
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
    let mut hosts = managed_hosts()
        .lock()
        .map_err(|_| DbError::connection_failed("Managed auth-provider host registry is poisoned".to_string()))?;

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
