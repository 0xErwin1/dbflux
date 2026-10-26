use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use dbflux_core::DbError;
use dbflux_core::auth::ResolvedCredentials;
use dbflux_core::values::{DynSecretProvider, ProviderError, ResolveContext};
use secrecy::SecretString;

use super::host::{connect, ensure_host_running_for, validate_socket_id};
use super::manifest::{ValidatedManifest, validate_manifest};
use super::protocol::{
    AuthProfileDependency, GetSecretRequest, SecretProviderHelloRequest, SecretProviderRequestBody,
    SecretProviderRequestEnvelope, SecretProviderResponseBody, SecretProviderResponseEnvelope,
    SecretProviderRpcError, SecretProviderRpcErrorCode,
};
use crate::auth::{SECRET_PROVIDER_RPC_AUTH_TOKEN_ENV, process_auth_token};
use crate::auth_provider_client::IpcServiceLaunchConfig;
use crate::auth_provider_protocol::ResolvedCredentialsDto;
use crate::envelope::{
    ProtocolVersion, RpcApiFamily, SECRET_PROVIDER_RPC_API_CONTRACT,
    negotiate_highest_mutual_version, secret_provider_rpc_supported_versions,
};
use crate::framing;

const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 5_000;
const MIN_STARTUP_TIMEOUT_MS: u64 = 1;
const MAX_PROVIDER_MESSAGE_CHARS: usize = 512;
const RPC_SERVICE: &str = "rpc";
const GET_SECRET_OPERATION: &str = "GetSecret";
const SOURCE_LOST_HINT: &str = "Unlock or restart the secret provider, then reconnect";

/// Failure codes meaning the provider's source was disconnected or locked again; callers
/// invalidate cached values of that provider when they see one of them.
pub const SOURCE_LOST_CODES: [&str; 3] = ["Locked", "Unavailable", "Transport"];
pub const TRANSPORT_CODE: &str = "Transport";
pub const TIMEOUT_CODE: &str = "Timeout";
pub const PROTOCOL_CODE: &str = "Protocol";
pub const MISSING_CREDENTIALS_CODE: &str = "MissingCredentials";

pub fn is_source_lost_code(code: &str) -> bool {
    SOURCE_LOST_CODES.contains(&code)
}

/// Classifies a `DbError` returned by an RPC secret provider as a lost source.
pub fn is_source_lost_error(error: &DbError) -> bool {
    let DbError::ValueResolutionFailed(message) = error else {
        return false;
    };

    SOURCE_LOST_CODES
        .iter()
        .any(|code| message.contains(&format!("/{RPC_SERVICE}] {GET_SECRET_OPERATION} ({code}):")))
}

pub fn negotiate_secret_provider_version(
    remote_versions: &[ProtocolVersion],
) -> Option<ProtocolVersion> {
    negotiate_highest_mutual_version(
        RpcApiFamily::SecretProviderRpc,
        secret_provider_rpc_supported_versions(),
        remote_versions,
    )
}

pub struct RpcSecretProvider {
    socket_id: String,
    launch: Option<IpcServiceLaunchConfig>,
    selected_version: ProtocolVersion,
    manifest: ValidatedManifest,
    next_request_id: AtomicU64,
}

/// Result of one blocking exchange; the secret value is wrapped before it leaves the worker.
enum ExchangeOutcome {
    Secret(SecretString),
    ProviderError(SecretProviderRpcError),
    Protocol(String),
    Transport(String),
}

impl RpcSecretProvider {
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
            Some(program) => program.to_string(),
            None if args.is_empty() => return Ok(None),
            None => {
                return Err(DbError::connection_failed(format!(
                    "Managed secret-provider service '{}' must set an explicit command; default driver-host launch is driver-only",
                    socket_id
                )));
            }
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

    /// Performs the Hello handshake and validates the version and manifest.
    ///
    /// Blocking: call it from a background thread.
    #[allow(clippy::result_large_err)]
    pub fn probe(
        socket_id: &str,
        launch: Option<IpcServiceLaunchConfig>,
        reserved_ids: &[&str],
    ) -> Result<Self, DbError> {
        validate_socket_id(socket_id)?;
        ensure_host_running_for(socket_id, launch.as_ref())?;

        let mut stream = connect(socket_id)?;
        let request = SecretProviderRequestEnvelope::new(
            SECRET_PROVIDER_RPC_API_CONTRACT.version,
            0,
            SecretProviderRequestBody::Hello(SecretProviderHelloRequest {
                client_name: "dbflux_ipc".to_string(),
                client_version: env!("CARGO_PKG_VERSION").to_string(),
                supported_versions: secret_provider_rpc_supported_versions().to_vec(),
                auth_token: hello_auth_token(),
            }),
        );

        framing::send_msg(&mut stream, &request)?;
        let response: SecretProviderResponseEnvelope = framing::recv_msg(&mut stream)?;

        if response.request_id != request.request_id {
            return Err(DbError::connection_failed(format!(
                "Request ID mismatch: sent {}, got {}",
                request.request_id, response.request_id
            )));
        }

        let hello = match response.body {
            SecretProviderResponseBody::Hello(hello) => hello,
            SecretProviderResponseBody::Error(error) => {
                return Err(DbError::connection_failed(format!(
                    "Secret provider '{}' rejected Hello ({}): {}",
                    socket_id,
                    error.code.as_str(),
                    sanitize_provider_message(&error.message)
                )));
            }
            SecretProviderResponseBody::Secret { .. } => {
                return Err(DbError::connection_failed(
                    "Unexpected response to secret-provider Hello".to_string(),
                ));
            }
        };

        validate_selected_version(hello.selected_version)?;

        let manifest = validate_manifest(&hello.manifest, reserved_ids).map_err(|error| {
            DbError::connection_failed(format!(
                "Secret provider '{}' has an invalid manifest: {}",
                socket_id, error
            ))
        })?;

        Ok(Self {
            socket_id: socket_id.to_string(),
            launch,
            selected_version: hello.selected_version,
            manifest,
            next_request_id: AtomicU64::new(1),
        })
    }

    #[cfg(test)]
    pub(super) fn with_socket_id_for_test(&self, socket_id: &str) -> Self {
        Self {
            socket_id: socket_id.to_string(),
            launch: None,
            selected_version: self.selected_version,
            manifest: self.manifest.clone(),
            next_request_id: AtomicU64::new(1),
        }
    }

    pub fn socket_id(&self) -> &str {
        &self.socket_id
    }

    pub fn manifest(&self) -> &ValidatedManifest {
        &self.manifest
    }

    pub fn selected_version(&self) -> ProtocolVersion {
        self.selected_version
    }

    pub async fn get_secret_with_credentials(
        &self,
        locator: &str,
        json_key: Option<&str>,
        credentials: Option<&ResolvedCredentials>,
    ) -> Result<SecretString, DbError> {
        let correlation_id = uuid::Uuid::now_v7().to_string();
        let request =
            self.build_get_secret_request(locator, json_key, credentials, &correlation_id)?;

        let outcome = self
            .execute(SecretProviderRequestBody::GetSecret(request))
            .await;

        let failure = match outcome {
            Ok(ExchangeOutcome::Secret(value)) => return Ok(value),
            Ok(ExchangeOutcome::ProviderError(error)) => {
                self.provider_failure(&error, &correlation_id)
            }
            Ok(ExchangeOutcome::Protocol(message)) => {
                self.failure(PROTOCOL_CODE, message, None, false, &correlation_id)
            }
            Ok(ExchangeOutcome::Transport(message)) => self.failure(
                TRANSPORT_CODE,
                message,
                Some(SOURCE_LOST_HINT),
                true,
                &correlation_id,
            ),
            Err(elapsed) => self.failure(
                TIMEOUT_CODE,
                format!("no response within {} ms", elapsed.as_millis()),
                None,
                true,
                &correlation_id,
            ),
        };

        log::warn!(
            "Secret provider '{}' failed {} with code {} [correlation id: {}]",
            self.manifest.provider_id,
            GET_SECRET_OPERATION,
            failure.code,
            correlation_id
        );

        Err(DbError::from(failure))
    }

    #[allow(clippy::result_large_err)]
    fn build_get_secret_request(
        &self,
        locator: &str,
        json_key: Option<&str>,
        credentials: Option<&ResolvedCredentials>,
        correlation_id: &str,
    ) -> Result<GetSecretRequest, DbError> {
        if json_key.is_some() && !self.manifest.capabilities.json_key {
            return Err(DbError::from(self.failure(
                SecretProviderRpcErrorCode::InvalidRequest.as_str(),
                "provider does not support JSON keys".to_string(),
                None,
                false,
                correlation_id,
            )));
        }

        let credentials = match self.manifest.auth_profile_dependency {
            AuthProfileDependency::None => None,
            AuthProfileDependency::Optional | AuthProfileDependency::Required => {
                credentials.map(ResolvedCredentialsDto::from)
            }
        };

        if self.manifest.auth_profile_dependency == AuthProfileDependency::Required
            && credentials.is_none()
        {
            return Err(DbError::from(self.failure(
                MISSING_CREDENTIALS_CODE,
                "provider requires an auth profile on the connection".to_string(),
                Some("Select an auth profile for this connection"),
                false,
                correlation_id,
            )));
        }

        Ok(GetSecretRequest {
            locator: locator.to_string(),
            json_key: json_key.map(str::to_string),
            credentials,
        })
    }

    /// Runs the blocking exchange on its own thread so a dropped future abandons the request
    /// instead of blocking; the abandoned worker finishes when the provider answers or closes.
    ///
    /// The deadline is a second thread racing the worker on the same channel rather than a
    /// tokio timer: the GUI drives the connect pipeline on the GPUI executor, where no tokio
    /// runtime exists and `tokio::time` would panic.
    async fn execute(&self, body: SecretProviderRequestBody) -> Result<ExchangeOutcome, Duration> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Option<ExchangeOutcome>>(2);
        let timer_sender = sender.clone();
        let socket_id = self.socket_id.clone();
        let launch = self.launch.clone();
        let envelope = SecretProviderRequestEnvelope::new(
            self.selected_version,
            self.next_request_id.fetch_add(1, Ordering::Relaxed),
            body,
        );

        let spawned = thread::Builder::new()
            .name(format!("dbflux-secret-provider-{socket_id}"))
            .spawn(move || {
                let outcome = exchange(&socket_id, launch.as_ref(), &envelope);
                if sender.try_send(Some(outcome)).is_err() {
                    log::debug!("Secret-provider request on '{socket_id}' was abandoned");
                }
            });

        if let Err(error) = spawned {
            return Ok(ExchangeOutcome::Transport(format!(
                "could not start the request worker: {error}"
            )));
        }

        let timeout = self.manifest.request_timeout;
        let timer = thread::Builder::new()
            .name("dbflux-secret-provider-deadline".to_string())
            .spawn(move || {
                thread::sleep(timeout);
                if timer_sender.try_send(None).is_err() {
                    log::trace!("Secret-provider request finished before its deadline");
                }
            });

        if let Err(error) = timer {
            return Ok(ExchangeOutcome::Transport(format!(
                "could not start the request deadline: {error}"
            )));
        }

        match receiver.recv().await {
            Some(Some(outcome)) => Ok(outcome),
            Some(None) => Err(timeout),
            None => Ok(ExchangeOutcome::Transport(
                "request worker stopped without a result".to_string(),
            )),
        }
    }

    fn provider_failure(
        &self,
        error: &SecretProviderRpcError,
        correlation_id: &str,
    ) -> ProviderError {
        let hint = matches!(
            error.code,
            SecretProviderRpcErrorCode::Locked | SecretProviderRpcErrorCode::Unavailable
        )
        .then_some(SOURCE_LOST_HINT);

        self.failure(
            error.code.as_str(),
            sanitize_provider_message(&error.message),
            hint,
            error.retriable,
            correlation_id,
        )
    }

    fn failure(
        &self,
        code: &str,
        message: String,
        recovery_hint: Option<&str>,
        retriable: bool,
        correlation_id: &str,
    ) -> ProviderError {
        ProviderError {
            provider: self.manifest.provider_id.clone(),
            service: RPC_SERVICE.to_string(),
            operation: GET_SECRET_OPERATION.to_string(),
            code: code.to_string(),
            message,
            recovery_hint: recovery_hint.map(str::to_string),
            retriable,
            correlation_id: Some(correlation_id.to_string()),
        }
    }
}

#[async_trait::async_trait]
impl DynSecretProvider for RpcSecretProvider {
    fn provider_id(&self) -> &str {
        &self.manifest.provider_id
    }

    fn display_name(&self) -> &str {
        &self.manifest.display_name
    }

    fn cache_ttl(&self) -> Option<Duration> {
        self.manifest.cache_ttl
    }

    async fn get_secret(
        &self,
        locator: &str,
        json_key: Option<&str>,
    ) -> Result<SecretString, DbError> {
        self.get_secret_with_credentials(locator, json_key, None)
            .await
    }

    async fn get_secret_in_context(
        &self,
        locator: &str,
        json_key: Option<&str>,
        context: &ResolveContext<'_>,
    ) -> Result<SecretString, DbError> {
        self.get_secret_with_credentials(locator, json_key, context.credentials)
            .await
    }
}

fn hello_auth_token() -> Option<String> {
    process_auth_token()
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .or_else(|| {
            std::env::var(SECRET_PROVIDER_RPC_AUTH_TOKEN_ENV)
                .ok()
                .filter(|token| !token.is_empty())
        })
}

#[allow(clippy::result_large_err)]
fn validate_selected_version(selected_version: ProtocolVersion) -> Result<(), DbError> {
    if secret_provider_rpc_supported_versions().contains(&selected_version) {
        return Ok(());
    }

    Err(DbError::connection_failed(format!(
        "Secret provider selected unsupported protocol version {}.{}",
        selected_version.major, selected_version.minor
    )))
}

fn exchange(
    socket_id: &str,
    launch: Option<&IpcServiceLaunchConfig>,
    envelope: &SecretProviderRequestEnvelope,
) -> ExchangeOutcome {
    if let Err(error) = ensure_host_running_for(socket_id, launch) {
        return ExchangeOutcome::Transport(error.to_string());
    }

    let mut stream = match connect(socket_id) {
        Ok(stream) => stream,
        Err(error) => return ExchangeOutcome::Transport(error.to_string()),
    };

    if let Err(error) = framing::send_msg(&mut stream, envelope) {
        return ExchangeOutcome::Transport(error.to_string());
    }

    let response: SecretProviderResponseEnvelope = match framing::recv_msg(&mut stream) {
        Ok(response) => response,
        Err(error) => return ExchangeOutcome::Transport(error.to_string()),
    };

    if response.request_id != envelope.request_id {
        return ExchangeOutcome::Protocol(format!(
            "request id mismatch: sent {}, got {}",
            envelope.request_id, response.request_id
        ));
    }

    match response.body {
        SecretProviderResponseBody::Secret { value } => {
            ExchangeOutcome::Secret(SecretString::from(value))
        }
        SecretProviderResponseBody::Error(error) => ExchangeOutcome::ProviderError(error),
        SecretProviderResponseBody::Hello(_) => {
            ExchangeOutcome::Protocol("unexpected Hello response to GetSecret".to_string())
        }
    }
}

/// Strips control characters and bounds the length of a provider-supplied message.
pub(super) fn sanitize_provider_message(message: &str) -> String {
    message
        .chars()
        .filter(|character| !character.is_control())
        .take(MAX_PROVIDER_MESSAGE_CHARS)
        .collect()
}
