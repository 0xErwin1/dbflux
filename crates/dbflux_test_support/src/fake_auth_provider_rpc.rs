use std::collections::VecDeque;
use std::io;
use std::thread;

use dbflux_core::auth::{AuthFormDef, AuthProviderCapabilities, AuthSessionState};
use dbflux_ipc::auth_provider_protocol::{
    AuthProviderHelloResponse, AuthProviderHelloResponseV1_1, AuthProviderRequestBody,
    AuthProviderRequestEnvelope, AuthProviderResponseBody, AuthProviderResponseEnvelope,
    AuthProviderRpcError, AuthProviderRpcErrorCode, AuthSessionDto, FetchFieldOptionsResponse,
    ResolvedCredentialsDto, parse_auth_profile,
};
use dbflux_ipc::{
    AUTH_PROVIDER_RPC_API_CONTRACT, AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV, ProtocolVersion,
    auth_provider_rpc_supported_versions, auth_provider_socket_name, framing,
    negotiate_auth_provider_version,
};
use interprocess::local_socket::{ListenerNonblockingMode::Neither, ListenerOptions};

#[derive(Clone, Debug)]
pub enum FakeAuthRpcResult<T> {
    Ok(T),
    Err(AuthProviderRpcError),
}

#[derive(Clone, Debug)]
pub struct FakeAuthProviderRpcConfig {
    pub socket_id: String,
    pub provider_id: String,
    pub display_name: String,
    pub form_definition: AuthFormDef,
    pub capabilities: AuthProviderCapabilities,
    pub supported_versions: Vec<ProtocolVersion>,
    pub expected_connections: usize,
    pub validate_session: FakeAuthRpcResult<AuthSessionState>,
    pub validate_session_sequence: Vec<FakeAuthRpcResult<AuthSessionState>>,
    pub login_progress: Option<Option<String>>,
    pub login: FakeAuthRpcResult<AuthSessionDto>,
    pub resolve_credentials: FakeAuthRpcResult<ResolvedCredentialsDto>,
    pub fetch_dynamic_options: FakeAuthRpcResult<FetchFieldOptionsResponse>,
    pub expected_auth_token: Option<String>,
}

impl FakeAuthProviderRpcConfig {
    pub fn new(socket_id: impl Into<String>, provider_id: impl Into<String>) -> Self {
        Self {
            socket_id: socket_id.into(),
            provider_id: provider_id.into(),
            display_name: "Fake RPC Auth Provider".to_string(),
            form_definition: AuthFormDef { tabs: vec![] },
            capabilities: AuthProviderCapabilities::default(),
            supported_versions: auth_provider_rpc_supported_versions().to_vec(),
            expected_connections: 1,
            validate_session: FakeAuthRpcResult::Ok(AuthSessionState::LoginRequired),
            validate_session_sequence: Vec::new(),
            login_progress: None,
            login: FakeAuthRpcResult::Err(AuthProviderRpcError {
                code: AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "login not configured".to_string(),
                retriable: false,
            }),
            resolve_credentials: FakeAuthRpcResult::Err(AuthProviderRpcError {
                code: AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "resolve_credentials not configured".to_string(),
                retriable: false,
            }),
            fetch_dynamic_options: FakeAuthRpcResult::Err(AuthProviderRpcError {
                code: AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "fetch_dynamic_options not configured".to_string(),
                retriable: false,
            }),
            expected_auth_token: std::env::var(AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV)
                .ok()
                .filter(|token| !token.is_empty()),
        }
    }
}

pub struct FakeAuthProviderRpcServer {
    join_handle: Option<thread::JoinHandle<io::Result<()>>>,
}

impl FakeAuthProviderRpcServer {
    pub fn start(config: FakeAuthProviderRpcConfig) -> io::Result<Self> {
        let socket_name = auth_provider_socket_name(&config.socket_id)?;
        let listener = ListenerOptions::new()
            .name(socket_name.borrow())
            .nonblocking(Neither)
            .create_sync()?;

        let join_handle = thread::spawn(move || run_server(listener, config));

        Ok(Self {
            join_handle: Some(join_handle),
        })
    }

    pub fn wait(mut self) -> io::Result<()> {
        let Some(join_handle) = self.join_handle.take() else {
            return Ok(());
        };

        join_handle
            .join()
            .map_err(|_| io::Error::other("fake auth-provider server thread panicked"))?
    }
}

fn run_server(
    listener: impl interprocess::local_socket::traits::Listener,
    config: FakeAuthProviderRpcConfig,
) -> io::Result<()> {
    let mut handled_connections = 0;
    let mut validate_session_sequence = VecDeque::from(config.validate_session_sequence.clone());

    while handled_connections < config.expected_connections {
        let mut stream = listener.accept()?;
        let request: AuthProviderRequestEnvelope = match framing::recv_msg(&mut stream) {
            Ok(request) => request,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::BrokenPipe
                ) =>
            {
                continue;
            }
            Err(error) => return Err(error),
        };

        handled_connections += 1;

        let response = handle_request(&config, &mut validate_session_sequence, request);
        match response {
            FakeResponse::Single(envelope) => framing::send_msg(&mut stream, &envelope)?,
            FakeResponse::Streaming(progress, terminal) => {
                framing::send_msg(&mut stream, &progress)?;
                framing::send_msg(&mut stream, &terminal)?;
            }
        }
    }

    Ok(())
}

enum FakeResponse {
    Single(AuthProviderResponseEnvelope),
    Streaming(AuthProviderResponseEnvelope, AuthProviderResponseEnvelope),
}

fn handle_request(
    config: &FakeAuthProviderRpcConfig,
    validate_session_sequence: &mut VecDeque<FakeAuthRpcResult<AuthSessionState>>,
    request: AuthProviderRequestEnvelope,
) -> FakeResponse {
    if request.protocol_version.major != AUTH_PROVIDER_RPC_API_CONTRACT.version.major {
        return FakeResponse::Single(AuthProviderResponseEnvelope::error(
            request.protocol_version,
            request.request_id,
            AuthProviderRpcErrorCode::VersionMismatch,
            format!(
                "No compatible protocol version. Server: {}.{}",
                AUTH_PROVIDER_RPC_API_CONTRACT.version.major,
                AUTH_PROVIDER_RPC_API_CONTRACT.version.minor
            ),
            false,
        ));
    }

    match request.body {
        AuthProviderRequestBody::Hello(hello) => {
            if config.expected_auth_token.as_deref() != hello.auth_token.as_deref() {
                return FakeResponse::Single(AuthProviderResponseEnvelope::error(
                    request.protocol_version,
                    request.request_id,
                    AuthProviderRpcErrorCode::Transport,
                    "unauthorized auth-provider request",
                    false,
                ));
            }

            let Some(selected_version) =
                negotiate_auth_provider_version(&config.supported_versions)
            else {
                return FakeResponse::Single(AuthProviderResponseEnvelope::error(
                    request.protocol_version,
                    request.request_id,
                    AuthProviderRpcErrorCode::VersionMismatch,
                    format!(
                        "No compatible protocol version. Server: {}.{}",
                        AUTH_PROVIDER_RPC_API_CONTRACT.version.major,
                        AUTH_PROVIDER_RPC_API_CONTRACT.version.minor
                    ),
                    false,
                ));
            };

            let body = if selected_version.minor >= 1 {
                AuthProviderResponseBody::HelloV1_1(AuthProviderHelloResponseV1_1 {
                    server_name: "fake-auth-provider".to_string(),
                    server_version: "0.0.0-test".to_string(),
                    selected_version,
                    provider_id: config.provider_id.clone(),
                    display_name: config.display_name.clone(),
                    form_definition: config.form_definition.clone(),
                    capabilities: config.capabilities,
                })
            } else {
                AuthProviderResponseBody::Hello(AuthProviderHelloResponse {
                    server_name: "fake-auth-provider".to_string(),
                    server_version: "0.0.0-test".to_string(),
                    selected_version,
                    provider_id: config.provider_id.clone(),
                    display_name: config.display_name.clone(),
                    form_definition: config.form_definition.clone(),
                })
            };

            FakeResponse::Single(AuthProviderResponseEnvelope::ok(
                selected_version,
                request.request_id,
                body,
            ))
        }
        AuthProviderRequestBody::ValidateSession(validate) => {
            let _ = parse_auth_profile(&validate.profile_json);

            let session_state = validate_session_sequence
                .pop_front()
                .unwrap_or_else(|| config.validate_session.clone());

            match session_state {
                FakeAuthRpcResult::Ok(state) => {
                    FakeResponse::Single(AuthProviderResponseEnvelope::ok(
                        request.protocol_version,
                        request.request_id,
                        AuthProviderResponseBody::SessionState {
                            state: state.into(),
                        },
                    ))
                }
                FakeAuthRpcResult::Err(error) => {
                    FakeResponse::Single(AuthProviderResponseEnvelope::ok(
                        request.protocol_version,
                        request.request_id,
                        AuthProviderResponseBody::Error(error),
                    ))
                }
            }
        }
        AuthProviderRequestBody::Login(login) => {
            let _ = parse_auth_profile(&login.profile_json);

            let terminal = match &config.login {
                FakeAuthRpcResult::Ok(session) => AuthProviderResponseEnvelope::ok(
                    request.protocol_version,
                    request.request_id,
                    AuthProviderResponseBody::LoginResult {
                        session: session.clone(),
                    },
                ),
                FakeAuthRpcResult::Err(error) => AuthProviderResponseEnvelope::ok(
                    request.protocol_version,
                    request.request_id,
                    AuthProviderResponseBody::Error(error.clone()),
                ),
            };

            if let Some(progress_url) = &config.login_progress {
                FakeResponse::Streaming(
                    AuthProviderResponseEnvelope::login_url_progress(
                        request.protocol_version,
                        request.request_id,
                        progress_url.clone(),
                    ),
                    terminal,
                )
            } else {
                FakeResponse::Single(terminal)
            }
        }
        AuthProviderRequestBody::ResolveCredentials(resolve) => {
            let _ = parse_auth_profile(&resolve.profile_json);

            match &config.resolve_credentials {
                FakeAuthRpcResult::Ok(credentials) => {
                    FakeResponse::Single(AuthProviderResponseEnvelope::ok(
                        request.protocol_version,
                        request.request_id,
                        AuthProviderResponseBody::Credentials {
                            credentials: credentials.clone(),
                        },
                    ))
                }
                FakeAuthRpcResult::Err(error) => {
                    FakeResponse::Single(AuthProviderResponseEnvelope::ok(
                        request.protocol_version,
                        request.request_id,
                        AuthProviderResponseBody::Error(error.clone()),
                    ))
                }
            }
        }
        AuthProviderRequestBody::FetchDynamicOptions(fetch) => {
            let _ = parse_auth_profile(&fetch.profile_json);

            match &config.fetch_dynamic_options {
                FakeAuthRpcResult::Ok(response) => {
                    FakeResponse::Single(AuthProviderResponseEnvelope::ok(
                        request.protocol_version,
                        request.request_id,
                        AuthProviderResponseBody::DynamicOptions(response.clone()),
                    ))
                }
                FakeAuthRpcResult::Err(error) => {
                    FakeResponse::Single(AuthProviderResponseEnvelope::ok(
                        request.protocol_version,
                        request.request_id,
                        AuthProviderResponseBody::Error(error.clone()),
                    ))
                }
            }
        }
    }
}
