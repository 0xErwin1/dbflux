//! Example custom auth provider for DBFlux.
//!
//! This is a minimal implementation of the DBFlux auth-provider RPC protocol.
//! It simulates a device-auth style provider with deterministic session and
//! credential responses for testing purposes.
//!
//! Usage:
//!   cargo run --bin custom-auth-provider -- --socket my-auth-provider.sock
//!
//! Register the binary from Settings → RPC Services in DBFlux as an
//! `Auth Provider` service. Then create an auth profile from
//! Settings → Auth Profiles.

use std::io;

use dbflux_core::auth::{
    AuthProfile, AuthProviderCapabilities, AuthProviderLoginCapabilities, AuthSession,
    AuthSessionState, ResolvedCredentials,
};
use dbflux_core::chrono::{self, Duration};
use dbflux_core::{
    AuthFormDef, DbError, FormFieldDef, FormFieldKind, FormSection, FormTab, SelectOption,
};
use dbflux_core::secrecy::SecretString;
use dbflux_ipc::auth::AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV;
use dbflux_ipc::auth_provider_protocol::{
    AuthProviderHelloResponse, AuthProviderHelloResponseV1_1, AuthProviderRequestBody,
    AuthProviderRequestEnvelope, AuthProviderResponseBody, AuthProviderResponseEnvelope,
    AuthProviderRpcErrorCode, LoginRequest, ResolveCredentialsRequest, ValidateSessionRequest,
    parse_auth_profile,
};
use dbflux_ipc::{
    AUTH_PROVIDER_RPC_API_CONTRACT, ProtocolVersion, RpcApiFamily,
    auth_provider_rpc_supported_versions, auth_provider_socket_name, framing,
    negotiate_highest_mutual_version,
};
use interprocess::local_socket::{ListenerOptions, Stream as IpcStream, traits::Listener};

const PROVIDER_ID: &str = "example-device-auth";
const PROVIDER_NAME: &str = "Example Device Auth";

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = parse_args();
    let socket_name = auth_provider_socket_name(&args.socket).unwrap_or_else(|error| {
        eprintln!("Invalid socket name: {error}");
        std::process::exit(1);
    });

    let listener = ListenerOptions::new()
        .name(socket_name.borrow())
        .create_sync()
        .unwrap_or_else(|error| {
            eprintln!("Failed to bind socket: {error}");
            std::process::exit(1);
        });

    log::info!("Custom auth provider listening on socket: {}", args.socket);
    log::info!("Press Ctrl+C to stop");

    loop {
        match listener.accept() {
            Ok(stream) => {
                log::info!("Client connected");

                std::thread::spawn(move || {
                    if let Err(error) = handle_connection(stream) {
                        log::warn!("Connection error: {error}");
                    }

                    log::info!("Client disconnected");
                });
            }
            Err(error) => {
                log::error!("Accept failed: {error}");
                break;
            }
        }
    }
}

fn handle_connection(mut stream: IpcStream) -> io::Result<()> {
    let mut negotiated_version = None;

    loop {
        let envelope: AuthProviderRequestEnvelope = match framing::recv_msg(&mut stream) {
            Ok(envelope) => envelope,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                log::debug!("Client closed connection");
                break;
            }
            Err(error) => {
                log::warn!("Failed to read request: {error}");
                break;
            }
        };

        let request_id = envelope.request_id;
        let request_version = envelope.protocol_version;

        if !matches!(envelope.body, AuthProviderRequestBody::Hello(_)) {
            let Some(selected_version) = negotiated_version else {
                let response = request_error(
                    request_version,
                    request_id,
                    "Hello required before auth-provider requests",
                );
                framing::send_msg(&mut stream, &response)?;
                continue;
            };

            if request_version != selected_version {
                let response = AuthProviderResponseEnvelope::error(
                    request_version,
                    request_id,
                    AuthProviderRpcErrorCode::VersionMismatch,
                    format!(
                        "Protocol version drift detected: negotiated {}.{}, received {}.{}",
                        selected_version.major,
                        selected_version.minor,
                        request_version.major,
                        request_version.minor
                    ),
                    false,
                );
                framing::send_msg(&mut stream, &response)?;
                continue;
            }
        }

        match envelope.body {
            AuthProviderRequestBody::Hello(hello) => {
                let expected_auth_token = std::env::var(AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV)
                    .ok()
                    .filter(|token| !token.is_empty());

                if expected_auth_token.as_deref() != hello.auth_token.as_deref() {
                    let response = AuthProviderResponseEnvelope::error(
                        request_version,
                        request_id,
                        AuthProviderRpcErrorCode::Transport,
                        "unauthorized auth-provider request",
                        false,
                    );
                    framing::send_msg(&mut stream, &response)?;
                    continue;
                }

                let Some(selected_version) = choose_negotiated_version(&hello.supported_versions)
                else {
                    let response = AuthProviderResponseEnvelope::error(
                        request_version,
                        request_id,
                        AuthProviderRpcErrorCode::VersionMismatch,
                        format!(
                            "No compatible protocol version. Server: {}.{}",
                            AUTH_PROVIDER_RPC_API_CONTRACT.version.major,
                            AUTH_PROVIDER_RPC_API_CONTRACT.version.minor
                        ),
                        false,
                    );
                    framing::send_msg(&mut stream, &response)?;
                    continue;
                };

                negotiated_version = Some(selected_version);

                let body = if selected_version.minor >= 1 {
                    AuthProviderResponseBody::HelloV1_1(AuthProviderHelloResponseV1_1 {
                        server_name: "custom-auth-provider".to_string(),
                        server_version: env!("CARGO_PKG_VERSION").to_string(),
                        selected_version,
                        provider_id: PROVIDER_ID.to_string(),
                        display_name: PROVIDER_NAME.to_string(),
                        form_definition: auth_form(),
                        capabilities: AuthProviderCapabilities {
                            login: AuthProviderLoginCapabilities {
                                supported: true,
                                verification_url_progress: true,
                            },
                        },
                    })
                } else {
                    AuthProviderResponseBody::Hello(AuthProviderHelloResponse {
                        server_name: "custom-auth-provider".to_string(),
                        server_version: env!("CARGO_PKG_VERSION").to_string(),
                        selected_version,
                        provider_id: PROVIDER_ID.to_string(),
                        display_name: PROVIDER_NAME.to_string(),
                        form_definition: auth_form(),
                    })
                };

                let response =
                    AuthProviderResponseEnvelope::ok(selected_version, request_id, body);

                framing::send_msg(&mut stream, &response)?;
            }
            AuthProviderRequestBody::ValidateSession(ValidateSessionRequest { profile_json }) => {
                let response = match parse_auth_profile(&profile_json) {
                    Ok(profile) => AuthProviderResponseEnvelope::ok(
                        negotiated_version.expect("validated before dispatch"),
                        request_id,
                        AuthProviderResponseBody::SessionState {
                            state: determine_session_state(&profile).into(),
                        },
                    ),
                    Err(error) => AuthProviderResponseEnvelope::ok(
                        negotiated_version.expect("validated before dispatch"),
                        request_id,
                        AuthProviderResponseBody::Error(error),
                    ),
                };

                framing::send_msg(&mut stream, &response)?;
            }
            AuthProviderRequestBody::Login(LoginRequest { profile_json }) => {
                match parse_auth_profile(&profile_json) {
                    Ok(profile) => {
                        let selected_version = negotiated_version.expect("validated before dispatch");

                        if let Some(verification_url) = profile
                            .fields
                            .get("verification_url")
                            .cloned()
                            .filter(|value| !value.trim().is_empty())
                        {
                            let progress = AuthProviderResponseEnvelope::login_url_progress(
                                selected_version,
                                request_id,
                                Some(verification_url),
                            );
                            framing::send_msg(&mut stream, &progress)?;
                        }

                        let response = AuthProviderResponseEnvelope::ok(
                            selected_version,
                            request_id,
                            AuthProviderResponseBody::LoginResult {
                                session: (&AuthSession::from_profile(&profile)).into(),
                            },
                        );
                        framing::send_msg(&mut stream, &response)?;
                    }
                    Err(error) => {
                        let response = AuthProviderResponseEnvelope::ok(
                            negotiated_version.expect("validated before dispatch"),
                            request_id,
                            AuthProviderResponseBody::Error(error),
                        );
                        framing::send_msg(&mut stream, &response)?;
                    }
                }
            }
            AuthProviderRequestBody::ResolveCredentials(ResolveCredentialsRequest { profile_json }) => {
                let response = match parse_auth_profile(&profile_json) {
                    Ok(profile) => match resolve_credentials(&profile) {
                        Ok(credentials) => AuthProviderResponseEnvelope::ok(
                            negotiated_version.expect("validated before dispatch"),
                            request_id,
                            AuthProviderResponseBody::Credentials {
                                credentials: (&credentials).into(),
                            },
                        ),
                        Err(error) => AuthProviderResponseEnvelope::error(
                            negotiated_version.expect("validated before dispatch"),
                            request_id,
                            AuthProviderRpcErrorCode::InvalidRequest,
                            error.to_string(),
                            false,
                        ),
                    },
                    Err(error) => AuthProviderResponseEnvelope::ok(
                        negotiated_version.expect("validated before dispatch"),
                        request_id,
                        AuthProviderResponseBody::Error(error),
                    ),
                };

                framing::send_msg(&mut stream, &response)?;
            }
        }
    }

    Ok(())
}

fn auth_form() -> AuthFormDef {
    AuthFormDef {
        tabs: vec![FormTab {
            id: "main".into(),
            label: "Main".into(),
            sections: vec![FormSection {
                title: "Example Device Auth".into(),
                fields: vec![
                    FormFieldDef {
                        id: "region".into(),
                        label: "Region".into(),
                        kind: FormFieldKind::Text,
                        placeholder: "us-east-1".into(),
                        required: true,
                        default_value: "us-east-1".into(),
                        enabled_when_checked: None,
                        enabled_when_unchecked: None,
                    },
                    FormFieldDef {
                        id: "access_key_id".into(),
                        label: "Access Key ID".into(),
                        kind: FormFieldKind::Text,
                        placeholder: "AKIAEXAMPLE".into(),
                        required: true,
                        default_value: "AKIAEXAMPLE".into(),
                        enabled_when_checked: None,
                        enabled_when_unchecked: None,
                    },
                    FormFieldDef {
                        id: "session_state".into(),
                        label: "Session State".into(),
                        kind: FormFieldKind::Select {
                            options: vec![
                                SelectOption::new("login_required", "Login Required"),
                                SelectOption::new("valid", "Valid"),
                                SelectOption::new("expired", "Expired"),
                            ],
                        },
                        placeholder: String::new(),
                        required: true,
                        default_value: "login_required".into(),
                        enabled_when_checked: None,
                        enabled_when_unchecked: None,
                    },
                    FormFieldDef {
                        id: "verification_url".into(),
                        label: "Verification URL".into(),
                        kind: FormFieldKind::Text,
                        placeholder: "https://verify.example/device".into(),
                        required: false,
                        default_value: "https://verify.example/device".into(),
                        enabled_when_checked: None,
                        enabled_when_unchecked: None,
                    },
                ],
            }],
        }],
    }
}

fn determine_session_state(profile: &AuthProfile) -> AuthSessionState {
    match profile.fields.get("session_state").map(String::as_str) {
        Some("valid") => AuthSessionState::Valid {
            expires_at: Some(chrono::Utc::now() + Duration::minutes(30)),
        },
        Some("expired") => AuthSessionState::Expired,
        _ => AuthSessionState::LoginRequired,
    }
}

fn resolve_credentials(profile: &AuthProfile) -> Result<ResolvedCredentials, DbError> {
    let access_key_id = profile
        .fields
        .get("access_key_id")
        .cloned()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| DbError::connection_failed("access_key_id is required"))?;

    let region = profile
        .fields
        .get("region")
        .cloned()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| DbError::connection_failed("region is required"))?;

    let mut credentials = ResolvedCredentials::default();
    credentials.fields.insert("region".to_string(), region);
    credentials
        .fields
        .insert("access_key_id".to_string(), access_key_id);
    credentials.secret_fields.insert(
        "session_token".to_string(),
        SecretString::from("example-session-token".to_string()),
    );

    Ok(credentials)
}

fn choose_negotiated_version(
    client_supported_versions: &[ProtocolVersion],
) -> Option<ProtocolVersion> {
    negotiate_highest_mutual_version(
        RpcApiFamily::AuthProviderRpc,
        auth_provider_rpc_supported_versions(),
        client_supported_versions,
    )
}

fn request_error(
    protocol_version: ProtocolVersion,
    request_id: u64,
    message: &str,
) -> AuthProviderResponseEnvelope {
    AuthProviderResponseEnvelope::error(
        protocol_version,
        request_id,
        AuthProviderRpcErrorCode::InvalidRequest,
        message.to_string(),
        false,
    )
}

struct Args {
    socket: String,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut socket = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket" => socket = args.next(),
            "--help" | "-h" => {
                println!("Custom Auth Provider Example for DBFlux");
                println!();
                println!("Usage: custom-auth-provider --socket <name>");
                println!();
                println!("Options:");
                println!("  --socket <name>  Socket name to bind (required)");
                println!("  --help, -h       Show this help");
                std::process::exit(0);
            }
            other => {
                eprintln!("Unknown argument: {other}");
                std::process::exit(1);
            }
        }
    }

    Args {
        socket: socket.unwrap_or_else(|| {
            eprintln!("Error: --socket is required");
            eprintln!("Use --help for usage information");
            std::process::exit(1);
        }),
    }
}

trait ExampleAuthSession {
    fn from_profile(profile: &AuthProfile) -> Self;
}

impl ExampleAuthSession for AuthSession {
    fn from_profile(profile: &AuthProfile) -> Self {
        Self {
            provider_id: PROVIDER_ID.to_string(),
            profile_id: profile.id,
            expires_at: Some(chrono::Utc::now() + Duration::minutes(30)),
            data: None,
        }
    }
}
