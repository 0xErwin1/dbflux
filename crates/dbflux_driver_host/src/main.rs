mod session;

use std::io;
use std::process;
use std::sync::Arc;

#[cfg(feature = "mysql")]
use dbflux_core::DbKind;
use dbflux_core::secrecy::SecretString;
use dbflux_core::{ConnectionProfile, DbDriver};
use dbflux_ipc::driver_protocol::{
    DriverHelloResponse, DriverRequestBody, DriverRequestEnvelope, DriverResponseBody,
    DriverResponseEnvelope, DriverRpcError, DriverRpcErrorCode,
};
use dbflux_ipc::{
    DRIVER_RPC_AUTH_TOKEN_ENV, DRIVER_RPC_VERSION, ProtocolVersion, driver_rpc_supported_versions,
    framing, negotiate_highest_mutual_version,
};
use interprocess::local_socket::{
    GenericNamespaced, ListenerNonblockingMode::Neither, ListenerOptions, prelude::*,
};
use session::SessionManager;
use uuid::Uuid;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = parse_args();
    let auth_token = std::env::var(DRIVER_RPC_AUTH_TOKEN_ENV)
        .ok()
        .filter(|token| !token.is_empty());

    let driver = create_driver(&args.driver)
        .unwrap_or_else(|e| fatal(&format!("Failed to create driver '{}': {e}", args.driver)));

    let socket_display = args.socket.clone();
    let name = args
        .socket
        .to_ns_name::<GenericNamespaced>()
        .unwrap_or_else(|e| fatal(&format!("Invalid socket name '{socket_display}': {e}")));

    let listener = ListenerOptions::new()
        .name(name)
        .nonblocking(Neither)
        .create_sync()
        .unwrap_or_else(|e| fatal(&format!("Failed to bind socket '{socket_display}': {e}")));

    log::info!(
        "Driver host started: driver={}, socket={socket_display}",
        args.driver,
    );

    // Accept loop — one connection at a time (the parent DBFlux process holds a
    // single connection per driver-host instance).
    loop {
        match listener.accept() {
            Ok(stream) => {
                log::info!("Client connected");
                handle_connection(stream, driver.as_ref(), auth_token.as_deref());
                log::info!("Client disconnected");
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => {
                log::error!("Accept failed: {e}");
                break;
            }
        }
    }

    log::info!("Driver host shutting down");
}

/// Handles one client connection for its entire lifetime.
fn handle_connection(
    mut stream: interprocess::local_socket::Stream,
    driver: &dyn DbDriver,
    expected_auth_token: Option<&str>,
) {
    let mut sessions = SessionManager::new();
    let mut negotiated_version = None;

    loop {
        let envelope: DriverRequestEnvelope = match framing::recv_msg(&mut stream) {
            Ok(env) => env,
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    log::debug!("Client closed connection");
                } else {
                    log::warn!("Failed to read request: {e}");
                }
                break;
            }
        };

        let request_id = envelope.request_id;
        let session_id = envelope.session_id;
        let request_version = envelope.protocol_version;

        if !matches!(envelope.body, DriverRequestBody::Hello(_)) {
            let Some(selected_version) = negotiated_version else {
                let response = DriverResponseEnvelope::error(
                    request_version,
                    request_id,
                    session_id,
                    DriverRpcErrorCode::InvalidRequest,
                    "Hello handshake required before OpenSession",
                    false,
                );

                if let Err(e) = framing::send_msg(&mut stream, &response) {
                    log::warn!("Failed to send response: {e}");
                    break;
                }

                continue;
            };

            if let Err(error) =
                validate_negotiated_request_version(selected_version, request_version)
            {
                let response = DriverResponseEnvelope::error(
                    request_version,
                    request_id,
                    session_id,
                    error.code,
                    error.message,
                    error.retriable,
                );

                if let Err(e) = framing::send_msg(&mut stream, &response) {
                    log::warn!("Failed to send response: {e}");
                    break;
                }

                continue;
            }
        }

        let response = match envelope.body {
            DriverRequestBody::Hello(hello_req) => {
                if let Some(expected_token) = expected_auth_token
                    && hello_req.auth_token.as_deref() != Some(expected_token)
                {
                    DriverResponseEnvelope::error(
                        request_version,
                        request_id,
                        None,
                        DriverRpcErrorCode::InvalidRequest,
                        "Unauthorized driver RPC client",
                        false,
                    )
                } else {
                    match negotiate_hello_version(&hello_req.supported_versions) {
                        Ok(selected_version) => {
                            negotiated_version = Some(selected_version);

                            DriverResponseEnvelope::ok(
                                selected_version,
                                request_id,
                                None,
                                DriverResponseBody::Hello(DriverHelloResponse {
                                    server_name: "dbflux-driver-host".to_string(),
                                    server_version: env!("CARGO_PKG_VERSION").to_string(),
                                    selected_version,
                                    capabilities: hello_req.requested_capabilities,
                                    driver_kind: driver.kind(),
                                    driver_metadata: driver.metadata().clone(),
                                    form_definition: driver.form_definition().clone(),
                                    settings_schema: driver
                                        .settings_schema()
                                        .map(|schema| schema.as_ref().clone()),
                                }),
                            )
                        }
                        Err(error) => {
                            hello_version_error_response(request_version, request_id, error)
                        }
                    }
                }
            }

            DriverRequestBody::OpenSession {
                profile_json,
                password,
                ssh_secret,
            } => handle_open_session(
                negotiated_version.expect("validated before dispatch"),
                request_id,
                driver,
                &mut sessions,
                &profile_json,
                password.as_deref(),
                ssh_secret.as_deref(),
            ),

            DriverRequestBody::CloseSession => {
                if let Some(sid) = session_id {
                    match sessions.remove(&sid) {
                        Some(mut conn) => match conn.close() {
                            Ok(()) => DriverResponseEnvelope::ok(
                                negotiated_version.expect("validated before dispatch"),
                                request_id,
                                Some(sid),
                                DriverResponseBody::SessionClosed,
                            ),
                            Err(e) => {
                                log::warn!("Error closing session {sid}: {e}");
                                DriverResponseEnvelope::error(
                                    negotiated_version.expect("validated before dispatch"),
                                    request_id,
                                    Some(sid),
                                    DriverRpcErrorCode::Driver,
                                    format!("Failed to close session: {e}"),
                                    false,
                                )
                            }
                        },
                        None => DriverResponseEnvelope::error(
                            negotiated_version.expect("validated before dispatch"),
                            request_id,
                            Some(sid),
                            DriverRpcErrorCode::SessionNotFound,
                            format!("Session {sid} not found"),
                            false,
                        ),
                    }
                } else {
                    DriverResponseEnvelope::error(
                        negotiated_version.expect("validated before dispatch"),
                        request_id,
                        None,
                        DriverRpcErrorCode::SessionNotFound,
                        "No session_id provided for CloseSession",
                        false,
                    )
                }
            }

            other => {
                if let Some(sid) = session_id {
                    if let Some(conn) = sessions.get(&sid) {
                        let body = session::dispatch(conn, other);
                        DriverResponseEnvelope::ok(
                            negotiated_version.expect("validated before dispatch"),
                            request_id,
                            Some(sid),
                            body,
                        )
                    } else {
                        DriverResponseEnvelope::error(
                            negotiated_version.expect("validated before dispatch"),
                            request_id,
                            Some(sid),
                            DriverRpcErrorCode::SessionNotFound,
                            format!("Session {sid} not found"),
                            false,
                        )
                    }
                } else {
                    DriverResponseEnvelope::error(
                        negotiated_version.expect("validated before dispatch"),
                        request_id,
                        None,
                        DriverRpcErrorCode::SessionNotFound,
                        "No session_id provided",
                        false,
                    )
                }
            }
        };

        if let Err(e) = framing::send_msg(&mut stream, &response) {
            log::warn!("Failed to send response: {e}");
            break;
        }
    }

    sessions.close_all();
}

fn handle_open_session(
    protocol_version: ProtocolVersion,
    request_id: u64,
    driver: &dyn DbDriver,
    sessions: &mut SessionManager,
    profile_json: &str,
    password: Option<&str>,
    ssh_secret: Option<&str>,
) -> DriverResponseEnvelope {
    let profile: ConnectionProfile = match serde_json::from_str(profile_json) {
        Ok(p) => p,
        Err(e) => {
            return DriverResponseEnvelope::error(
                protocol_version,
                request_id,
                None,
                DriverRpcErrorCode::InvalidRequest,
                format!("Invalid profile JSON: {e}"),
                false,
            );
        }
    };

    let password_secret = password.map(|value| SecretString::from(value.to_string()));
    let ssh_secret_secret = ssh_secret.map(|value| SecretString::from(value.to_string()));

    match driver.connect_with_secrets(
        &profile,
        password_secret.as_ref(),
        ssh_secret_secret.as_ref(),
    ) {
        Ok(conn) => {
            let session_id = Uuid::new_v4();
            let kind = conn.kind();
            let metadata = conn.metadata().clone();
            let schema_loading_strategy = conn.schema_loading_strategy();
            let schema_features = conn.schema_features();
            let code_gen_capabilities = conn.code_gen_capabilities();

            sessions.insert(session_id, conn);

            DriverResponseEnvelope::ok(
                protocol_version,
                request_id,
                Some(session_id),
                DriverResponseBody::SessionOpened {
                    session_id,
                    kind,
                    metadata,
                    schema_loading_strategy,
                    schema_features,
                    code_gen_capabilities,
                },
            )
        }
        Err(e) => DriverResponseEnvelope::error(
            protocol_version,
            request_id,
            None,
            DriverRpcErrorCode::Driver,
            e.to_string(),
            false,
        ),
    }
}

fn choose_negotiated_driver_version(
    client_supported_versions: &[ProtocolVersion],
) -> Option<ProtocolVersion> {
    negotiate_highest_mutual_version(
        dbflux_ipc::RpcApiFamily::DriverRpc,
        driver_rpc_supported_versions(),
        client_supported_versions,
    )
}

fn hello_version_error_response(
    request_version: ProtocolVersion,
    request_id: u64,
    error: DriverRpcError,
) -> DriverResponseEnvelope {
    DriverResponseEnvelope::error(
        request_version,
        request_id,
        None,
        error.code,
        error.message,
        error.retriable,
    )
}

fn negotiate_hello_version(
    client_supported_versions: &[ProtocolVersion],
) -> Result<ProtocolVersion, DriverRpcError> {
    choose_negotiated_driver_version(client_supported_versions).ok_or_else(|| DriverRpcError {
        code: DriverRpcErrorCode::VersionMismatch,
        message: format!(
            "No compatible protocol version. Server: {}.{}",
            DRIVER_RPC_VERSION.major, DRIVER_RPC_VERSION.minor
        ),
        retriable: false,
    })
}

fn validate_negotiated_request_version(
    negotiated_version: ProtocolVersion,
    request_version: ProtocolVersion,
) -> Result<(), DriverRpcError> {
    if negotiated_version != request_version {
        return Err(DriverRpcError {
            code: DriverRpcErrorCode::VersionMismatch,
            message: format!(
                "Protocol version drift detected: negotiated {}.{}, received {}.{}",
                negotiated_version.major,
                negotiated_version.minor,
                request_version.major,
                request_version.minor
            ),
            retriable: false,
        });
    }

    Ok(())
}

struct Args {
    driver: String,
    socket: String,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut driver = None;
    let mut socket = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--driver" => driver = args.next(),
            "--socket" => socket = args.next(),
            "--help" | "-h" => {
                eprintln!("Usage: dbflux-driver-host --driver <name> --socket <name>");
                eprintln!();
                eprintln!("Options:");
                eprintln!(
                    "  --driver <name>  Driver to host (sqlite, postgres, mysql, mariadb, mongodb, redis, dynamodb)"
                );
                eprintln!("  --socket <name>  Socket name to bind");
                process::exit(0);
            }
            other => fatal(&format!("Unknown argument: {other}")),
        }
    }

    Args {
        driver: driver.unwrap_or_else(|| fatal("--driver is required")),
        socket: socket.unwrap_or_else(|| fatal("--socket is required")),
    }
}

fn create_driver(name: &str) -> Result<Arc<dyn DbDriver>, String> {
    match name {
        #[cfg(feature = "sqlite")]
        "sqlite" => Ok(Arc::new(dbflux_driver_sqlite::SqliteDriver)),

        #[cfg(feature = "postgres")]
        "postgres" => Ok(Arc::new(dbflux_driver_postgres::PostgresDriver)),

        #[cfg(feature = "mysql")]
        "mysql" => Ok(Arc::new(dbflux_driver_mysql::MysqlDriver::new(
            DbKind::MySQL,
        ))),

        #[cfg(feature = "mysql")]
        "mariadb" => Ok(Arc::new(dbflux_driver_mysql::MysqlDriver::new(
            DbKind::MariaDB,
        ))),

        #[cfg(feature = "mongodb")]
        "mongodb" => Ok(Arc::new(dbflux_driver_mongodb::MongoDriver)),

        #[cfg(feature = "redis")]
        "redis" => Ok(Arc::new(dbflux_driver_redis::RedisDriver)),

        #[cfg(feature = "dynamodb")]
        "dynamodb" => Ok(Arc::new(dbflux_driver_dynamodb::DynamoDriver::new())),

        _ => {
            #[allow(unused_mut)]
            let mut available: Vec<&str> = Vec::new();
            #[cfg(feature = "sqlite")]
            available.push("sqlite");
            #[cfg(feature = "postgres")]
            available.push("postgres");
            #[cfg(feature = "mysql")]
            {
                available.push("mysql");
                available.push("mariadb");
            }
            #[cfg(feature = "mongodb")]
            available.push("mongodb");
            #[cfg(feature = "redis")]
            available.push("redis");
            #[cfg(feature = "dynamodb")]
            available.push("dynamodb");

            if available.is_empty() {
                Err("No drivers compiled into this binary. Enable features: sqlite, postgres, mysql, mongodb, redis, dynamodb".to_string())
            } else {
                Err(format!(
                    "Unknown driver '{name}'. Available: {}",
                    available.join(", ")
                ))
            }
        }
    }
}

fn fatal(message: &str) -> ! {
    eprintln!("Error: {message}");
    process::exit(1)
}

#[cfg(test)]
mod tests {
    use super::{
        choose_negotiated_driver_version, create_driver, hello_version_error_response,
        negotiate_hello_version, validate_negotiated_request_version,
    };
    use dbflux_ipc::{
        ProtocolVersion,
        driver_protocol::{DriverResponseBody, DriverRpcErrorCode},
    };

    #[cfg(feature = "dynamodb")]
    #[test]
    fn create_driver_returns_dynamodb_when_feature_enabled() {
        let driver = create_driver("dynamodb").expect("dynamodb driver should be registered");
        assert_eq!(driver.metadata().id, "dynamodb");
    }

    #[cfg(not(feature = "dynamodb"))]
    #[test]
    fn create_driver_rejects_dynamodb_when_feature_disabled() {
        let error = match create_driver("dynamodb") {
            Ok(_) => panic!("dynamodb should be unavailable"),
            Err(error) => error,
        };

        if let Some(available) = error.split("Available: ").nth(1) {
            assert!(!available.contains("dynamodb"));
        } else {
            assert!(
                error.contains("No drivers compiled into this binary"),
                "unexpected error when dynamodb feature is disabled: {error}"
            );
        }
    }

    #[test]
    fn choose_negotiated_driver_version_prefers_highest_mutual_minor() {
        let selected = choose_negotiated_driver_version(&[
            ProtocolVersion::new(1, 0),
            ProtocolVersion::new(1, 1),
        ]);

        assert_eq!(selected, Some(ProtocolVersion::new(1, 1)));
    }

    #[test]
    fn choose_negotiated_driver_version_returns_none_without_overlap() {
        let selected = choose_negotiated_driver_version(&[ProtocolVersion::new(2, 0)]);

        assert_eq!(selected, None);
    }

    #[test]
    fn validate_negotiated_request_version_rejects_post_hello_drift() {
        let error = validate_negotiated_request_version(
            ProtocolVersion::new(1, 0),
            ProtocolVersion::new(1, 1),
        )
        .expect_err("drifted protocol version should be rejected");

        assert_eq!(
            error.code,
            dbflux_ipc::driver_protocol::DriverRpcErrorCode::VersionMismatch
        );
        assert!(error.message.contains("1.0"));
        assert!(error.message.contains("1.1"));
    }

    #[test]
    fn negotiate_hello_version_returns_version_mismatch_response_without_overlap() {
        let error = negotiate_hello_version(&[ProtocolVersion::new(2, 0)])
            .expect_err("missing overlap should reject hello before opening a session");

        assert_eq!(error.code, DriverRpcErrorCode::VersionMismatch);
        assert!(error.message.contains("No compatible protocol version"));
    }

    #[test]
    fn hello_version_error_response_preserves_request_metadata() {
        let response = hello_version_error_response(
            ProtocolVersion::new(1, 0),
            17,
            dbflux_ipc::driver_protocol::DriverRpcError {
                code: DriverRpcErrorCode::VersionMismatch,
                message: "No compatible protocol version. Server: 1.1".to_string(),
                retriable: false,
            },
        );

        assert_eq!(response.protocol_version, ProtocolVersion::new(1, 0));
        assert_eq!(response.request_id, 17);

        match response.body {
            DriverResponseBody::Error(error) => {
                assert_eq!(error.code, DriverRpcErrorCode::VersionMismatch);
                assert!(error.message.contains("No compatible protocol version"));
            }
            other => panic!("expected version mismatch error, got {other:?}"),
        }
    }
}
