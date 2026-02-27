mod session;

use std::io;
use std::process;
use std::sync::Arc;

#[cfg(feature = "mysql")]
use dbflux_core::DbKind;
use dbflux_core::{ConnectionProfile, DbDriver};
use dbflux_ipc::driver_protocol::{
    DriverFormDefDto, DriverHelloResponse, DriverMetadataDto, DriverRequestBody,
    DriverRequestEnvelope, DriverResponseBody, DriverResponseEnvelope, DriverRpcErrorCode,
};
use dbflux_ipc::{DRIVER_RPC_VERSION, framing};
use interprocess::local_socket::{
    GenericNamespaced, ListenerNonblockingMode::Neither, ListenerOptions, prelude::*,
};
use session::SessionManager;
use uuid::Uuid;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = parse_args();

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
                handle_connection(stream, driver.as_ref());
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
fn handle_connection(mut stream: interprocess::local_socket::Stream, driver: &dyn DbDriver) {
    let mut sessions = SessionManager::new();
    let mut hello_done = false;

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

        let response = match envelope.body {
            DriverRequestBody::Hello(hello_req) => {
                hello_done = true;

                let compatible = hello_req
                    .supported_versions
                    .iter()
                    .any(|v| v.is_compatible_with(DRIVER_RPC_VERSION));

                if !compatible {
                    DriverResponseEnvelope::error(
                        request_id,
                        None,
                        DriverRpcErrorCode::VersionMismatch,
                        format!(
                            "No compatible protocol version. Server: {}.{}",
                            DRIVER_RPC_VERSION.major, DRIVER_RPC_VERSION.minor
                        ),
                        false,
                    )
                } else {
                    DriverResponseEnvelope::ok(
                        request_id,
                        None,
                        DriverResponseBody::Hello(DriverHelloResponse {
                            server_name: "dbflux-driver-host".to_string(),
                            server_version: env!("CARGO_PKG_VERSION").to_string(),
                            selected_version: DRIVER_RPC_VERSION,
                            capabilities: hello_req.requested_capabilities,
                            driver_kind: driver.kind(),
                            driver_metadata: DriverMetadataDto::from(driver.metadata()),
                            form_definition: DriverFormDefDto::from(driver.form_definition()),
                        }),
                    )
                }
            }

            DriverRequestBody::OpenSession {
                profile_json,
                password,
                ssh_secret,
            } => {
                if !hello_done {
                    DriverResponseEnvelope::error(
                        request_id,
                        None,
                        DriverRpcErrorCode::InvalidRequest,
                        "Hello handshake required before OpenSession",
                        false,
                    )
                } else {
                    handle_open_session(
                        request_id,
                        driver,
                        &mut sessions,
                        &profile_json,
                        password.as_deref(),
                        ssh_secret.as_deref(),
                    )
                }
            }

            DriverRequestBody::CloseSession => {
                if let Some(sid) = session_id {
                    match sessions.remove(&sid) {
                        Some(mut conn) => match conn.close() {
                            Ok(()) => DriverResponseEnvelope::ok(
                                request_id,
                                Some(sid),
                                DriverResponseBody::SessionClosed,
                            ),
                            Err(e) => {
                                log::warn!("Error closing session {sid}: {e}");
                                DriverResponseEnvelope::error(
                                    request_id,
                                    Some(sid),
                                    DriverRpcErrorCode::Driver,
                                    format!("Failed to close session: {e}"),
                                    false,
                                )
                            }
                        },
                        None => DriverResponseEnvelope::error(
                            request_id,
                            Some(sid),
                            DriverRpcErrorCode::SessionNotFound,
                            format!("Session {sid} not found"),
                            false,
                        ),
                    }
                } else {
                    DriverResponseEnvelope::error(
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
                        DriverResponseEnvelope::ok(request_id, Some(sid), body)
                    } else {
                        DriverResponseEnvelope::error(
                            request_id,
                            Some(sid),
                            DriverRpcErrorCode::SessionNotFound,
                            format!("Session {sid} not found"),
                            false,
                        )
                    }
                } else {
                    DriverResponseEnvelope::error(
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
                request_id,
                None,
                DriverRpcErrorCode::InvalidRequest,
                format!("Invalid profile JSON: {e}"),
                false,
            );
        }
    };

    match driver.connect_with_secrets(&profile, password, ssh_secret) {
        Ok(conn) => {
            let session_id = Uuid::new_v4();
            let kind = conn.kind();
            let metadata_dto = DriverMetadataDto::from(conn.metadata());
            let schema_loading_strategy = conn.schema_loading_strategy();
            let schema_features = conn.schema_features();
            let code_gen_capabilities = conn.code_gen_capabilities();

            sessions.insert(session_id, conn);

            DriverResponseEnvelope::ok(
                request_id,
                Some(session_id),
                DriverResponseBody::SessionOpened {
                    session_id,
                    kind,
                    metadata: metadata_dto,
                    schema_loading_strategy,
                    schema_features,
                    code_gen_capabilities,
                },
            )
        }
        Err(e) => DriverResponseEnvelope::error(
            request_id,
            None,
            DriverRpcErrorCode::Driver,
            e.to_string(),
            false,
        ),
    }
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
                    "  --driver <name>  Driver to host (sqlite, postgres, mysql, mariadb, mongodb, redis)"
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

            if available.is_empty() {
                Err("No drivers compiled into this binary. Enable features: sqlite, postgres, mysql, mongodb, redis".to_string())
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
