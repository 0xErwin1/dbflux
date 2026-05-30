use std::io;
use std::thread;

use dbflux_core::{
    DatabaseCategory, DbKind, DriverFormDef, DriverMetadataBuilder, QueryLanguage,
};
use dbflux_ipc::{
    DRIVER_RPC_VERSION, driver_rpc_supported_versions, driver_socket_name, framing,
    driver_protocol::{
        DriverCapability, DriverHelloResponse, DriverRequestEnvelope,
        DriverResponseBody, DriverResponseEnvelope,
    },
};
use dbflux_ipc::audit::AuditEventEmitDto;
use interprocess::local_socket::{ListenerNonblockingMode::Neither, ListenerOptions};

/// Script of actions for the fake driver server to perform on each connection.
#[derive(Clone, Debug)]
pub enum FakeDriverAction {
    /// Reply with a pong.
    Pong,
    /// Emit an audit event (intermediate, `done=false`) then pong.
    EmitAuditThenPong(AuditEventEmitDto),
}

#[derive(Clone, Debug)]
pub struct FakeDriverRpcConfig {
    pub socket_id: String,
    /// Whether to advertise `DriverCapability::AuditEmit` in the hello.
    pub audit_emit_capability: bool,
    /// Actions to execute for each incoming request after hello.
    pub actions: Vec<FakeDriverAction>,
    /// Number of full connections (hello + actions) to serve before stopping.
    pub expected_connections: usize,
}

impl FakeDriverRpcConfig {
    pub fn new(socket_id: impl Into<String>) -> Self {
        Self {
            socket_id: socket_id.into(),
            audit_emit_capability: false,
            actions: vec![FakeDriverAction::Pong],
            expected_connections: 1,
        }
    }

    pub fn with_audit_emit_capability(mut self) -> Self {
        self.audit_emit_capability = true;
        self
    }

    pub fn with_actions(mut self, actions: Vec<FakeDriverAction>) -> Self {
        self.actions = actions;
        self
    }

    pub fn with_expected_connections(mut self, n: usize) -> Self {
        self.expected_connections = n;
        self
    }
}

pub struct FakeDriverRpcServer {
    join_handle: Option<thread::JoinHandle<io::Result<()>>>,
}

impl FakeDriverRpcServer {
    pub fn start(config: FakeDriverRpcConfig) -> io::Result<Self> {
        let socket_name = driver_socket_name(&config.socket_id)?;
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
            .map_err(|_| io::Error::other("fake driver server thread panicked"))?
    }
}

impl Drop for FakeDriverRpcServer {
    fn drop(&mut self) {
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }
}

fn run_server(
    listener: impl interprocess::local_socket::traits::Listener,
    config: FakeDriverRpcConfig,
) -> io::Result<()> {
    for _ in 0..config.expected_connections {
        let mut stream = listener.accept()?;

        let hello_req: DriverRequestEnvelope = framing::recv_msg(&mut stream)?;
        let hello_response = build_hello_response(&config, hello_req.request_id);
        framing::send_msg(&mut stream, &hello_response)?;

        for action in &config.actions {
            let request: DriverRequestEnvelope = framing::recv_msg(&mut stream)?;

            match action {
                FakeDriverAction::Pong => {
                    let pong = DriverResponseEnvelope::ok(
                        DRIVER_RPC_VERSION,
                        request.request_id,
                        request.session_id,
                        DriverResponseBody::Pong,
                    );
                    framing::send_msg(&mut stream, &pong)?;
                }

                FakeDriverAction::EmitAuditThenPong(dto) => {
                    let audit_frame = DriverResponseEnvelope {
                        protocol_version: DRIVER_RPC_VERSION,
                        request_id: request.request_id,
                        session_id: request.session_id,
                        done: false,
                        body: DriverResponseBody::EmitAuditEvent(dto.clone()),
                    };
                    framing::send_msg(&mut stream, &audit_frame)?;

                    let pong = DriverResponseEnvelope::ok(
                        DRIVER_RPC_VERSION,
                        request.request_id,
                        request.session_id,
                        DriverResponseBody::Pong,
                    );
                    framing::send_msg(&mut stream, &pong)?;
                }
            }
        }
    }

    Ok(())
}

fn build_hello_response(config: &FakeDriverRpcConfig, request_id: u64) -> DriverResponseEnvelope {
    let metadata = DriverMetadataBuilder::new(
        "fake-rpc",
        "Fake RPC Driver",
        DatabaseCategory::Relational,
        QueryLanguage::Sql,
    )
    .build();

    let mut capabilities = vec![DriverCapability::Cancellation];
    if config.audit_emit_capability {
        capabilities.push(DriverCapability::AuditEmit);
    }

    let hello = DriverHelloResponse {
        server_name: "fake-rpc-host".to_string(),
        server_version: "0.0.1".to_string(),
        selected_version: negotiate_version(),
        capabilities,
        driver_kind: DbKind::SQLite,
        driver_metadata: metadata,
        form_definition: DriverFormDef { tabs: vec![] },
        settings_schema: None,
    };

    DriverResponseEnvelope::ok(
        DRIVER_RPC_VERSION,
        request_id,
        None,
        DriverResponseBody::Hello(hello),
    )
}

fn negotiate_version() -> dbflux_ipc::ProtocolVersion {
    driver_rpc_supported_versions()
        .iter()
        .copied()
        .max_by_key(|v| (v.major, v.minor))
        .unwrap_or(DRIVER_RPC_VERSION)
}
