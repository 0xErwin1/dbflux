use std::io;
use std::sync::{Arc, Mutex};
use std::thread;

use dbflux_core::{
    DatabaseCategory, DbKind, DriverCapabilities, DriverFormDef, DriverMetadataBuilder,
    QueryLanguage,
};
use dbflux_ipc::audit::AuditEventEmitDto;
use dbflux_ipc::{
    ProtocolVersion,
    driver_protocol::{
        DriverCapability, DriverHelloResponse, DriverRequestBody, DriverRequestEnvelope,
        DriverResponseBody, DriverResponseEnvelope,
    },
    driver_socket_name, framing,
};

/// Protocol version the fake advertises in `Hello` and stamps on every frame,
/// unless a test overrides it with [`FakeDriverRpcConfig::with_protocol_version`].
///
/// Deliberately pinned to v1.3. `run_server` has no per-request dispatch: it
/// reads one request per configured [`FakeDriverAction`] and replays the
/// script whatever the request body contains, so it serves no v1.4 operations
/// (e.g. `SchemaColumns`). Advertizing the current `DRIVER_RPC_VERSION` would
/// claim a feature set the fake does not implement.
const FAKE_DRIVER_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::new(1, 3);

use interprocess::local_socket::{ListenerNonblockingMode::Neither, ListenerOptions};

/// Script of actions for the fake driver server to perform on each connection.
#[derive(Clone, Debug)]
pub enum FakeDriverAction {
    /// Reply with a pong.
    Pong,
    /// Emit an audit event (intermediate, `done=false`) then pong.
    EmitAuditThenPong(AuditEventEmitDto),
    /// Emit N audit frames (all `done=false`) then a pong.
    /// Used to exercise the rate-limit drop path while verifying session continuity.
    EmitNAuditThenPong(u32, AuditEventEmitDto),
    /// Reply with the given body, whatever the request was.
    Respond(Box<DriverResponseBody>),
}

impl FakeDriverAction {
    pub fn respond(body: DriverResponseBody) -> Self {
        Self::Respond(Box::new(body))
    }
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
    /// Version advertised in `Hello` and stamped on every frame.
    pub protocol_version: ProtocolVersion,
    /// Capabilities the advertised driver metadata declares.
    pub driver_capabilities: DriverCapabilities,
    /// Every request body received after hello, in arrival order.
    pub received_requests: Arc<Mutex<Vec<DriverRequestBody>>>,
}

impl FakeDriverRpcConfig {
    pub fn new(socket_id: impl Into<String>) -> Self {
        Self {
            socket_id: socket_id.into(),
            audit_emit_capability: false,
            actions: vec![FakeDriverAction::Pong],
            expected_connections: 1,
            protocol_version: FAKE_DRIVER_PROTOCOL_VERSION,
            driver_capabilities: DriverCapabilities::empty(),
            received_requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Advertises `version` instead of the pinned v1.3. The fake still has no
    /// per-request dispatch, so a test that raises the version must script
    /// every response the client expects on that version.
    pub fn with_protocol_version(mut self, version: ProtocolVersion) -> Self {
        self.protocol_version = version;
        self
    }

    pub fn with_driver_capabilities(mut self, capabilities: DriverCapabilities) -> Self {
        self.driver_capabilities = capabilities;
        self
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
    received_requests: Arc<Mutex<Vec<DriverRequestBody>>>,
}

impl FakeDriverRpcServer {
    pub fn start(config: FakeDriverRpcConfig) -> io::Result<Self> {
        let socket_name = driver_socket_name(&config.socket_id)?;
        let listener = ListenerOptions::new()
            .name(socket_name.borrow())
            .nonblocking(Neither)
            .create_sync()?;

        let received_requests = Arc::clone(&config.received_requests);
        let join_handle = thread::spawn(move || run_server(listener, config));

        Ok(Self {
            join_handle: Some(join_handle),
            received_requests,
        })
    }

    /// Request bodies the server has received after hello so far.
    pub fn received_requests(&self) -> Vec<DriverRequestBody> {
        match self.received_requests.lock() {
            Ok(requests) => requests.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
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

        let protocol_version = config.protocol_version;

        for action in &config.actions {
            let request: DriverRequestEnvelope = framing::recv_msg(&mut stream)?;
            match config.received_requests.lock() {
                Ok(mut requests) => requests.push(request.body.clone()),
                Err(poisoned) => poisoned.into_inner().push(request.body.clone()),
            }

            match action {
                FakeDriverAction::Pong => {
                    let pong = DriverResponseEnvelope::ok(
                        protocol_version,
                        request.request_id,
                        request.session_id,
                        DriverResponseBody::Pong,
                    );
                    framing::send_msg(&mut stream, &pong)?;
                }

                FakeDriverAction::EmitAuditThenPong(dto) => {
                    let audit_frame = DriverResponseEnvelope {
                        protocol_version,
                        request_id: request.request_id,
                        session_id: request.session_id,
                        done: false,
                        body: DriverResponseBody::EmitAuditEvent(dto.clone()),
                    };
                    framing::send_msg(&mut stream, &audit_frame)?;

                    let pong = DriverResponseEnvelope::ok(
                        protocol_version,
                        request.request_id,
                        request.session_id,
                        DriverResponseBody::Pong,
                    );
                    framing::send_msg(&mut stream, &pong)?;
                }

                FakeDriverAction::EmitNAuditThenPong(n, dto) => {
                    for _ in 0..*n {
                        let audit_frame = DriverResponseEnvelope {
                            protocol_version,
                            request_id: request.request_id,
                            session_id: request.session_id,
                            done: false,
                            body: DriverResponseBody::EmitAuditEvent(dto.clone()),
                        };
                        framing::send_msg(&mut stream, &audit_frame)?;
                    }

                    let pong = DriverResponseEnvelope::ok(
                        protocol_version,
                        request.request_id,
                        request.session_id,
                        DriverResponseBody::Pong,
                    );
                    framing::send_msg(&mut stream, &pong)?;
                }

                FakeDriverAction::Respond(body) => {
                    let response = DriverResponseEnvelope::ok(
                        protocol_version,
                        request.request_id,
                        request.session_id,
                        body.as_ref().clone(),
                    );
                    framing::send_msg(&mut stream, &response)?;
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
    .capabilities(config.driver_capabilities)
    .build();

    let mut capabilities = vec![DriverCapability::Cancellation];
    if config.audit_emit_capability {
        capabilities.push(DriverCapability::AuditEmit);
    }

    let hello = DriverHelloResponse {
        server_name: "fake-rpc-host".to_string(),
        server_version: "0.0.1".to_string(),
        selected_version: config.protocol_version,
        capabilities,
        driver_kind: DbKind::SQLite,
        driver_metadata: metadata,
        form_definition: DriverFormDef { tabs: vec![] },
        settings_schema: None,
    };

    DriverResponseEnvelope::ok(
        config.protocol_version,
        request_id,
        None,
        DriverResponseBody::Hello(hello),
    )
}

#[cfg(test)]
mod tests {
    use super::FAKE_DRIVER_PROTOCOL_VERSION;
    use dbflux_ipc::{DRIVER_RPC_VERSION, ProtocolVersion};

    #[test]
    fn fake_pins_v1_3_below_the_current_protocol_version() {
        assert_eq!(FAKE_DRIVER_PROTOCOL_VERSION, ProtocolVersion::new(1, 3));
        assert_eq!(FAKE_DRIVER_PROTOCOL_VERSION.major, DRIVER_RPC_VERSION.major);
        // Compile-time pin: the fake protocol version must stay below the
        // current protocol version, so a bump that breaks the fake fails here.
        const {
            assert!(FAKE_DRIVER_PROTOCOL_VERSION.minor < DRIVER_RPC_VERSION.minor);
        }
    }
}
