//! Best-effort app-control permission transport; no standalone telemetry owner.
use dbflux_ipc::{
    APP_CONTROL_VERSION,
    framing::{recv_telemetry_msg, send_telemetry_msg},
    protocol::{AppControlRequest, AppControlResponse, IpcMessage, IpcResponse},
};
use std::{future::Future, io, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite};
use uuid::Uuid;

const TRANSACTION_TIMEOUT: Duration = Duration::from_millis(250);

pub(crate) struct McpToolUsageClient {
    session_nonce: Uuid,
    #[cfg(test)]
    acknowledged_begins: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    fixture: Option<std::sync::Mutex<std::collections::VecDeque<tokio::io::DuplexStream>>>,
}

/// Consumed by finish, including explicit cancellation via finish(false).
/// Dropping performs no I/O: a future foreground owner must expire abandoned caps.
pub(crate) struct McpToolUsageCapability {
    session_nonce: Uuid,
    capability: Uuid,
}

impl McpToolUsageClient {
    pub(crate) fn new() -> Self {
        Self {
            session_nonce: Uuid::new_v4(),
            #[cfg(test)]
            acknowledged_begins: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            fixture: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn fixture(streams: Vec<tokio::io::DuplexStream>) -> Self {
        let mut client = Self::new();
        client.fixture = Some(std::sync::Mutex::new(streams.into()));
        client
    }

    #[cfg(test)]
    pub(crate) fn acknowledged_begins(&self) -> usize {
        self.acknowledged_begins
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) async fn begin(&self) -> Option<McpToolUsageCapability> {
        let capability = self.accept_begin(
            self.request(IpcMessage::BeginMcpToolUsage {
                session_nonce: self.session_nonce,
            })
            .await?,
        );
        #[cfg(test)]
        if capability.is_some() {
            self.acknowledged_begins
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        capability
    }

    fn accept_begin(&self, response: IpcResponse) -> Option<McpToolUsageCapability> {
        match response {
            IpcResponse::McpToolUsageCapability {
                capability: Some(capability),
            } => Some(McpToolUsageCapability {
                session_nonce: self.session_nonce,
                capability,
            }),
            _ => None,
        }
    }

    pub(crate) async fn finish(
        &self,
        capability: McpToolUsageCapability,
        success: bool,
    ) -> Option<()> {
        if capability.session_nonce != self.session_nonce {
            return None;
        }
        match self
            .request(IpcMessage::FinishMcpToolUsage {
                session_nonce: self.session_nonce,
                capability: capability.capability,
                success,
            })
            .await?
        {
            IpcResponse::McpToolUsageFinished { accepted: true } => Some(()),
            _ => None,
        }
    }

    async fn request(&self, body: IpcMessage) -> Option<IpcResponse> {
        #[cfg(test)]
        if let Some(fixture) = &self.fixture {
            let stream = fixture.lock().ok()?.pop_front()?;
            return transaction(
                async { Ok(stream) },
                Some(tests::TOKEN.into()),
                body,
                TRANSACTION_TIMEOUT,
            )
            .await;
        }
        // Resolve authentication only at use, never during construction. Missing
        // GUI/permission is an ordinary skip, not a tool failure or log payload.
        let token = dbflux_ipc::read_app_control_token().ok()?;
        transaction(connect(), Some(token), body, TRANSACTION_TIMEOUT).await
    }
}

async fn transaction<S, F>(
    connection: F,
    token: Option<String>,
    body: IpcMessage,
    deadline: Duration,
) -> Option<IpcResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: Future<Output = io::Result<S>>,
{
    let token = token?;
    tokio::time::timeout(deadline, async move {
        let stream = connection.await.ok()?;
        let request = AppControlRequest::new(Uuid::new_v4().as_u128() as u64, Some(token), body);
        exchange(stream, &request).await
    })
    .await
    .ok()
    .flatten()
}

async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    request: &AppControlRequest,
) -> Option<IpcResponse> {
    send_telemetry_msg(&mut stream, request).await.ok()?;
    let response: AppControlResponse = recv_telemetry_msg(&mut stream).await.ok()?;
    if !response
        .protocol_version
        .is_compatible_with(APP_CONTROL_VERSION)
        || response.protocol_version.minor < 1
        || response.request_id != request.request_id
    {
        return None;
    }
    Some(response.body)
}

#[cfg(unix)]
async fn connect() -> io::Result<interprocess::local_socket::tokio::Stream> {
    use interprocess::local_socket::tokio::{Stream, prelude::*};
    Stream::connect(dbflux_ipc::socket_name()?).await
}

#[cfg(windows)]
async fn connect() -> io::Result<tokio::net::windows::named_pipe::NamedPipeClient> {
    use tokio::net::windows::named_pipe::ClientOptions;
    // Win32 ERROR_PIPE_BUSY. Only this condition merits cooperative waiting;
    // the encompassing transaction deadline includes every retry and all I/O.
    // This bounds waiting/async I/O, not the duration of the native open syscall.
    const ERROR_PIPE_BUSY: i32 = 231;
    let path = dbflux_ipc::socket::app_control_named_pipe_path();
    loop {
        match ClientOptions::new().open(&path) {
            Ok(stream) => return Ok(stream),
            Err(error) if error.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

// Scope permission and positive completion evidence are deliberately separate.
// Neither carries tool identity or result data across the app-control boundary.
tokio::task_local! {
    static CALL_CLAIMED: AtomicBool;
    pub(crate) static OPERATION: Option<Arc<CompletionEvidence>>;
}

pub(crate) struct CompletionEvidence {
    tool: &'static str,
    typed: AtomicBool,
    primary_mutation: AtomicBool,
    completed: AtomicBool,
    failed: AtomicBool,
}

pub(crate) async fn scope_call<F: Future>(future: F) -> F::Output {
    CALL_CLAIMED
        .scope(AtomicBool::new(false), OPERATION.scope(None, future))
        .await
}

pub(crate) fn capture(tool: &str) -> Option<Arc<CompletionEvidence>> {
    let tool = reviewed_tool(tool)?;
    let claimed = CALL_CLAIMED
        .try_with(|claimed| claimed.swap(true, Ordering::Relaxed))
        .ok()?;
    (!claimed).then(|| {
        Arc::new(CompletionEvidence {
            tool,
            typed: AtomicBool::new(false),
            primary_mutation: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            failed: AtomicBool::new(false),
        })
    })
}

fn reviewed_tool(tool: &str) -> Option<&'static str> {
    // These opaque contracts lose required completion evidence before the hook:
    // count_records scalar conversion, insert_record CrudResult conversion, and
    // list_scripts' best-effort filesystem enumeration.
    Some(match tool {
        "list_connections" => "list_connections",
        "connect" => "connect",
        "disconnect" => "disconnect",
        "get_connection_info" => "get_connection_info",
        "list_databases" => "list_databases",
        "list_schemas" => "list_schemas",
        "list_tables" => "list_tables",
        "describe_object" => "describe_object",
        "explain_query" => "explain_query",
        "preview_mutation" => "preview_mutation",
        "select_data" => "select_data",
        "aggregate_data" => "aggregate_data",
        "update_records" => "update_records",
        "upsert_record" => "upsert_record",
        "delete_records" => "delete_records",
        "truncate_table" => "truncate_table",
        "drop_table" => "drop_table",
        "drop_database" => "drop_database",
        "create_table" => "create_table",
        "alter_table" => "alter_table",
        "create_index" => "create_index",
        "drop_index" => "drop_index",
        "create_type" => "create_type",
        "get_script" => "get_script",
        "create_script" => "create_script",
        "update_script" => "update_script",
        "delete_script" => "delete_script",
        "execute_script" => "execute_script",
        "query_audit_logs" => "query_audit_logs",
        "get_audit_entry" => "get_audit_entry",
        "export_audit_logs" => "export_audit_logs",
        _ => return None,
    })
}

impl CompletionEvidence {
    pub(crate) fn successful(&self) -> bool {
        self.completed.load(Ordering::Relaxed) && !self.failed.load(Ordering::Relaxed)
    }

    fn observe_json(&self, value: &serde_json::Value) {
        use serde_json::Value;
        let array = |key| value.get(key).is_some_and(Value::is_array);
        let string = |key| value.get(key).is_some_and(Value::is_string);
        let number = |key| value.get(key).and_then(Value::as_u64).is_some();
        let truth = |key| value.get(key) == Some(&Value::Bool(true));
        let query = || {
            self.typed.load(Ordering::Relaxed)
                && array("columns")
                && array("rows")
                && number("row_count")
        };
        let keys: &[&str] = match self.tool {
            "list_connections" => &["connections"],
            "connect" | "disconnect" => &["success", "message"],
            "get_connection_info" => &[
                "connection_id",
                "driver_type",
                "category",
                "status",
                "version",
                "current_database",
            ],
            "list_databases" => &["databases"],
            "list_schemas" => &["schemas"],
            "list_tables" => &["tables"],
            "describe_object" | "select_data" | "aggregate_data" | "explain_query"
            | "preview_mutation" | "execute_script" => {
                &["columns", "rows", "row_count", "result_sets"]
            }
            "update_records" => &["updated", "records"],
            "upsert_record" => &["upserted", "records"],
            "delete_records" => &["deleted", "records"],
            "truncate_table" => &["truncated", "table"],
            "drop_table" => &["dropped", "table"],
            "drop_database" => &["dropped", "database"],
            "create_table" => &["created", "table"],
            "create_index" => &["created", "index_name", "table"],
            "drop_index" => &["dropped", "index_name"],
            "create_type" => &["created", "type", "skipped"],
            "alter_table" => &[
                "altered",
                "table",
                "atomic",
                "operations",
                "non_atomic_alter",
                "aborted_at",
                "driver_managed",
                "statement_count",
                "illustrative_preview",
            ],
            "get_script" => &["path", "name", "content", "language", "size"],
            "create_script" => &["path", "name"],
            "update_script" => &["path", "size"],
            "get_audit_entry" | "query_audit_logs" => &[
                "id",
                "actor_id",
                "tool_id",
                "decision",
                "reason",
                "created_at_epoch_ms",
            ],
            _ => &[],
        };
        let known_object = |value: &Value| {
            value
                .as_object()
                .is_some_and(|object| object.keys().all(|key| keys.contains(&key.as_str())))
        };
        let known_shape = if let Some(items) = value.as_array() {
            items.iter().all(known_object)
        } else {
            known_object(value)
        };
        let positive = match self.tool {
            "list_connections" => array("connections"),
            "connect" | "disconnect" => truth("success") && string("message"),
            "get_connection_info" => {
                string("connection_id")
                    && string("driver_type")
                    && string("category")
                    && value.get("status").and_then(Value::as_str) == Some("connected")
            }
            "list_databases" => array("databases"),
            "list_schemas" => array("schemas"),
            "list_tables" => array("tables"),
            "describe_object" | "select_data" | "aggregate_data" | "explain_query"
            | "preview_mutation" | "execute_script" => query(),
            "update_records" => self.primary_mutation.load(Ordering::Relaxed) && number("updated"),
            "upsert_record" => self.primary_mutation.load(Ordering::Relaxed) && number("upserted"),
            "delete_records" => self.primary_mutation.load(Ordering::Relaxed) && number("deleted"),
            "truncate_table" => truth("truncated") && string("table"),
            "drop_table" => truth("dropped") && string("table"),
            "drop_database" => truth("dropped") && string("database"),
            "create_table" => truth("created") && string("table"),
            "create_index" => truth("created") && string("index_name") && string("table"),
            "drop_index" => truth("dropped") && string("index_name"),
            "create_type" => {
                string("type")
                    && (truth("created")
                        || (truth("skipped") && value.get("created") == Some(&Value::Bool(false))))
            }
            "alter_table" => {
                let aborted = value
                    .get("aborted_at")
                    .is_some_and(|value| !value.is_null());
                let failed_step = value
                    .get("operations")
                    .and_then(Value::as_array)
                    .is_some_and(|steps| {
                        steps
                            .iter()
                            .any(|step| step.get("success") != Some(&Value::Bool(true)))
                    });
                if !truth("altered") || aborted || failed_step {
                    self.failed.store(true, Ordering::Relaxed);
                }
                truth("altered")
                    && string("table")
                    && array("operations")
                    && (value.get("atomic").is_some_and(Value::is_boolean)
                        || truth("non_atomic_alter"))
                    && !aborted
                    && !failed_step
            }
            "query_audit_logs" => value.as_array().is_some_and(|items| {
                items.iter().all(|item| {
                    item.get("id").and_then(Value::as_i64).is_some()
                        && ["actor_id", "tool_id", "decision"]
                            .iter()
                            .all(|key| item.get(key).is_some_and(Value::is_string))
                        && item
                            .get("created_at_epoch_ms")
                            .and_then(Value::as_i64)
                            .is_some()
                })
            }),
            "get_script" => {
                string("path")
                    && string("name")
                    && string("content")
                    && string("language")
                    && number("size")
            }
            "create_script" => string("path") && string("name"),
            "update_script" => string("path") && number("size"),
            "get_audit_entry" => {
                value.is_object()
                    && number("id")
                    && string("actor_id")
                    && string("tool_id")
                    && string("decision")
                    && value
                        .get("created_at_epoch_ms")
                        .and_then(Value::as_i64)
                        .is_some()
            }
            _ => false,
        };
        if positive && known_shape {
            self.completed.store(true, Ordering::Relaxed);
        }
    }
}

pub(crate) fn observe_json(value: &serde_json::Value) {
    OPERATION
        .try_with(|evidence| {
            if let Some(evidence) = evidence {
                evidence.observe_json(value);
            }
        })
        .ok();
}

pub(crate) fn observe_primary_mutation(result: &dbflux_core::QueryResult, affected_key: &str) {
    observe_typed(result);
    OPERATION
        .try_with(|evidence| {
            if let Some(evidence) = evidence {
                let expected_key = match evidence.tool {
                    "update_records" => "updated",
                    "delete_records" => "deleted",
                    "upsert_record" => "upserted",
                    _ => return,
                };
                if affected_key == expected_key {
                    evidence.primary_mutation.store(true, Ordering::Relaxed);
                }
            }
        })
        .ok();
}

pub(crate) fn observe_typed(result: &dbflux_core::QueryResult) {
    OPERATION
        .try_with(|evidence| {
            if let Some(evidence) = evidence {
                evidence.typed.store(true, Ordering::Relaxed);
                fn failed(result: &dbflux_core::QueryResult) -> bool {
                    result
                        .metadata_extra
                        .as_ref()
                        .is_some_and(|metadata| metadata.contains_key("script_failure"))
                        || result.additional_results.iter().any(failed)
                }
                if failed(result) {
                    evidence.failed.store(true, Ordering::Relaxed);
                }
            }
        })
        .ok();
}

pub(crate) fn observe_final(result: &rmcp::model::CallToolResult) {
    OPERATION
        .try_with(|evidence| {
            if let Some(evidence) = evidence {
                evidence.completed.store(false, Ordering::Relaxed);
                let [content] = result.content.as_slice() else {
                    return;
                };
                if let Some(text) = content.as_text()
                    && let Ok(value) = serde_json::from_str(&text.text)
                {
                    evidence.observe_json(&value);
                }
                match evidence.tool {
                    // These two reviewed handlers return text only after their fallible primary work.
                    "export_audit_logs" if content.as_text().is_some() => {
                        evidence.completed.store(true, Ordering::Relaxed);
                    }
                    "delete_script"
                        if content.as_text().is_some_and(|text| {
                            text.text == r#"{"status": "Script deleted successfully"}"#
                        }) =>
                    {
                        evidence.completed.store(true, Ordering::Relaxed);
                    }
                    _ => {}
                }
            }
        })
        .ok();
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_ipc::envelope::ProtocolVersion;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    pub(super) const TOKEN: &str = "private-fixture-auth";

    fn fixture(streams: Vec<tokio::io::DuplexStream>) -> McpToolUsageClient {
        let mut client = McpToolUsageClient::new();
        client.fixture = Some(std::sync::Mutex::new(streams.into()));
        client
    }

    fn begin_body() -> IpcMessage {
        IpcMessage::BeginMcpToolUsage {
            session_nonce: Uuid::new_v4(),
        }
    }

    async fn reply(peer: &mut tokio::io::DuplexStream, body: IpcResponse) {
        let request: AppControlRequest = recv_telemetry_msg(peer).await.unwrap();
        assert_eq!(request.auth_token.as_deref(), Some(TOKEN));
        send_telemetry_msg(peer, &AppControlResponse::ok(request.request_id, body))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn authenticated_begin_and_finish_closed_roundtrip() {
        for success in [false, true] {
            let (begin, mut begin_peer) = tokio::io::duplex(4096);
            let (finish, mut finish_peer) = tokio::io::duplex(4096);
            let client = fixture(vec![begin, finish]);
            let capability = Uuid::new_v4();
            let server = async {
                let request: AppControlRequest = recv_telemetry_msg(&mut begin_peer).await.unwrap();
                assert_eq!(request.auth_token.as_deref(), Some(TOKEN));
                assert_eq!(request.protocol_version, ProtocolVersion::new(1, 1));
                assert!(matches!(request.body,
                    IpcMessage::BeginMcpToolUsage { session_nonce }
                    if session_nonce == client.session_nonce));
                send_telemetry_msg(
                    &mut begin_peer,
                    &AppControlResponse::ok(
                        request.request_id,
                        IpcResponse::McpToolUsageCapability {
                            capability: Some(capability),
                        },
                    ),
                )
                .await
                .unwrap();
            };
            let (handle, ()) = tokio::join!(client.begin(), server);
            let handle = handle.unwrap();
            assert_eq!(handle.capability, capability);
            let server = async {
                let request: AppControlRequest =
                    recv_telemetry_msg(&mut finish_peer).await.unwrap();
                assert_eq!(request.auth_token.as_deref(), Some(TOKEN));
                assert!(matches!(request.body,
                    IpcMessage::FinishMcpToolUsage {
                        session_nonce, capability: received, success: received_success
                    } if session_nonce == client.session_nonce
                        && received == capability && received_success == success));
                send_telemetry_msg(
                    &mut finish_peer,
                    &AppControlResponse::ok(
                        request.request_id,
                        IpcResponse::McpToolUsageFinished { accepted: true },
                    ),
                )
                .await
                .unwrap();
            };
            let (finished, ()) = tokio::join!(client.finish(handle, success), server);
            assert_eq!(finished, Some(()));
        }
    }

    #[tokio::test]
    async fn old_major_mismatched_and_unavailable_replies_skip() {
        for case in 0..7 {
            let (stream, mut peer) = tokio::io::duplex(4096);
            let client = fixture(vec![stream]);
            let server = async {
                let request: AppControlRequest = recv_telemetry_msg(&mut peer).await.unwrap();
                let mut response = AppControlResponse::ok(
                    request.request_id,
                    IpcResponse::McpToolUsageCapability {
                        capability: Some(Uuid::new_v4()),
                    },
                );
                match case {
                    0 => response.protocol_version.minor = 0,
                    1 => response.protocol_version.major = 2,
                    2 => response.request_id = request.request_id.wrapping_add(1),
                    3 => {
                        response.body = IpcResponse::Error {
                            message: "unauthorized".into(),
                        }
                    }
                    4 => response.body = IpcResponse::McpToolUsageCapability { capability: None },
                    5 => response.body = IpcResponse::Ok,
                    _ => response.body = IpcResponse::McpToolUsageFinished { accepted: false },
                }
                send_telemetry_msg(&mut peer, &response).await.unwrap();
            };
            let (response, ()) = tokio::join!(client.begin(), server);
            assert!(response.is_none());
        }
    }

    #[tokio::test]
    async fn finish_rejection_and_wrong_session_skip() {
        let client = fixture(vec![]);
        let wrong = McpToolUsageCapability {
            session_nonce: Uuid::new_v4(),
            capability: Uuid::new_v4(),
        };
        assert!(client.finish(wrong, true).await.is_none());
        for body in [
            IpcResponse::McpToolUsageFinished { accepted: false },
            IpcResponse::Ok,
            IpcResponse::Error {
                message: "unsupported".into(),
            },
        ] {
            let (stream, mut peer) = tokio::io::duplex(4096);
            let client = fixture(vec![stream]);
            let handle = McpToolUsageCapability {
                session_nonce: client.session_nonce,
                capability: Uuid::new_v4(),
            };
            let (result, ()) = tokio::join!(client.finish(handle, true), reply(&mut peer, body));
            assert!(result.is_none());
        }
    }

    #[tokio::test]
    async fn malformed_oversized_short_and_closed_replies_skip() {
        for bytes in [
            vec![1, 0, 0, 0, 255],
            vec![1, 0, 0, 0],
            vec![1, 16, 0, 0],
            vec![],
        ] {
            let (stream, mut peer) = tokio::io::duplex(4096);
            let client = fixture(vec![stream]);
            let server = async {
                let _: AppControlRequest = recv_telemetry_msg(&mut peer).await.unwrap();
                peer.write_all(&bytes).await.unwrap();
                drop(peer);
            };
            let (result, ()) = tokio::join!(client.begin(), server);
            assert!(result.is_none());
        }
    }

    #[tokio::test]
    async fn missing_token_does_not_poll_connection_and_missing_owner_skips() {
        let connection = std::future::poll_fn(
            |_| -> std::task::Poll<io::Result<tokio::io::DuplexStream>> {
                panic!("missing auth must not connect");
            },
        );
        assert!(
            transaction(connection, None, begin_body(), TRANSACTION_TIMEOUT)
                .await
                .is_none()
        );
        let missing =
            async { Err::<tokio::io::DuplexStream, _>(io::Error::from(io::ErrorKind::NotFound)) };
        assert!(
            transaction(
                missing,
                Some(TOKEN.into()),
                begin_body(),
                TRANSACTION_TIMEOUT
            )
            .await
            .is_none()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn single_deadline_includes_connect_and_read_and_closes_resource() {
        let (stream, mut peer) = tokio::io::duplex(4096);
        let connection = async {
            tokio::time::sleep(Duration::from_millis(150)).await;
            Ok(stream)
        };
        let server = async {
            let _: AppControlRequest = recv_telemetry_msg(&mut peer).await.unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
            let mut byte = [0];
            assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
        };
        let operation = async {
            let started = tokio::time::Instant::now();
            let result = transaction(
                connection,
                Some(TOKEN.into()),
                begin_body(),
                TRANSACTION_TIMEOUT,
            )
            .await;
            assert!(result.is_none());
            assert_eq!(started.elapsed(), TRANSACTION_TIMEOUT);
        };
        tokio::join!(operation, server);
    }

    #[tokio::test(start_paused = true)]
    async fn single_deadline_includes_backpressured_write_and_reply_read() {
        let (stream, mut peer) = tokio::io::duplex(1);
        let server = async {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let _: AppControlRequest = recv_telemetry_msg(&mut peer).await.unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
            let mut byte = [0];
            assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
        };
        let operation = async {
            let started = tokio::time::Instant::now();
            assert!(
                transaction(
                    async { Ok(stream) },
                    Some(TOKEN.into()),
                    begin_body(),
                    TRANSACTION_TIMEOUT,
                )
                .await
                .is_none()
            );
            assert_eq!(started.elapsed(), TRANSACTION_TIMEOUT);
        };
        tokio::join!(operation, server);
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_write_uses_same_deadline_and_drops_stream() {
        let (stream, mut peer) = tokio::io::duplex(1);
        let started = tokio::time::Instant::now();
        assert!(
            transaction(
                async { Ok(stream) },
                Some(TOKEN.into()),
                begin_body(),
                TRANSACTION_TIMEOUT,
            )
            .await
            .is_none()
        );
        assert_eq!(started.elapsed(), TRANSACTION_TIMEOUT);
        let mut remaining = Vec::new();
        peer.read_to_end(&mut remaining).await.unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[tokio::test]
    async fn cancel_read_drops_stream_without_cleanup_task() {
        let (stream, mut peer) = tokio::io::duplex(4096);
        let mut operation = Box::pin(transaction(
            async { Ok(stream) },
            Some(TOKEN.into()),
            begin_body(),
            TRANSACTION_TIMEOUT,
        ));
        tokio::select! {
            result = &mut operation => panic!("unexpected completion: {}", result.is_some()),
            request = recv_telemetry_msg::<_, AppControlRequest>(&mut peer) => { request.unwrap(); }
        }
        drop(operation);
        let mut byte = [0];
        assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    }
}
