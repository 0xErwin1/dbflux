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
            fixture: None,
        }
    }

    pub(crate) async fn begin(&self) -> Option<McpToolUsageCapability> {
        self.accept_begin(
            self.request(IpcMessage::BeginMcpToolUsage {
                session_nonce: self.session_nonce,
            })
            .await?,
        )
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
