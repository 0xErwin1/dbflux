use super::*;
use dbflux_ipc::envelope::ProtocolVersion;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) const TOKEN: &str = "private-fixture-auth";

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
        let client = McpToolUsageClient::fixture(vec![begin, finish]);
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
            let request: AppControlRequest = recv_telemetry_msg(&mut finish_peer).await.unwrap();
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
        let client = McpToolUsageClient::fixture(vec![stream]);
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
    let client = McpToolUsageClient::fixture(vec![]);
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
        let client = McpToolUsageClient::fixture(vec![stream]);
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
        let client = McpToolUsageClient::fixture(vec![stream]);
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

async fn successful_after(tool: &str, response: serde_json::Value) -> bool {
    scope_call(async {
        let evidence = capture(tool).expect("the first reviewed call claims the scope");
        evidence.observe_json(&response);
        evidence.successful()
    })
    .await
}

#[tokio::test]
async fn connect_accepts_its_optional_database_fields_but_disconnect_does_not() {
    let minimal = serde_json::json!({"success": true, "message": "connected"});
    let with_databases = serde_json::json!({
        "success": true,
        "message": "connected",
        "current_database": "app",
        "databases": ["app", "postgres"],
    });
    assert!(successful_after("connect", minimal.clone()).await);
    assert!(successful_after("connect", with_databases.clone()).await);
    assert!(successful_after("disconnect", minimal).await);
    assert!(
        !successful_after("disconnect", with_databases).await,
        "the stricter disconnect shape is unchanged"
    );
}

#[tokio::test]
async fn an_empty_alter_table_request_earns_no_credit() {
    let response = |operations: serde_json::Value| {
        serde_json::json!({
            "altered": true,
            "table": "users",
            "atomic": true,
            "operations": operations,
        })
    };
    assert!(!successful_after("alter_table", response(serde_json::json!([]))).await);
    assert!(
        successful_after(
            "alter_table",
            response(serde_json::json!([{"success": true}]))
        )
        .await
    );
}

#[tokio::test]
async fn a_retry_that_reopens_the_claim_can_capture_again_but_a_nested_call_cannot() {
    scope_call(async {
        assert!(capture("execute_script").is_some());
        assert!(
            capture("list_tables").is_none(),
            "a nested call is excluded"
        );
        reopen_call_claim();
        assert!(capture("execute_script").is_some());
        assert!(capture("list_tables").is_none());
    })
    .await;
}

#[tokio::test]
async fn observation_is_inactive_without_evidence() {
    assert!(!observing(), "outside any scope");
    scope_call(async { assert!(!observing(), "no evidence installed") }).await;
    scope_call(async {
        let evidence = capture("list_tables");
        OPERATION
            .scope(evidence, async { assert!(observing()) })
            .await;
    })
    .await;
}

#[tokio::test]
async fn finish_retries_once_after_a_transport_failure_but_not_after_a_reply() {
    // The first connection closes before answering: a transport failure.
    let (lost, lost_peer) = tokio::io::duplex(4096);
    drop(lost_peer);
    let (retry, mut retry_peer) = tokio::io::duplex(4096);
    let client = McpToolUsageClient::fixture(vec![lost, retry]);
    let capability = McpToolUsageCapability {
        session_nonce: client.session_nonce,
        capability: Uuid::new_v4(),
    };
    let server = reply(
        &mut retry_peer,
        IpcResponse::McpToolUsageFinished { accepted: true },
    );
    let (finished, ()) = tokio::join!(client.finish(capability, true), server);
    assert_eq!(finished, Some(()));

    // An explicit rejection is final: the second connection is never used.
    let (rejected, mut rejected_peer) = tokio::io::duplex(4096);
    let (unused, mut unused_peer) = tokio::io::duplex(4096);
    let client = McpToolUsageClient::fixture(vec![rejected, unused]);
    let capability = McpToolUsageCapability {
        session_nonce: client.session_nonce,
        capability: Uuid::new_v4(),
    };
    let server = reply(
        &mut rejected_peer,
        IpcResponse::McpToolUsageFinished { accepted: false },
    );
    let (finished, ()) = tokio::join!(client.finish(capability, true), server);
    assert_eq!(finished, None);
    let mut byte = [0_u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(50), unused_peer.read(&mut byte))
            .await
            .is_err(),
        "a rejected finish must not open another connection"
    );
}
