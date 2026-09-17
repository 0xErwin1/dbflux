//! Process-level end-to-end coverage for `dbflux mcp`.
//!
//! This is the only test that leaves the process: it spawns the real binary,
//! seeds the settings database the server validates `--client-id` against, and
//! drives the server with hand-written JSON-RPC over stdio. Nothing here shares
//! code with the server, so a refactor that breaks both ends symmetrically still
//! fails: the handshake, the negotiated protocol version, the error shape and
//! the stdout stream are asserted as bytes an external client would send and
//! receive.
//!
//! The child is isolated with `XDG_DATA_HOME`/`HOME`/`XDG_CONFIG_HOME`, which is
//! why this test is Linux-gated: elsewhere those variables do not decide the
//! data directory, and an unisolated run would touch the real `dbflux.db`.

#![cfg(all(target_os = "linux", feature = "mcp"))]

use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use dbflux_storage::repositories::governance_settings::{GovernanceSettingsDto, TrustedClientDto};
use serde_json::{Value, json};

/// The actor id the child's settings database is seeded with.
const AGENT: &str = "e2e-stdio-agent";

/// The protocol version the client asks for. The server must answer with the
/// version it supports; pinning it here is what catches an accidental spec bump.
const PROTOCOL_VERSION: &str = "2025-11-25";

const REPLY_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// A running `dbflux mcp` process plus the pipes it is driven through.
struct StdioAgent {
    child: Child,
    stdin: ChildStdin,
    replies: Receiver<Result<String, String>>,
    stderr: Option<JoinHandle<String>>,
    next_id: u64,
}

impl StdioAgent {
    /// Spawns the binary against a freshly seeded settings database.
    fn start() -> (Self, tempfile::TempDir) {
        let home = tempfile::tempdir().expect("create the isolated home");
        seed_settings_database(home.path());

        let child = Command::new(env!("CARGO_BIN_EXE_dbflux"))
            .args(["mcp", "--client-id", AGENT])
            .env("HOME", home.path())
            .env("XDG_DATA_HOME", home.path())
            .env("XDG_CONFIG_HOME", home.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn `dbflux mcp`");

        let mut child = child;
        let stdin = child.stdin.take().expect("stdin should be piped");
        let stdout = child.stdout.take().expect("stdout should be piped");
        let mut stderr = child.stderr.take().expect("stderr should be piped");

        let (sender, replies) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let line = line.map_err(|error| error.to_string());
                if sender.send(line).is_err() {
                    return;
                }
            }
        });

        let stderr = std::thread::spawn(move || {
            let mut buffer = String::new();
            let _ = stderr.read_to_string(&mut buffer);
            buffer
        });

        (
            Self {
                child,
                stdin,
                replies,
                stderr: Some(stderr),
                next_id: 1,
            },
            home,
        )
    }

    fn send(&mut self, message: &Value) {
        let line = serde_json::to_string(message).expect("serialize a JSON-RPC message");
        self.stdin
            .write_all(line.as_bytes())
            .and_then(|()| self.stdin.write_all(b"\n"))
            .and_then(|()| self.stdin.flush())
            .expect("write to the child's stdin");
    }

    /// Sends a request and returns its response, skipping notifications.
    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;

        self.send(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }));

        let deadline = Instant::now() + REPLY_TIMEOUT;
        loop {
            let line = self.next_line(deadline);
            let message: Value = serde_json::from_str(&line).unwrap_or_else(|error| {
                self.fail(format!("stdout carried a non-JSON line ({error}): {line}"))
            });
            if message.get("id") == Some(&json!(id)) {
                return message;
            }
        }
    }

    /// Reads the next stdout line, failing fast when the child dies first.
    fn next_line(&mut self, deadline: Instant) -> String {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match self.replies.recv_timeout(remaining) {
            Ok(Ok(line)) => line,
            Ok(Err(error)) => self.fail(format!("reading the child's stdout failed: {error}")),
            Err(RecvTimeoutError::Timeout) => {
                let status = self
                    .child
                    .try_wait()
                    .ok()
                    .flatten()
                    .map(|status| status.to_string());
                self.fail(match status {
                    Some(status) => {
                        format!("the server exited ({status}) without answering")
                    }
                    None => format!("the server did not answer within {REPLY_TIMEOUT:?}"),
                })
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.fail("the server closed stdout without answering".to_string())
            }
        }
    }

    /// Reads the text block of a tool result and parses it as JSON.
    fn tool_json(&mut self, response: &Value) -> Value {
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_else(|| self.fail(format!("tool result carried no text block: {response}")))
            .to_string();
        serde_json::from_str(&text).unwrap_or_else(|error| {
            self.fail(format!("tool result text is not JSON ({error}): {text}"))
        })
    }

    /// Closes stdin and waits for the process to shut down cleanly.
    fn shutdown(self) {
        let StdioAgent {
            child,
            stdin,
            replies,
            stderr,
            ..
        } = self;
        let mut child = child;
        let mut stderr = stderr;
        drop(stdin);

        let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
        loop {
            match child.try_wait().expect("poll the child") {
                Some(status) => {
                    let diagnostics = join_stderr(stderr.take());
                    assert!(
                        status.success(),
                        "`dbflux mcp` should exit cleanly on stdin EOF, got {status}.\nstderr:\n{diagnostics}"
                    );
                    break;
                }
                None if Instant::now() >= deadline => {
                    let _ = child.kill();
                    panic!(
                        "`dbflux mcp` did not exit within {SHUTDOWN_TIMEOUT:?} of stdin EOF.\nstderr:\n{}",
                        join_stderr(stderr.take())
                    );
                }
                None => std::thread::sleep(Duration::from_millis(20)),
            }
        }

        // Every reply was already checked; this drains whatever the server
        // wrote before closing, so nothing on stdout goes unexamined.
        while let Ok(line) = replies.recv() {
            match line {
                Ok(line) if line.trim().is_empty() => {}
                Ok(line) => assert!(
                    serde_json::from_str::<Value>(&line).is_ok(),
                    "stdout carried a non-JSON line: {line}"
                ),
                Err(error) => panic!("reading the child's stdout failed: {error}"),
            }
        }
    }

    fn take_stderr(&mut self) -> String {
        join_stderr(self.stderr.take())
    }

    fn fail(&mut self, message: String) -> ! {
        let _ = self.child.kill();
        let stderr = self.take_stderr();
        panic!("{message}\nstderr:\n{stderr}");
    }
}

fn join_stderr(handle: Option<JoinHandle<String>>) -> String {
    handle
        .map(|handle| {
            handle
                .join()
                .unwrap_or_else(|_| "<stderr reader panicked>".into())
        })
        .unwrap_or_default()
}

/// Creates the settings database the child reads, with the agent registered as a
/// trusted client and MCP enabled by default — the same state the GUI writes.
fn seed_settings_database(home: &std::path::Path) {
    let data_dir = data_dir_for(home);
    std::fs::create_dir_all(&data_dir).expect("create the isolated data directory");

    let database = data_dir.join(dbflux_core::ReleaseChannel::current().db_file_name());
    let runtime =
        dbflux_storage::StorageRuntime::for_path(database.clone()).expect("create dbflux.db");

    runtime
        .governance_settings()
        .upsert(&GovernanceSettingsDto {
            id: 1,
            mcp_enabled_by_default: 1,
            updated_at: String::new(),
        })
        .expect("enable MCP by default");

    runtime
        .governance_settings()
        .replace_trusted_clients(&[TrustedClientDto {
            id: "1".to_string(),
            governance_id: 1,
            client_id: AGENT.to_string(),
            name: "MCP stdio e2e agent".to_string(),
            issuer: None,
            active: 1,
        }])
        .expect("register the trusted client");
}

/// Mirrors `dbflux_storage::paths::data_dir()` under the isolated `XDG_DATA_HOME`
/// the child is given.
fn data_dir_for(home: &std::path::Path) -> PathBuf {
    home.join("dbflux")
}

#[test]
fn stdio_agent_completes_the_handshake_and_lists_the_catalog() {
    let (mut agent, _home) = StdioAgent::start();

    let initialized = agent.request(
        "initialize",
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": { "name": "dbflux-e2e-agent", "version": "0.0.0" }
        }),
    );

    assert_eq!(
        initialized["result"]["protocolVersion"],
        json!(PROTOCOL_VERSION),
        "the server must negotiate the protocol version the client asked for: {initialized}"
    );
    assert!(
        initialized["result"]["serverInfo"]["name"]
            .as_str()
            .is_some_and(|name| !name.is_empty()),
        "the server must identify itself: {initialized}"
    );
    assert!(
        initialized["result"]["capabilities"]["tools"].is_object(),
        "the server must advertise the tools capability: {initialized}"
    );

    agent.send(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }));

    let listed = agent.request("tools/list", json!({}));
    let tools = listed["result"]["tools"]
        .as_array()
        .expect("tools/list should return a tools array");
    assert!(
        tools.len() >= 30,
        "the catalog should expose the full tool surface, got {}",
        tools.len()
    );
    assert!(
        tools
            .iter()
            .any(|tool| tool["name"] == json!("list_connections")),
        "list_connections should be part of the catalog"
    );

    let connections = agent.request(
        "tools/call",
        json!({ "name": "list_connections", "arguments": {} }),
    );
    let connections = agent.tool_json(&connections);
    assert_eq!(
        connections["connections"],
        json!([]),
        "a fresh settings database exposes no connections"
    );

    agent.shutdown();
}

#[test]
fn stdio_agent_denial_is_reported_and_audited() {
    let (mut agent, _home) = StdioAgent::start();

    agent.request(
        "initialize",
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": { "name": "dbflux-e2e-agent", "version": "0.0.0" }
        }),
    );
    agent.send(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }));

    // A client the settings database grants the default read-only role cannot
    // write. Two gates can deny it, and both are asserted here because the order
    // they fire in is part of the contract an agent has to reason about: the
    // connection gate runs before the policy engine.
    let denied_on_connection = agent.request(
        "tools/call",
        json!({
            "name": "insert_record",
            "arguments": {
                "connection_id": "00000000-0000-0000-0000-000000000000",
                "table": "items",
                "records": [{ "id": 1 }]
            }
        }),
    );
    assert_eq!(
        denied_on_connection["error"]["code"],
        json!(-32600),
        "a denial is an invalid-request error: {denied_on_connection}"
    );
    assert_eq!(
        denied_on_connection["error"]["data"]["code"],
        json!("connection_not_mcp_enabled"),
        "an unknown connection is refused before the policy is consulted: {denied_on_connection}"
    );
    assert!(
        denied_on_connection["result"].is_null(),
        "a denial must not carry a tool result: {denied_on_connection}"
    );

    // A global tool the read-only role does not list never reaches the handler.
    let denied_by_policy = agent.request(
        "tools/call",
        json!({
            "name": "delete_script",
            "arguments": { "path": "e2e.sql", "confirm": "e2e.sql" }
        }),
    );
    assert_eq!(
        denied_by_policy["error"]["code"],
        json!(-32600),
        "a policy denial is an invalid-request error: {denied_by_policy}"
    );
    assert_eq!(
        denied_by_policy["error"]["data"]["code"],
        json!("policy_denied"),
        "the denial should carry its governance code: {denied_by_policy}"
    );
    assert_eq!(
        denied_by_policy["error"]["message"],
        json!("tool denied by policy"),
        "the agent should see the policy verdict: {denied_by_policy}"
    );

    // The agent can read back its own verdicts from the audit trail.
    let audited = agent.request(
        "tools/call",
        json!({
            "name": "query_audit_logs",
            "arguments": { "actor_id": AGENT, "limit": 50 }
        }),
    );
    let audited = agent.tool_json(&audited);
    let entries = audited
        .as_array()
        .expect("query_audit_logs should return an array of events");
    let failures = entries
        .iter()
        .filter(|entry| entry["decision"] == json!("failure"))
        .count();
    assert!(
        failures >= 2,
        "both denials should be recorded in the audit database: {audited}"
    );

    agent.shutdown();
}
