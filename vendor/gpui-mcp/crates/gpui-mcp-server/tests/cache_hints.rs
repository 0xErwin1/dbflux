//! SEP-2549 cache hints on the list and read results the MCP stdio server returns.
//!
//! Protocol version `2026-07-28` requires every list and read result to carry
//! `ttlMs` and `cacheScope`; clients that negotiate it reject results without
//! them. Older versions must keep the legacy shape, which has neither field.

use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

const MODERN_VERSION: &str = "2026-07-28";
const LEGACY_VERSION: &str = "2025-06-18";
const REPLY_TIMEOUT: Duration = Duration::from_secs(20);

/// A GPUI MCP server child process driven over its real JSON-RPC stdio surface.
struct Server {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    next_id: i64,
    _directory: TempDir,
}

impl Server {
    fn start() -> Result<Self, String> {
        let directory = TempDir::new().map_err(|error| format!("temporary directory: {error}"))?;
        let mut child = Command::new(env!("CARGO_BIN_EXE_gpui-mcp"))
            .arg("--endpoint-dir")
            .arg(directory.path().join("endpoints"))
            .arg("--artifact-dir")
            .arg(directory.path().join("artifacts"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| format!("could not spawn the server: {error}"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "the server has no stdin".to_owned())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "the server has no stdout".to_owned())?;
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
            next_id: 1,
            _directory: directory,
        })
    }

    async fn send(&mut self, message: &JsonValue) -> Result<(), String> {
        let mut line = message.to_string();
        line.push('\n');
        self.stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|error| format!("could not write to the server: {error}"))?;
        self.stdin
            .flush()
            .await
            .map_err(|error| format!("could not flush the server stdin: {error}"))
    }

    async fn notify(&mut self, method: &str, params: JsonValue) -> Result<(), String> {
        let message = json!({ "jsonrpc": "2.0", "method": method, "params": params });
        self.send(&message).await
    }

    async fn request(&mut self, method: &str, params: JsonValue) -> Result<JsonValue, String> {
        let id = self.next_id;
        self.next_id += 1;
        let message = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        self.send(&message).await?;
        loop {
            let line = timeout(REPLY_TIMEOUT, self.stdout.next_line())
                .await
                .map_err(|_| format!("timed out waiting for the {method} reply"))?
                .map_err(|error| format!("could not read the {method} reply: {error}"))?
                .ok_or_else(|| format!("the server closed stdout before replying to {method}"))?;
            let Ok(message) = serde_json::from_str::<JsonValue>(&line) else {
                continue;
            };
            if message.get("id").and_then(JsonValue::as_i64) != Some(id) {
                continue;
            }
            if let Some(error) = message.get("error") {
                return Err(format!("{method} failed: {error}"));
            }
            return message
                .get("result")
                .cloned()
                .ok_or_else(|| format!("{method} returned neither a result nor an error"));
        }
    }

    async fn stop(mut self) {
        drop(self.stdin);
        let _ = self.child.kill().await;
    }
}

/// Request metadata that puts a request in `version`'s dialect (SEP-1319).
fn protocol_meta(version: &str) -> JsonValue {
    json!({
        "_meta": {
            "io.modelcontextprotocol/protocolVersion": version,
            "io.modelcontextprotocol/clientCapabilities": {},
        }
    })
}

fn assert_cache_hints(method: &str, result: &JsonValue) {
    let ttl_ms = result.get("ttlMs");
    assert!(
        ttl_ms.is_some_and(JsonValue::is_u64),
        "{method} must carry ttlMs as an integer of at least zero, got {ttl_ms:?}"
    );
    let cache_scope = result.get("cacheScope").and_then(JsonValue::as_str);
    assert!(
        matches!(cache_scope, Some("public" | "private")),
        "{method} must carry cacheScope as public or private, got {cache_scope:?}"
    );
}

#[tokio::test]
async fn modern_protocol_list_and_read_results_carry_cache_hints() -> Result<(), String> {
    let mut server = Server::start()?;
    let meta = protocol_meta(MODERN_VERSION);

    let discover = server.request("server/discover", meta.clone()).await?;
    assert!(
        discover
            .get("supportedVersions")
            .and_then(JsonValue::as_array)
            .is_some_and(|versions| versions.iter().any(|version| version == MODERN_VERSION)),
        "the server must advertise {MODERN_VERSION}, got {discover}"
    );

    for method in ["tools/list", "resources/list", "resources/templates/list"] {
        let result = server.request(method, meta.clone()).await?;
        assert_cache_hints(method, &result);
    }

    let mut read_params = meta.clone();
    if let Some(params) = read_params.as_object_mut() {
        params.insert("uri".to_owned(), json!("gpui://apps"));
    }
    let read = server.request("resources/read", read_params).await?;
    assert_cache_hints("resources/read", &read);

    server.stop().await;
    Ok(())
}

#[tokio::test]
async fn legacy_protocol_results_omit_cache_hints() -> Result<(), String> {
    let mut server = Server::start()?;
    let initialize = server
        .request(
            "initialize",
            json!({
                "protocolVersion": LEGACY_VERSION,
                "capabilities": {},
                "clientInfo": { "name": "gpui-mcp-cache-hints-test", "version": "0.0.0" },
            }),
        )
        .await?;
    assert_eq!(
        initialize
            .get("protocolVersion")
            .and_then(JsonValue::as_str),
        Some(LEGACY_VERSION),
        "the server must negotiate {LEGACY_VERSION}, got {initialize}"
    );
    server
        .notify("notifications/initialized", json!({}))
        .await?;

    let tools = server.request("tools/list", json!({})).await?;
    assert!(
        tools.get("ttlMs").is_none(),
        "{LEGACY_VERSION} tools/list must not carry ttlMs, got {tools}"
    );
    assert!(
        tools.get("cacheScope").is_none(),
        "{LEGACY_VERSION} tools/list must not carry cacheScope, got {tools}"
    );
    assert!(
        tools.get("tools").and_then(JsonValue::as_array).is_some(),
        "{LEGACY_VERSION} tools/list must still list tools, got {tools}"
    );

    server.stop().await;
    Ok(())
}
