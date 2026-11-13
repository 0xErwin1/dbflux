//! MCP tasks (SEP-2663) and the message resource, over the real stdio surface.
//!
//! No GPUI application runs here, so every tool that needs one fails. That is
//! enough to show the plumbing: a client that declares the tasks extension gets
//! a task handle for a blocking tool and observes it reach a terminal state
//! through `tasks/get`, while a client that does not gets the result inline.

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

/// Request parameters in the 2026-07-28 dialect, with the given client capabilities.
fn modern(capabilities: &JsonValue, params: JsonValue) -> JsonValue {
    let mut params = params;
    if let Some(object) = params.as_object_mut() {
        object.insert(
            "_meta".to_owned(),
            json!({
                "io.modelcontextprotocol/protocolVersion": MODERN_VERSION,
                "io.modelcontextprotocol/clientCapabilities": capabilities,
            }),
        );
    }
    params
}

fn tasks_capability() -> JsonValue {
    json!({ "extensions": { "io.modelcontextprotocol/tasks": {} } })
}

#[tokio::test]
async fn blocking_tools_run_as_tasks_for_clients_that_declare_them() -> Result<(), String> {
    let mut server = Server::start()?;
    let call =
        json!({ "name": "wait_for_messages", "arguments": { "since": 0, "timeout_ms": 50 } });

    let created = server
        .request("tools/call", modern(&tasks_capability(), call.clone()))
        .await?;
    assert_eq!(created["resultType"], "task", "{created}");
    let task_id = created
        .get("taskId")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| format!("expected a task handle, got {created}"))?
        .to_owned();

    let mut last = JsonValue::Null;
    let terminal = async {
        loop {
            last = server
                .request(
                    "tasks/get",
                    modern(&tasks_capability(), json!({ "taskId": task_id })),
                )
                .await?;
            let status = last
                .pointer("/task/status")
                .or_else(|| last.get("status"))
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .to_owned();
            if matches!(status.as_str(), "completed" | "failed" | "cancelled") {
                return Ok::<_, String>(status);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    let status = timeout(REPLY_TIMEOUT, terminal)
        .await
        .map_err(|_| format!("the task never finished; last state {last}"))??;
    assert!(
        matches!(status.as_str(), "completed" | "failed"),
        "with no application the wait must end, got {status}: {last}"
    );

    // A client without the extension gets the tool's own answer inline.
    let inline = server
        .request("tools/call", modern(&json!({}), call))
        .await?;
    assert!(
        inline.get("task").is_none() && inline.get("content").is_some(),
        "expected an inline tool result, got {inline}"
    );

    server.stop().await;
    Ok(())
}

#[tokio::test]
async fn the_message_log_is_a_subscribable_resource() -> Result<(), String> {
    let mut server = Server::start()?;
    let initialize = server
        .request(
            "initialize",
            json!({
                "protocolVersion": LEGACY_VERSION,
                "capabilities": {},
                "clientInfo": { "name": "gpui-mcp-messages-test", "version": "0.0.0" },
            }),
        )
        .await?;
    assert_eq!(
        initialize.pointer("/capabilities/resources/subscribe"),
        Some(&json!(true)),
        "the server must advertise resource subscriptions, got {initialize}"
    );
    server
        .notify("notifications/initialized", json!({}))
        .await?;

    let resources = server.request("resources/list", json!({})).await?;
    assert!(
        resources
            .get("resources")
            .and_then(JsonValue::as_array)
            .is_some_and(|list| list.iter().any(|r| r["uri"] == "gpui://messages")),
        "gpui://messages must be listed, got {resources}"
    );
    server
        .request("resources/subscribe", json!({ "uri": "gpui://messages" }))
        .await?;
    assert!(
        server
            .request("resources/subscribe", json!({ "uri": "gpui://apps" }))
            .await
            .is_err(),
        "only the message log supports subscriptions"
    );
    server
        .request("resources/unsubscribe", json!({ "uri": "gpui://messages" }))
        .await?;

    server.stop().await;
    Ok(())
}
