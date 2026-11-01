//! Annotations and app-to-agent messaging against the demo, over real stdio and IPC.
//!
//! The demo registers `on_message` (it shows the agent's latest reply) and
//! `on_annotations` (it mirrors how many annotations exist and who changed them
//! last), and its "Ask agent" button posts a message for the agent. So every
//! step here crosses the whole path: MCP tool, bridge IPC, GPUI thread, and back.

mod support;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

const REPLY_TIMEOUT: Duration = Duration::from_mins(1);
const DISCOVERY_DEADLINE: Duration = Duration::from_secs(45);
const SETTLE_MS: u64 = 5000;

/// A GPUI MCP server child process driven over its real JSON-RPC stdio surface.
struct Server {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    next_id: i64,
    notifications: Vec<JsonValue>,
}

impl Server {
    fn start(endpoints: &Path, artifacts: &Path) -> Result<Self, String> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_gpui-mcp"))
            .arg("--endpoint-dir")
            .arg(endpoints)
            .arg("--artifact-dir")
            .arg(artifacts)
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
            notifications: Vec::new(),
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
            .map_err(|error| format!("could not flush the server's stdin: {error}"))
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
            if message.get("id").is_none() && message.get("method").is_some() {
                self.notifications.push(message);
                continue;
            }
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

    async fn call(&mut self, tool: &str, arguments: JsonValue) -> Result<JsonValue, String> {
        let result = self
            .request(
                "tools/call",
                json!({ "name": tool, "arguments": arguments }),
            )
            .await?;
        if result.get("isError").and_then(JsonValue::as_bool) == Some(true) {
            let content = result.get("content").cloned().unwrap_or(JsonValue::Null);
            return Err(format!("{tool} reported an error: {content}"));
        }
        Ok(result)
    }

    /// The structured payload of a tool that answers with JSON.
    async fn call_json(&mut self, tool: &str, arguments: JsonValue) -> Result<JsonValue, String> {
        let result = self.call(tool, arguments).await?;
        if let Some(structured) = result.get("structuredContent") {
            return Ok(structured.clone());
        }
        let text = result
            .get("content")
            .and_then(JsonValue::as_array)
            .and_then(|content| {
                content
                    .iter()
                    .find_map(|entry| entry.get("text").and_then(JsonValue::as_str))
            })
            .ok_or_else(|| format!("{tool} returned no JSON payload"))?;
        serde_json::from_str(text)
            .map_err(|error| format!("{tool} returned unreadable JSON: {error}"))
    }

    async fn stop(mut self) {
        drop(self.stdin);
        let _ = self.child.kill().await;
    }
}

/// The instrumented GPUI application the workspace ships as its bridge demo.
struct Fixture {
    child: Child,
    log: PathBuf,
}

impl Fixture {
    fn start(endpoints: &Path) -> Result<Self, String> {
        let log = endpoints.with_extension("fixture.stderr.log");
        let stderr = std::fs::File::create(&log)
            .map_err(|error| format!("could not create fixture log: {error}"))?;
        let child = Command::new(fixture_executable()?)
            .arg("--endpoint-dir")
            .arg(endpoints)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| format!("could not spawn the fixture application: {error}"))?;
        Ok(Self { child, log })
    }

    async fn stop(mut self) {
        let _ = self.child.kill().await;
    }
}

/// The demo application is a separate workspace member, so its binary is found
/// beside this test's own server binary rather than through `CARGO_BIN_EXE`.
fn fixture_executable() -> Result<PathBuf, String> {
    let server = PathBuf::from(env!("CARGO_BIN_EXE_gpui-mcp"));
    let directory = server
        .parent()
        .ok_or_else(|| "the server binary has no parent directory".to_owned())?;
    let path = directory.join(format!("gpui-mcp-demo{}", std::env::consts::EXE_SUFFIX));
    if path.is_file() {
        Ok(path)
    } else {
        Err(format!(
            "the demo fixture is not built at {}",
            path.display()
        ))
    }
}

/// Whether this machine can open a window at all. The Linux CI job runs the test
/// suite without a display server and drives windowed fixtures under Xvfb from a
/// separate step.
fn has_a_desktop_session() -> bool {
    if cfg!(target_os = "linux") {
        std::env::var_os("DISPLAY").is_some() || std::env::var_os("WAYLAND_DISPLAY").is_some()
    } else {
        true
    }
}

impl Server {
    /// Read until a notification with `method` arrives, keeping earlier ones.
    async fn notification(&mut self, method: &str) -> Result<JsonValue, String> {
        loop {
            if let Some(index) = self
                .notifications
                .iter()
                .position(|message| message["method"] == method)
            {
                return Ok(self.notifications.remove(index));
            }
            let line = timeout(REPLY_TIMEOUT, self.stdout.next_line())
                .await
                .map_err(|_| format!("timed out waiting for {method}"))?
                .map_err(|error| format!("could not read from the server: {error}"))?
                .ok_or_else(|| format!("the server closed stdout before {method}"))?;
            if let Ok(message) = serde_json::from_str::<JsonValue>(&line)
                && message.get("method").is_some()
            {
                self.notifications.push(message);
            }
        }
    }

    /// Wait until the node `id` is labelled `label`.
    async fn wait_for_label(&mut self, id: &str, label: &str) -> Result<(), String> {
        let waited = self
            .call(
                "wait_for_element",
                json!({ "query": label, "exact": true, "timeout_ms": SETTLE_MS }),
            )
            .await;
        if waited.is_ok() {
            return Ok(());
        }
        let tree = self.call_json("get_ui_tree", json!({})).await?;
        Err(format!(
            "{id} was never labelled {label:?}; it is {}",
            tree["nodes"][id]
        ))
    }
}

impl support::FixtureClient for Server {
    async fn call_json(&mut self, tool: &str, arguments: JsonValue) -> Result<JsonValue, String> {
        Server::call_json(self, tool, arguments).await
    }
}

#[tokio::test]
async fn annotations_and_messages_cross_the_bridge_both_ways() -> Result<(), String> {
    if !has_a_desktop_session() {
        eprintln!("skipping: this machine has no desktop session to open a window on");
        return Ok(());
    }
    let directory = TempDir::new()
        .map_err(|error| format!("could not create a temporary directory: {error}"))?;
    let endpoints = directory.path().join("endpoints");
    std::fs::create_dir_all(&endpoints)
        .map_err(|error| format!("could not create the endpoint directory: {error}"))?;
    let fixture = Fixture::start(&endpoints)?;
    let mut server = Server::start(&endpoints, &directory.path().join("artifacts"))?;

    let outcome = exercise(&mut server).await;

    let outcome = outcome.map_err(|error| support::fixture_failure(&fixture.log, &error));
    server.stop().await;
    fixture.stop().await;
    outcome
}

#[allow(clippy::too_many_lines)]
async fn exercise(server: &mut Server) -> Result<(), String> {
    server
        .request(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "gpui-mcp-chat-test", "version": "0.0.0" },
            }),
        )
        .await?;
    server
        .send(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized", "params": {} }))
        .await?;
    support::wait_for_fixture(server, &["chat-send", "increment"], DISCOVERY_DEADLINE).await?;

    // An annotation on a node resolves to that node's bounds, and the app's
    // on_annotations callback sees the agent's change.
    let annotated = server
        .call_json(
            "annotate_elements",
            json!({ "annotations": [{
                "id": "focus",
                "node_id": "increment",
                "label": "Click me",
                "ttl_ms": 600_000,
            }] }),
        )
        .await?;
    let tree = server.call_json("get_ui_tree", json!({})).await?;
    let bounds = &tree["nodes"]["increment"]["bounds"];
    let resolved = &annotated["annotations"][0]["resolved"];
    for axis in ["x", "y", "width", "height"] {
        let (Some(expected), Some(actual)) = (bounds[axis].as_f64(), resolved[axis].as_f64())
        else {
            return Err(format!("no bounds to compare: {bounds} vs {resolved}"));
        };
        assert!(
            (expected - actual).abs() < 0.5,
            "the annotation must be drawn on its node: {bounds} vs {resolved}"
        );
    }
    assert_eq!(annotated["annotations"][0]["source"], "agent");
    server
        .wait_for_label("annotation-count", "Annotations: 1 (agent)")
        .await?;

    // highlight_elements is now a group of node annotations beside it.
    server
        .call("highlight_elements", json!({ "ids": ["reset"] }))
        .await?;
    let listed = server.call_json("list_annotations", json!({})).await?;
    assert_eq!(
        listed["annotations"].as_array().map(Vec::len),
        Some(2),
        "{listed}"
    );
    server.call("clear_highlights", json!({})).await?;
    let listed = server.call_json("list_annotations", json!({})).await?;
    assert_eq!(listed["annotations"][0]["id"], "focus", "{listed}");
    assert_eq!(listed["annotations"].as_array().map(Vec::len), Some(1));

    // The app posts a message; a subscribed client is told, and the agent reads it.
    server
        .request("resources/subscribe", json!({ "uri": "gpui://messages" }))
        .await?;
    server
        .call("click_element", json!({ "id": "chat-send" }))
        .await?;
    let updated = server
        .notification("notifications/resources/updated")
        .await?;
    assert_eq!(updated["params"]["uri"], "gpui://messages");
    let page = server
        .call_json("read_messages", json!({ "since": 0 }))
        .await?;
    let message = &page["messages"][0];
    assert_eq!(message["from"], "app", "{page}");
    assert!(
        message["text"]
            .as_str()
            .is_some_and(|text| text.starts_with("Hello from the demo")),
        "{page}"
    );
    assert_eq!(page["unread_from_app"], 0, "reading marks the message read");

    // The agent replies; the app's on_message callback shows it.
    let reply_to = message["id"].clone();
    server
        .call(
            "send_message",
            json!({ "text": "Hi from the agent", "reply_to": reply_to }),
        )
        .await?;
    server
        .wait_for_label("chat-reply", "Hi from the agent")
        .await?;

    // A wait with nothing new times out with an empty page rather than an error.
    let latest = page["latest_id"].as_u64().unwrap_or_default() + 1;
    let waited = server
        .call_json(
            "wait_for_messages",
            json!({ "since": latest, "timeout_ms": 200 }),
        )
        .await?;
    assert_eq!(waited["timed_out"], true, "{waited}");

    server
        .call("remove_annotations", json!({ "all": true }))
        .await?;
    server
        .wait_for_label("annotation-count", "Annotations: 0 (agent)")
        .await?;
    Ok(())
}
