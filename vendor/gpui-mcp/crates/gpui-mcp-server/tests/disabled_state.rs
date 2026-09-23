//! Disabled state, over the real MCP stdio surface.
//!
//! OPE-135 reported that the UI tree does not merely omit whether a control
//! accepts input — it asserts a falsehood. `NodeState::enabled` is derived from
//! `accesskit::Node::is_disabled`, so a control that refuses input reads
//! `enabled: true` exactly like one that accepts it, and a consumer has no way
//! to tell the field is unreliable. The downstream cost is that an accessibility
//! oracle cannot separate "disabled, correctly not focusable" from "enabled but
//! wrongly unreachable", which are the two cases that matter.
//!
//! The fixture therefore carries a control that is disabled to begin with and a
//! toggle that flips it, and this test walks the attribute in both directions.
//! One direction alone is not enough: a tree that reported every control as
//! disabled would pass the first assertion, and today's tree — which reports
//! every control as enabled — passes the second. Only the flip, measured on one
//! node across one toggle, distinguishes a carried value from a constant.
//!
//! An enabled sibling is read from the same tree at every step, so a run in
//! which the whole tree collapsed to one value cannot be mistaken for a pass.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

const REPLY_TIMEOUT: Duration = Duration::from_mins(1);
const DISCOVERY_DEADLINE: Duration = Duration::from_secs(45);
/// How long the fixture is given to render the frame that carries a flipped
/// attribute. A click returns as soon as the application has handled it, and the
/// tree the next call reads is the last *rendered* frame.
const SETTLE_MS: u64 = 5000;

/// The control the fixture disables, and the toggle that enables it.
const LOCKED: &str = "locked-action";
const TOGGLE: &str = "lock-toggle";

/// A GPUI MCP server child process driven over its real JSON-RPC stdio surface.
struct Server {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    next_id: i64,
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

    /// Whether the tree reports `id` as accepting input.
    ///
    /// Read out of `get_ui_tree` rather than `get_element_state`, because the
    /// tree is the artifact the accessibility oracle consumes and the one the
    /// bug was reported against.
    async fn enabled(&mut self, id: &str) -> Result<bool, String> {
        let tree = self.call_json("get_ui_tree", json!({})).await?;
        tree.get("nodes")
            .and_then(|nodes| nodes.get(id))
            .ok_or_else(|| format!("the fixture published no node named {id}"))?
            .get("state")
            .and_then(|state| state.get("enabled"))
            .and_then(JsonValue::as_bool)
            .ok_or_else(|| format!("the node {id} carries no boolean enabled state"))
    }

    async fn stop(mut self) {
        drop(self.stdin);
        let _ = self.child.kill().await;
    }
}

/// The instrumented GPUI application the workspace ships as its bridge demo.
struct Fixture {
    child: Child,
}

impl Fixture {
    fn start(endpoints: &Path) -> Result<Self, String> {
        let child = Command::new(fixture_executable()?)
            .arg("--endpoint-dir")
            .arg(endpoints)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| format!("could not spawn the fixture application: {error}"))?;
        Ok(Self { child })
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

/// Wait until exactly the fixture is discoverable through the private endpoint
/// directory, so the measurement is against one application and one target.
async fn wait_for_the_fixture(server: &mut Server) -> Result<(), String> {
    let started = Instant::now();
    let mut last = String::new();
    while started.elapsed() < DISCOVERY_DEADLINE {
        match server.call_json("list_apps", json!({})).await {
            Ok(apps) => {
                let count = apps.get("count").and_then(JsonValue::as_u64).unwrap_or(0);
                if count == 1 {
                    return Ok(());
                }
                last = format!("the endpoint directory published {count} applications");
            }
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!(
        "the fixture did not become discoverable within {DISCOVERY_DEADLINE:?}: {last}"
    ))
}

#[tokio::test]
async fn a_disabled_control_reads_as_disabled_and_follows_the_control_when_it_is_enabled()
-> Result<(), String> {
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

    let outcome = measure(&mut server).await;

    server.stop().await;
    fixture.stop().await;
    outcome
}

async fn measure(server: &mut Server) -> Result<(), String> {
    server
        .request(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "gpui-mcp-disabled-state-test", "version": "0.0.0" },
            }),
        )
        .await?;
    server
        .send(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized", "params": {} }))
        .await?;
    wait_for_the_fixture(server).await?;

    assert!(
        !server.enabled(LOCKED).await?,
        "the fixture starts with {LOCKED} refusing input, so the tree must report it disabled"
    );
    assert!(
        server.enabled(TOGGLE).await?,
        "{TOGGLE} accepts input in the same tree, so a tree that reports every control \
         disabled cannot be mistaken for a pass"
    );

    // Flipping the control is what separates a carried value from a constant:
    // the node, the tree and the reader are all the same across the toggle, and
    // only the application's own state changed.
    for expected in [true, false] {
        server
            .call("click_element", json!({ "id": TOGGLE }))
            .await?;
        server
            .call(
                "wait_for_state",
                json!({ "id": LOCKED, "enabled": expected, "timeout_ms": SETTLE_MS }),
            )
            .await
            .map_err(|error| {
                format!("{LOCKED} never reached enabled={expected} after the toggle: {error}")
            })?;
        assert_eq!(
            server.enabled(LOCKED).await?,
            expected,
            "after the toggle the tree must report {LOCKED} as enabled={expected}"
        );
        assert!(
            server.enabled(TOGGLE).await?,
            "{TOGGLE} must go on accepting input while it flips {LOCKED}"
        );
    }

    Ok(())
}
