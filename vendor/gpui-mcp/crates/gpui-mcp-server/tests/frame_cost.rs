//! What a hover costs, over the real MCP stdio surface.
//!
//! The fixture draws two regions as separate cached views, the way a workbench
//! draws each panel, so hovering the control in one region notifies that view
//! alone. GPUI should then render the hovered region and replay the other from
//! cache. The bridge used to hide this: it refreshed the whole window after every
//! injected event and settled with two more refreshes, so every hover measured a
//! whole-window redraw, three times over. `mark_frames` and `get_frame_report`
//! now show exactly which views rendered in the frames a hover caused, and why.
//!
//! One region renders and the other replays in the first hover; the second
//! hover swaps them. The swap is what makes the assertion mean something: a
//! report that always named the same entity as rendered, or that never
//! distinguished the two, fails it. A `refresh` cause anywhere fails it too,
//! because that is the cost the bridge used to add.
//!
//! The same hover driven by the operating system's own cursor is checked by an
//! ignored test, because it moves the real cursor: run it with
//! `cargo test -p gpui-mcp-server --test frame_cost -- --ignored`.

mod support;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
#[cfg(windows)]
use std::time::Instant;

use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

const REPLY_TIMEOUT: Duration = Duration::from_mins(1);
const DISCOVERY_DEADLINE: Duration = Duration::from_secs(45);
/// How long an operating-system cursor move is given to reach a drawn frame.
#[cfg(windows)]
const OS_INPUT_DEADLINE: Duration = Duration::from_secs(5);

const LEFT: &str = "probe-left-target";
const RIGHT: &str = "probe-right-target";
/// The fixture's heading, which reacts to nothing and so parks the pointer.
const PARKING: &str = "heading";
const REGION_TYPE: &str = "ProbeRegion";

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

    /// The window-relative logical center of a node in the current tree.
    async fn center(&mut self, id: &str) -> Result<(f64, f64), String> {
        let tree = self.call_json("get_ui_tree", json!({})).await?;
        let bounds = tree
            .get("nodes")
            .and_then(|nodes| nodes.get(id))
            .and_then(|node| node.get("bounds"))
            .ok_or_else(|| format!("the fixture published no bounds for {id}"))?;
        let field = |name: &str| {
            bounds
                .get(name)
                .and_then(JsonValue::as_f64)
                .ok_or_else(|| format!("the bounds of {id} carry no {name}"))
        };
        Ok((
            field("x")? + field("width")? / 2.0,
            field("y")? + field("height")? / 2.0,
        ))
    }

    async fn pointer_move(&mut self, point: (f64, f64)) -> Result<(), String> {
        self.call("pointer_move", json!({ "x": point.0, "y": point.1 }))
            .await
            .map(drop)
    }

    async fn report(&mut self) -> Result<Report, String> {
        let report = self.call_json("get_frame_report", json!({})).await?;
        Ok(Report(report))
    }

    async fn initialize(&mut self) -> Result<(), String> {
        self.request(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "gpui-mcp-frame-cost-test", "version": "0.0.0" },
            }),
        )
        .await?;
        self.send(
            &json!({ "jsonrpc": "2.0", "method": "notifications/initialized", "params": {} }),
        )
        .await?;
        wait_for_the_fixture(self).await
    }

    async fn stop(mut self) {
        drop(self.stdin);
        let _ = self.child.kill().await;
    }
}

/// A `get_frame_report` payload.
struct Report(JsonValue);

impl Report {
    fn frames(&self) -> &[JsonValue] {
        self.0
            .get("frames")
            .and_then(JsonValue::as_array)
            .map_or(&[], Vec::as_slice)
    }

    /// Every view the reported frames drew, as (entity, outcome, cause).
    fn draws(&self) -> Vec<(u64, String, String, Option<String>)> {
        self.0
            .get("views")
            .and_then(JsonValue::as_array)
            .into_iter()
            .flatten()
            .flat_map(|view| {
                let entity = view
                    .get("entity_id")
                    .and_then(JsonValue::as_u64)
                    .unwrap_or_default();
                let type_name = view
                    .get("type_name")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let causes = view
                    .get("causes")
                    .and_then(JsonValue::as_object)
                    .map(|causes| causes.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();
                let reused = view.get("reused").and_then(JsonValue::as_u64) != Some(0);
                causes
                    .into_iter()
                    .map({
                        let type_name = type_name.clone();
                        move |cause| {
                            (
                                entity,
                                type_name.clone(),
                                "rendered".to_owned(),
                                Some(cause),
                            )
                        }
                    })
                    .chain(reused.then(|| (entity, type_name, "reused".to_owned(), None)))
            })
            .collect()
    }

    /// Entities of the probe regions that rendered, and why.
    fn rendered_regions(&self) -> Vec<(u64, String)> {
        self.draws()
            .into_iter()
            .filter(|(_, type_name, outcome, _)| {
                type_name.ends_with(REGION_TYPE) && outcome == "rendered"
            })
            .map(|(entity, _, _, cause)| (entity, cause.unwrap_or_default()))
            .collect()
    }

    /// Entities of the probe regions that replayed from cache.
    fn reused_regions(&self) -> Vec<u64> {
        self.draws()
            .into_iter()
            .filter(|(_, type_name, outcome, _)| {
                type_name.ends_with(REGION_TYPE) && outcome == "reused"
            })
            .map(|(entity, ..)| entity)
            .collect()
    }

    fn refreshed(&self) -> bool {
        self.draws()
            .iter()
            .any(|(.., cause)| cause.as_deref() == Some("refresh"))
    }

    fn summary(&self) -> String {
        self.0
            .get("summary")
            .map(ToString::to_string)
            .unwrap_or_default()
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

/// Wait until exactly the fixture is discoverable through the private endpoint
/// directory, so the measurement is against one application and one target.
async fn wait_for_the_fixture(server: &mut Server) -> Result<(), String> {
    support::wait_for_fixture(server, &[LEFT, RIGHT, PARKING], DISCOVERY_DEADLINE).await
}

impl support::FixtureClient for Server {
    async fn call_json(&mut self, tool: &str, arguments: JsonValue) -> Result<JsonValue, String> {
        Server::call_json(self, tool, arguments).await
    }
}

/// Run `scenario` against a fresh fixture and server.
async fn with_fixture<F>(scenario: F) -> Result<(), String>
where
    F: AsyncFnOnce(&mut Server, &Fixture) -> Result<(), String>,
{
    let directory = TempDir::new()
        .map_err(|error| format!("could not create a temporary directory: {error}"))?;
    let endpoints = directory.path().join("endpoints");
    std::fs::create_dir_all(&endpoints)
        .map_err(|error| format!("could not create the endpoint directory: {error}"))?;
    let fixture = Fixture::start(&endpoints)?;
    let mut server = Server::start(&endpoints, &directory.path().join("artifacts"))?;

    let outcome = async {
        server.initialize().await?;
        scenario(&mut server, &fixture).await
    }
    .await;

    let outcome = outcome.map_err(|error| support::fixture_failure(&fixture.log, &error));
    server.stop().await;
    fixture.stop().await;
    outcome
}

/// Check that the report covers a hover that rendered one probe region, for the
/// reason a hover gives, and replayed the other; return the rendered entity.
fn one_region_rendered(report: &Report, hover: &str) -> Result<u64, String> {
    let rendered = report.rendered_regions();
    let reused = report.reused_regions();
    let described = format!(
        "{hover}: rendered {rendered:?}, reused {reused:?}; summary {}",
        report.summary()
    );
    if report.frames().is_empty() {
        return Err(format!("{hover} drew no frame at all: {described}"));
    }
    if report.refreshed() {
        return Err(format!(
            "{hover} refreshed the window, rendering every cached view: {described}"
        ));
    }
    let [(entity, cause)] = rendered.as_slice() else {
        return Err(format!(
            "{hover} must render exactly one probe region: {described}"
        ));
    };
    if cause != "notified" {
        return Err(format!(
            "the hovered region must render because it was notified: {described}"
        ));
    }
    if reused.is_empty() || reused.contains(entity) {
        return Err(format!(
            "the other region must replay from cache and only it: {described}"
        ));
    }
    Ok(*entity)
}

/// Check each reported frame's draw time is real and the bridge's share of it
/// is inside it.
fn draw_times_are_reported(report: &Report) -> Result<(), String> {
    for frame in report.frames() {
        let field = |name: &str| frame.get(name).and_then(JsonValue::as_f64);
        let (Some(draw), Some(app), Some(bridge)) =
            (field("draw_ms"), field("app_draw_ms"), field("bridge_ms"))
        else {
            return Err(format!("a frame sample lacks its draw times: {frame}"));
        };
        if draw <= 0.0 || bridge < 0.0 || bridge > draw || (app + bridge - draw).abs() > 1e-6 {
            return Err(format!(
                "a frame's draw, application, and bridge times disagree: {frame}"
            ));
        }
    }
    Ok(())
}

#[tokio::test]
async fn an_injected_hover_renders_only_the_hovered_cached_region() -> Result<(), String> {
    if !has_a_desktop_session() {
        eprintln!("skipping: this machine has no desktop session to open a window on");
        return Ok(());
    }
    with_fixture(async |server: &mut Server, _: &Fixture| {
        let parking = server.center(PARKING).await?;
        let left = server.center(LEFT).await?;

        // Each hover starts from the parking spot: moving straight from one
        // region to the other would unhover the first, and rightly render both.
        server.pointer_move(parking).await?;
        server.call("mark_frames", json!({})).await?;
        server.pointer_move(left).await?;
        let report = server.report().await?;
        let first = one_region_rendered(&report, "hovering the left region")?;
        draw_times_are_reported(&report)?;
        if report.frames().len() != 1 {
            return Err(format!(
                "one hover must cost one frame, not {}: {}",
                report.frames().len(),
                report.summary()
            ));
        }

        server.pointer_move(parking).await?;
        server.call("mark_frames", json!({})).await?;
        server.call("hover_element", json!({ "id": RIGHT })).await?;
        let report = server.report().await?;
        let second = one_region_rendered(&report, "hovering the right region")?;
        if first == second {
            return Err("the two hovers rendered the same region".to_owned());
        }

        let stats = server.call_json("get_frame_stats", json!({})).await?;
        let draw = stats.get("draw_average_ms").and_then(JsonValue::as_f64);
        if !draw.is_some_and(|draw| draw > 0.0) {
            return Err(format!("frame stats carry no draw time: {stats}"));
        }
        Ok(())
    })
    .await
}

/// Move the operating system's cursor to a window-relative logical point of the
/// fixture's window.
#[cfg(windows)]
async fn move_os_cursor(fixture: &Fixture, point: (f64, f64)) -> Result<(), String> {
    let process = fixture
        .child
        .id()
        .ok_or_else(|| "the fixture has exited".to_owned())?;
    // Per-monitor awareness makes `ClientToScreen` and `SetCursorPos` agree in
    // physical pixels; GPUI's logical pixels scale by the window's DPI.
    let script = format!(
        r#"
$ErrorActionPreference = 'Stop'
Add-Type @'
using System;
using System.Runtime.InteropServices;
public static class GpuiMcpCursor {{
    [StructLayout(LayoutKind.Sequential)] public struct Point {{ public int X; public int Y; }}
    [DllImport("user32.dll")] public static extern IntPtr SetThreadDpiAwarenessContext(IntPtr context);
    [DllImport("user32.dll")] public static extern bool ClientToScreen(IntPtr window, ref Point point);
    [DllImport("user32.dll")] public static extern uint GetDpiForWindow(IntPtr window);
    [DllImport("user32.dll")] public static extern bool SetCursorPos(int x, int y);
}}
'@
[GpuiMcpCursor]::SetThreadDpiAwarenessContext([IntPtr](-4)) | Out-Null
$window = (Get-Process -Id {process}).MainWindowHandle
if ($window -eq [IntPtr]::Zero) {{ throw 'the fixture has no main window' }}
$scale = [GpuiMcpCursor]::GetDpiForWindow($window) / 96.0
$origin = New-Object GpuiMcpCursor+Point
if (-not [GpuiMcpCursor]::ClientToScreen($window, [ref]$origin)) {{ throw 'ClientToScreen failed' }}
$x = $origin.X + [int][Math]::Round({x} * $scale)
$y = $origin.Y + [int][Math]::Round({y} * $scale)
if (-not [GpuiMcpCursor]::SetCursorPos($x, $y)) {{ throw 'SetCursorPos failed' }}
"#,
        x = point.0,
        y = point.1,
    );
    let output = Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .output()
        .await
        .map_err(|error| format!("could not run PowerShell: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "could not move the cursor: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

/// Wait until GPUI's pointer reaches `point`, which a platform event must have
/// delivered, and until the frames it caused are drawn.
#[cfg(windows)]
async fn wait_for_os_pointer(server: &mut Server, point: (f64, f64)) -> Result<(), String> {
    let started = Instant::now();
    loop {
        let location = server.call_json("pointer_location", json!({})).await?;
        let at = |name: &str, expected: f64| {
            location
                .get(name)
                .and_then(JsonValue::as_f64)
                .is_some_and(|value| (value - expected).abs() <= 1.5)
        };
        if at("x", point.0) && at("y", point.1) {
            // A draw that follows the event lands after the pointer moved.
            tokio::time::sleep(Duration::from_millis(250)).await;
            return Ok(());
        }
        if started.elapsed() >= OS_INPUT_DEADLINE {
            return Err(format!(
                "the operating system's cursor never reached {point:?} in the fixture: {location}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "moves the real cursor; run with --ignored while nothing else needs the mouse"]
async fn an_os_cursor_hover_renders_only_the_hovered_cached_region() -> Result<(), String> {
    with_fixture(async |server: &mut Server, fixture: &Fixture| {
        let parking = server.center(PARKING).await?;
        let left = server.center(LEFT).await?;
        let right = server.center(RIGHT).await?;

        // Entering the window changes its hover status, which GPUI answers with
        // a full refresh. Enter first, away from both regions.
        move_os_cursor(fixture, parking).await?;
        wait_for_os_pointer(server, parking).await?;
        server.call("mark_frames", json!({})).await?;
        move_os_cursor(fixture, left).await?;
        wait_for_os_pointer(server, left).await?;
        let report = server.report().await?;
        let first = one_region_rendered(&report, "the cursor entering the left region")?;
        draw_times_are_reported(&report)?;
        println!("operating-system hover frames: {}", report.summary());

        move_os_cursor(fixture, parking).await?;
        wait_for_os_pointer(server, parking).await?;
        server.call("mark_frames", json!({})).await?;
        move_os_cursor(fixture, right).await?;
        wait_for_os_pointer(server, right).await?;
        let report = server.report().await?;
        let second = one_region_rendered(&report, "the cursor entering the right region")?;
        if first == second {
            return Err("the two hovers rendered the same region".to_owned());
        }
        println!("operating-system hover frames: {}", report.summary());
        Ok(())
    })
    .await
}
