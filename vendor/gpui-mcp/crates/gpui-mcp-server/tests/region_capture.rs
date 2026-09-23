//! `screenshot_region` against `screenshot`, over the real MCP stdio surface.
//!
//! OPE-132 reported that a region crop went on returning the unfocused pixels of
//! a text field after keyboard focus moved to it, while a full-window capture
//! taken at the same moment carried the focus ring. The reproduction rule this
//! test encodes is the one that separates a stale crop from a frame that never
//! changed: drive one application through one transport, make the application
//! confirm the state moved before capturing anything, and read the oracle out of
//! the full capture rather than recomputing the server's own region arithmetic.
//!
//! Both shapes of change are exercised, because they fail differently: the
//! fixture's field draws focus as a one-pixel, high-contrast ring on its own
//! bounds, and hover as a low-contrast fill across its whole interior. A crop
//! that refreshes on layout but not on paint still passes the second alone. The
//! assertion in each case compares the same rectangle of a full window capture
//! in both capture orders. One channel level of native capture color jitter is
//! ignored; a shifted crop or a stale visual state still fails.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use base64::Engine as _;
use image::{Rgba, RgbaImage};
use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

const REPLY_TIMEOUT: Duration = Duration::from_mins(1);
const DISCOVERY_DEADLINE: Duration = Duration::from_secs(45);
const VISUAL_SETTLE_DEADLINE: Duration = Duration::from_secs(5);
/// The rectangle the fixture's focusable field occupies, grown by a margin, so a
/// focus treatment drawn just outside the element's own bounds is still inside
/// the crop and a null reading cannot be blamed on the margin.
const CROP_MARGIN: f64 = 14.0;

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
            .map_err(|error| format!("could not flush the server stdin: {error}"))
    }

    async fn request(&mut self, method: &str, params: JsonValue) -> Result<JsonValue, String> {
        let id = self.next_id;
        self.next_id += 1;
        self.send(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
            .await?;
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

    /// The decoded pixels of a tool that answers with a PNG.
    async fn call_image(&mut self, tool: &str, arguments: JsonValue) -> Result<RgbaImage, String> {
        let result = self.call(tool, arguments).await?;
        let encoded = result
            .get("content")
            .and_then(JsonValue::as_array)
            .and_then(|content| {
                content
                    .iter()
                    .find_map(|entry| entry.get("data").and_then(JsonValue::as_str))
            })
            .ok_or_else(|| format!("{tool} returned no image payload"))?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|error| format!("{tool} returned unreadable base64: {error}"))?;
        image::load_from_memory(&bytes)
            .map_err(|error| format!("{tool} returned an undecodable PNG: {error}"))
            .map(image::DynamicImage::into_rgba8)
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

fn rectangle(bounds: &JsonValue) -> Result<(f64, f64, f64, f64), String> {
    let read = |name: &str| {
        bounds
            .get(name)
            .and_then(JsonValue::as_f64)
            .ok_or_else(|| format!("element bounds carry no {name}"))
    };
    Ok((read("x")?, read("y")?, read("width")?, read("height")?))
}

/// Find where `tile` appears in `image`, allowing native color jitter.
///
/// This is the oracle for "the same rectangle": it reads the region's place in
/// the window out of the pixels instead of recomputing the mapping the server
/// used, so a mapping that crops the wrong rectangle cannot satisfy it.
fn locate(image: &RgbaImage, tile: &RgbaImage) -> Option<(u32, u32)> {
    let (width, height) = (image.width(), image.height());
    let (tile_width, tile_height) = (tile.width(), tile.height());
    if tile_width == 0 || tile_height == 0 || tile_width > width || tile_height > height {
        return None;
    }
    let samples = [
        (0, 0),
        (tile_width / 2, 0),
        (tile_width - 1, 0),
        (0, tile_height / 2),
        (tile_width / 2, tile_height / 2),
        (tile_width - 1, tile_height / 2),
        (0, tile_height - 1),
        (tile_width / 2, tile_height - 1),
        (tile_width - 1, tile_height - 1),
    ];
    (0..=height - tile_height)
        .flat_map(|top| (0..=width - tile_width).map(move |left| (left, top)))
        .find(|&(left, top)| {
            samples.iter().all(|&(x, y)| {
                colors_match(*image.get_pixel(left + x, top + y), *tile.get_pixel(x, y))
            }) && (0..tile_height).all(|y| {
                (0..tile_width).all(|x| {
                    colors_match(*image.get_pixel(left + x, top + y), *tile.get_pixel(x, y))
                })
            })
        })
}

fn sub_image(image: &RgbaImage, left: u32, top: u32, width: u32, height: u32) -> RgbaImage {
    image::imageops::crop_imm(image, left, top, width, height).to_image()
}

/// Count pixels whose color differs by more than native capture's observed
/// one-level channel jitter.
fn differing_pixels(left: &RgbaImage, right: &RgbaImage) -> usize {
    if left.dimensions() != right.dimensions() {
        return usize::MAX;
    }
    left.pixels()
        .zip(right.pixels())
        .filter(|(left, right)| !colors_match(**left, **right))
        .count()
}

fn colors_match(left: Rgba<u8>, right: Rgba<u8>) -> bool {
    left.0
        .iter()
        .zip(right.0.iter())
        .all(|(left, right)| left.abs_diff(*right) <= 1)
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
async fn a_region_crop_carries_the_focus_ring_the_full_window_capture_carries() -> Result<(), String>
{
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
                "clientInfo": { "name": "gpui-mcp-region-capture-test", "version": "0.0.0" },
            }),
        )
        .await?;
    server
        .send(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized", "params": {} }))
        .await?;
    wait_for_the_fixture(server).await?;

    let tree = server.call_json("get_ui_tree", json!({})).await?;
    let bounds = tree
        .get("nodes")
        .and_then(|nodes| nodes.get("search"))
        .and_then(|node| node.get("bounds"))
        .ok_or_else(|| "the fixture published no bounds for its search field".to_owned())?;
    let (x, y, width, height) = rectangle(bounds)?;
    let region = json!({
        "x": x - CROP_MARGIN,
        "y": y - CROP_MARGIN,
        "width": width + CROP_MARGIN * 2.0,
        "height": height + CROP_MARGIN * 2.0,
    });

    let away = (0.0, 0.0);
    return_to_rest(server, away).await?;
    let parked_region = stable_region(server, &region).await?;
    let anchor_since = Instant::now();
    let anchor = loop {
        let parked_window = server.call_image("screenshot", json!({})).await?;
        if let Some(anchor) = locate(&parked_window, &parked_region) {
            break anchor;
        }
        if anchor_since.elapsed() >= VISUAL_SETTLE_DEADLINE {
            return Err("the resting crop did not appear in the full window".to_owned());
        }
    };

    // Two shapes of change, because they fail differently and one alone is not
    // insurance: a focus ring is a thin, high-contrast edge on the rectangle's
    // own bounds, and a hover fill is a large, low-contrast area inside it. A
    // crop that refreshes on layout but not on paint still passes the second.
    // The fill changes thousands of pixels while the focus ring changes tens.
    // This keeps a late frame from the previous phase from satisfying the gate.
    for (change, tool, minimum_changed_pixels) in [
        ("the focus ring", "focus_element", 20),
        ("the hover fill", "hover_element", 1_000),
    ] {
        return_to_rest(server, away).await?;
        let resting_region = stable_region(server, &region).await?;
        server.call(tool, json!({ "id": "search" })).await?;
        if tool == "focus_element" {
            let state = server
                .call_json("get_element_state", json!({ "id": "search" }))
                .await?;
            assert_eq!(
                state.get("focused").and_then(JsonValue::as_bool),
                Some(true),
                "the fixture must agree the field is focused before anything is captured"
            );
        }

        for region_first in [true, false] {
            compare_changed_capture_pair(
                server,
                &region,
                &resting_region,
                anchor,
                change,
                minimum_changed_pixels,
                region_first,
            )
            .await?;
        }
    }

    Ok(())
}

async fn stable_region(server: &mut Server, region: &JsonValue) -> Result<RgbaImage, String> {
    let started = Instant::now();
    let mut previous = server
        .call_image("screenshot_region", region.clone())
        .await?;
    let mut repeats = 0;
    loop {
        let current = server
            .call_image("screenshot_region", region.clone())
            .await?;
        if differing_pixels(&previous, &current) == 0 {
            repeats += 1;
            if repeats == 2 {
                return Ok(current);
            }
        } else {
            repeats = 0;
        }
        if started.elapsed() >= VISUAL_SETTLE_DEADLINE {
            return Err("the resting crop did not stabilize".to_owned());
        }
        previous = current;
    }
}

async fn compare_changed_capture_pair(
    server: &mut Server,
    region: &JsonValue,
    parked_region: &RgbaImage,
    anchor: (u32, u32),
    change: &str,
    minimum_changed_pixels: usize,
    region_first: bool,
) -> Result<(), String> {
    // Native capture can still return an older compositor frame after GPUI
    // confirms focus or hover. Compare only once both capture paths show the change.
    let changed_since = Instant::now();
    let (changed_region, expected) = loop {
        let (crop, window) = if region_first {
            let crop = server
                .call_image("screenshot_region", region.clone())
                .await?;
            (crop, server.call_image("screenshot", json!({})).await?)
        } else {
            let window = server.call_image("screenshot", json!({})).await?;
            (
                server
                    .call_image("screenshot_region", region.clone())
                    .await?,
                window,
            )
        };
        let expected = sub_image(&window, anchor.0, anchor.1, crop.width(), crop.height());
        if differing_pixels(parked_region, &crop) >= minimum_changed_pixels
            && differing_pixels(parked_region, &expected) >= minimum_changed_pixels
        {
            break (crop, expected);
        }
        if changed_since.elapsed() >= VISUAL_SETTLE_DEADLINE {
            return Err(format!(
                "both native captures did not show {change} before the deadline: crop changed {} pixels, full-window rectangle changed {} pixels",
                differing_pixels(parked_region, &crop),
                differing_pixels(parked_region, &expected),
            ));
        }
    };

    assert_eq!(
        differing_pixels(&changed_region, &expected),
        0,
        "with {change} applied the region crop must match the same rectangle of a \
         full window capture within one color level (region captured {} the window)",
        if region_first { "before" } else { "after" }
    );
    Ok(())
}

/// Put the measured field back in its resting state: the pointer parked off it
/// and the keyboard focus on the fixture's other field.
async fn return_to_rest(server: &mut Server, away: (f64, f64)) -> Result<(), String> {
    server
        .call("pointer_move", json!({ "x": away.0, "y": away.1 }))
        .await?;
    server
        .call("focus_element", json!({ "id": "filter" }))
        .await?;
    Ok(())
}
