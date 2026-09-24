# Vendored `gpui-mcp`

This directory holds four crates from
[themixednuts/gpui-mcp](https://github.com/themixednuts/gpui-mcp), unmodified except for
their manifests. Together they let an agent drive a running DBFlux window: the bridge
reads each rendered frame of a GPUI window, injects synthetic input and answers requests
over an owner-only local socket, and the MCP server exposes that to agents and captures
screenshots. `docs/UI_AUTOMATION.md` describes how to use them.

- `crates/gpui-mcp`: the bridge installed into an application's windows. DBFlux links it
  only with the `ui-automation` feature.
- `crates/gpui-mcp-protocol`: the authenticated wire protocol and endpoint discovery shared
  by the bridge and its clients.
- `crates/gpui-mcp-server`: the MCP stdio server, binary `gpui-mcp`. A development tool
  that agents start; DBFlux never links it.
- `crates/gpui-mcp-capture`: native window capture (X11 on Linux, CoreGraphics on macOS,
  Windows Graphics Capture on Windows) used by the server for screenshots and video.

Upstream's `gpui-mcp-html` crate and its `examples/` are not vendored. DBFlux does not
use the HTML live-document preview, and the examples are separate GPUI applications.

## Why vendor

The bridge is written against the frame-observer hooks that `vendor/gpui-pre` carries as
`frame-observer.patch` (see `vendor/gpui-pre/VENDOR.md`). No published crate provides it,
and a git dependency on the upstream repository would put `main` on a personal git source
and pull in its own patched copy of Zed's gpui. Vendoring builds the crates against
DBFlux's own `gpui` (`gpui-pre` 0.3.5 with both patches applied), so the app links one
gpui. The server and capture crates come from the same commit so the server speaks the
bridge's exact protocol version.

## Provenance

- Repository: `https://github.com/themixednuts/gpui-mcp`
- Commit: `7b834c1705cb3353650dfc681f29eaa29e4cf308` (2026-09-22, "Refresh vendored GPUI and
  harden frame observation"). `vendor/gpui-pre/frame-observer.patch` is taken from the same
  commit, so the bridge and the gpui hooks it calls come from one upstream snapshot.
- License: Apache-2.0. `LICENSE` is upstream's license file, copied verbatim.
- Contents: `crates/gpui-mcp/`, `crates/gpui-mcp-protocol/`, `crates/gpui-mcp-server/` and
  `crates/gpui-mcp-capture/` as they are at that commit, with `dbflux-port.patch`,
  `text-input-automation.patch`, `screenshot-freshness.patch` and then
  `text-input-read-only-focus.patch` applied. `refresh.sh` rebuilds exactly this tree.

## The DBFlux delta

Four patches, applied in this order. `dbflux-port.patch` changes only the four `Cargo.toml`
files. `text-input-automation.patch`, `screenshot-freshness.patch` and
`text-input-read-only-focus.patch` are the only changes to Rust source and are described in
[Text input automation](#text-input-automation),
[Screenshot freshness](#screenshot-freshness) and
[Read-only state and text input focus](#read-only-state-and-text-input-focus) below.

- Package metadata. Upstream inherits `version`, `edition`, `license`, `repository` and
  `homepage` from its own workspace root, which is not vendored. Inheriting from DBFlux's
  root instead would silently relabel the crates as version `0.8.0-dev.0` under
  `MIT OR Apache-2.0`, so the upstream values are written out. `publish = false` is added,
  as in every DBFlux crate.
- `rust-version`: upstream declares 1.96; DBFlux pins 1.95.0 in `rust-toolchain.toml`, and
  all four crates build and pass their tests on it, so the patch declares 1.95.
- Dependencies. Upstream's `x.workspace = true` entries are replaced with the requirement
  and features its workspace root declares, copied verbatim.
- `gpui` stays `gpui.workspace = true` in `gpui-mcp`, as upstream wrote it. In DBFlux that
  resolves to `gpui = { package = "gpui-pre", version = "=0.3.5" }` from the root
  `Cargo.toml`, which `[patch.crates-io]` points at `vendor/gpui-pre`. No API fixups were
  needed: the frame-observer patch gives gpui-pre 0.3.5 the API this commit expects.
- `gpui-mcp-server` sets `autotests = false` and declares only `tests/cache_hints.rs`.
  Its other two integration tests, `tests/disabled_state.rs` and `tests/region_capture.rs`,
  launch upstream's `examples/demo` application, which is not vendored (it depends on a
  newer gpui's separate `gpui_platform` crate). They stay in the tree unbuilt. Without this,
  both fail on any machine with a desktop session; on a headless one they skip themselves.
- Lints: see below.

### Text input automation

`text-input-automation.patch` was written for DBFlux against this directory with
`dbflux-port.patch` applied, and has no upstream counterpart. It depends on
`Window::set_observed_element_value` and `Window::has_input_handler`, which
`vendor/gpui-pre/text-input-automation.patch` adds. Five files, 428 diff lines.

A text input rendered by gpui-component is addressed by the element id of its `Input` frame,
whose focus handle is not the one the platform input handler belongs to. Upstream's
`set_text` focuses the node and replaces text through the active input handler, so on such an
input it finds no handler; and the frame has no click listener, so `click_element` refuses it.

- `gpui-mcp-protocol`: a new `Operation::SetValue { node_id, value }`.
- `gpui-mcp`: `SetValue` calls `Window::set_observed_element_value`, which runs the node's
  accessibility `SetValue` listener, and fails with `Unsupported` when the node has none. It
  is validated like `Focus` (node id) and `TypeText` (text size). `ReplaceText` now reports
  "focused element has no active text input handler" when there is no handler, and keeps
  the document-range message for a handler that cannot expose its range.
- `gpui-mcp-server`: `set_text`, and `set_value` on text inputs, send `SetValue` when the
  node advertises the `SetValue` action and settle a frame, with no focus step. Other
  editable nodes keep upstream's focus-then-`ReplaceText` path. `click_element` and
  `double_click_element` also accept a node with the `SetText` action and click it, which
  focuses the editor for `type_text`. Other nodes keep the `Click` check.

### Screenshot freshness

`screenshot-freshness.patch` was written for DBFlux against this directory with the first two
patches applied, and has no upstream counterpart. It needs no change to `vendor/gpui-pre`.
Six files, 1078 diff lines.

Upstream settles a screenshot on a frame that has finished root paint. GPUI reports that from
inside `Window::draw`, before `Window::present` hands the frame to the platform window, and on
Linux the capture was a single X11 `GetImage`, so a screenshot taken right after an action
could show the previous frame.

- `gpui-mcp-protocol`: `Operation::WaitForFrame` gains `presented`. It defaults to `false` and
  is serialized only when `true`, so a request without it keeps the root-paint behavior and its
  wire shape.
- `gpui-mcp`: `Refresh` registers a GPUI next-frame callback that registers a second one. GPUI
  runs next-frame callbacks at the start of a frame request, before that request draws and
  presents, so the second callback runs at the start of the request after the one that drew
  the refreshed frame, once that frame's `present` has returned. It advances a presented-frame
  token, and `WaitForFrame` with `presented` waits on that token instead of on root paint.
  Only `Refresh` advances the token.
- `gpui-mcp-server`: the settle step used before a screenshot and after input (`Refresh` then
  `WaitForFrame`, twice) waits with `presented`. A new `wait_for_idle` tool checks every
  250 ms and succeeds once the semantic tree generation has not changed and the window has
  drawn at most one frame over two consecutive checks, so the 500 ms caret blink of a focused
  input still counts as idle. Each check waits for a newer tree with `WaitForTree`, so an
  unchanged tree is not transferred. `timeout_ms` defaults to 5000 and must be between 500
  and 30000.
- `gpui-mcp-capture`: on Linux a screenshot samples the window every 16 ms until two
  consecutive captures are identical, within the settle deadline (one second by default, plus
  the measured cost of one readback, as for the Windows freshness samples). At the deadline it
  returns the newest sample instead of failing, so a blinking caret or a spinner does not make
  a screenshot fail. Windows and macOS keep upstream's behavior, and video recording does not
  use this path.

### Read-only state and text input focus

`text-input-read-only-focus.patch` was written for DBFlux against this directory with the first
three patches applied, and has no upstream counterpart. It reads the read-only state that
`aria_read_only` and `vendor/gpui-pre/read-only-accessibility.patch` set on AccessKit nodes.
Five files, 435 diff lines.

- `gpui-mcp-protocol`: `NodeState` gains `read_only`. It defaults to `false` and is serialized
  only when `true`, so a tree without it keeps its wire shape.
- `gpui-mcp`: the observer reports `read_only` from the node's AccessKit read-only state. The
  node's actions are unchanged, so a read-only text input still advertises `SetText` and can
  still be clicked and focused to select and copy its text.
- `gpui-mcp-server`: `set_text`, and `set_value` on text inputs, fail with "element ... is
  read-only and does not accept a new value" on a node that reports `read_only`, before any
  operation reaches the bridge. The app refuses the same write on its own (see
  `vendor/gpui-pre/VENDOR.md`), so the bridge's `SetValue` now reports "semantic node is
  read-only or has no accessibility value handler" when `set_observed_element_value` fails.
- `gpui-mcp-server`: `focus_element` on a node with the `SetText` action clicks it instead of
  sending `Focus`, the way `click_element` does, and settles after the click. `Focus` on a
  gpui-component input focuses its frame, whose focus handle does not own the text input
  handler, so `type_text` failed after it. Other nodes keep `Focus`.
- `gpui-mcp-server`: `click_element`, `double_click_element` and `focus_element` click a node
  with the `SetText` action at a third of its width from the left edge, at most 40 logical
  pixels, and at its vertical center, instead of at its bounds center. In a narrow input the
  center can land on a trailing clear or show-password button. The 40 pixel cap clears the
  left padding and a leading icon, which belong to the input frame: a click there focuses the
  frame, not the editor. Other nodes are still clicked at their center.

### Dependencies the server and capture crates add

`gpui-mcp` and `gpui-mcp-protocol` only add `directories` 6.0.0 to `Cargo.lock`. The server
and capture crates add the MCP stack and the capture and video stack: `rmcp` 3.2 (DBFlux's
own `dbflux_mcp_server` stays on `rmcp` 2.1), `sysinfo` 0.37 (next to 0.31), `clap`,
`xcap` 0.9.8 with `pipewire`, `libwayshot-xcap`, `gbm`, `drm` and `xcb` on Linux,
`windows-capture` on Windows, and `openh264` plus `mp4` for recordings. Cargo resolves the
two `rmcp` and two `sysinfo` majors side by side; `deny.toml` keeps
`multiple-versions = "warn"`.

None of this reaches the application. `crates/dbflux` does not depend on either crate,
with or without `ui-automation`, so `cargo build -p dbflux` compiles none of it:

```sh
cargo tree -p dbflux -e normal --features ui-automation | rg 'rmcp v3|xcap|openh264|mp4 v|pipewire|sysinfo v0.37'
```

prints nothing. One side effect does reach the application: `xcap` requires `zbus` 5.17
or later, so adding it moved the shared `zbus` from 5.15.0 to 5.19.0, which gpui-pre's
Linux backend (AccessKit, ashpd, oo7) also uses. It is a semver-compatible minor update.

- `rmcp` is held at 3.2.0 in `Cargo.lock`, the version upstream's own lock file uses.
  `rmcp-macros` 3.4 rejects the server's `#[tool_router]` blocks ("found no `#[tool]` fn
  in this impl block"), so `cargo update` must not move `rmcp` or `rmcp-macros` past 3.2
  until the crates are refreshed. `.github/dependabot.yml` ignores 3.3 and later for both.
- `openh264` builds Cisco's OpenH264 encoder from the C++ source bundled in the
  `openh264-sys2` crate (its default `source` feature) with the system C++ compiler, and
  assembles its x86 kernels with `nasm` when one is on `PATH` (without it, or with
  `OPENH264_NO_ASM` set, the build falls back to plain C++). Nothing is downloaded at
  build time or at run time, and the `libloading` mode that loads Cisco's prebuilt binary
  is not enabled. The source is BSD-2-Clause. Cisco's offer to cover H.264 patent royalties applies only to its own
  prebuilt binary, not to encoders built from source, which is one more reason the server
  must stay a development tool that is never distributed.

### System libraries

On Linux the capture crate needs, at build time: `libpipewire-0.3` and `libspa-0.2`
(through `pipewire-sys`, whose bindings are generated with bindgen and therefore need
libclang), `libgbm`, EGL (`egl.pc`, probed by `khronos-egl`) and `libxcb` with the RandR
extension. The Nix dev shells add them through `automationBuildInputs` and
`automationNativeBuildInputs` in `default.nix` (`pipewire`, `libgbm`, `libGL` and
`rustPlatform.bindgenHook`); the Nix packages build `-p dbflux` and do not need them. The
Linux clippy and test jobs in CI install `libpipewire-0.3-dev`, `libgbm-dev`, `libegl-dev`,
`libxcb-randr0-dev` and `libclang-dev`. macOS and Windows use system frameworks only.

## Lints and AGENTS.md

All four crates are workspace members, so `cargo clippy --workspace -- -D warnings`,
`cargo fmt --all --check`, `cargo test --workspace` and `cargo nextest run --workspace` all
cover them, and they are linted as first-party code. `default-members` stays
`crates/dbflux`, so a bare `cargo build` does not build them.

`gpui-mcp-protocol` and `gpui-mcp-capture` keep `[lints] workspace = true` and pass
DBFlux's full lint set.

`gpui-mcp` and `gpui-mcp-server` need one exemption, `clippy::indexing_slicing`. Cargo
cannot relax one inherited lint, so their manifests restate the root `[workspace.lints]`
without `indexing_slicing`. **Keep those two tables in step with the root when the
workspace lints change.** Each site is bounded by the surrounding code, and rewriting them
to `.get()` would be a refactor of upstream code for no behavior change:

- `gpui-mcp`: five sites in library code (`src/registry.rs` lines 483, 584 and 621,
  `src/service.rs` lines 1457 and 1458) and more in its tests: a slice up to the current
  enumerate index, a position recorded from the same vector, a key taken from the map's own
  order list, a nibble indexing a 16-entry table.
- `gpui-mcp-server`: three sites in the binary (`src/recording.rs` lines 514, 576 and 616)
  and thirteen in its unit tests: a 3-byte slice of a 4-byte `chunks_exact` chunk, an
  Annex B NAL slice between two start-code offsets found in the same buffer, and a byte
  index guarded by a `len() == 4` check.

Upstream code is exempt from the AGENTS.md rule against `let _ =` on fallible
expressions, as `vendor/gpui-pre` is. The sites stay as upstream wrote them so the vendored
source remains a byte-identical copy. DBFlux-authored code, including the code that
installs the bridge, still follows AGENTS.md.

- `gpui-mcp`, `src/service.rs`: nine sites. Six discard a fallible result during
  best-effort cleanup (removing the endpoint or temporary descriptor file after a startup
  error, removing the owned descriptor, joining the network thread on shutdown), when
  replying to a caller that may have gone, or when reading the peer PID on Windows; three
  silence parameters unused on the current platform.
- `gpui-mcp-server`, `src/client.rs`: five sites. Line 438 removes the descriptor of a
  process that no longer exists during discovery; lines 558, 561, 602 and 1053 silence
  parameters unused on the current platform. Its integration tests have five more, each
  killing a child process during test cleanup.
- `gpui-mcp-capture`, `src/lib.rs`: two sites. Line 486 (Windows only) drops a failed
  `try_send` of a capture-closed notice to a receiver that may be gone; line 697 silences a
  parameter unused outside Windows and Linux.

## CI

- `cargo fmt --all -- --check`: the upstream source already matches DBFlux's
  `rustfmt.toml` (both use edition 2024 and `max_width = 100`).
- `cargo machete`: no unused dependencies reported.
- `cargo deny check licenses bans sources`: every crate the vendoring adds passes the
  license allow list in `deny.toml` (`openh264` and `openh264-sys2` are BSD-2-Clause). The
  only license findings are pre-existing ones (`dbflux_js` unlicensed, `libbz2-rs-sys`
  under `bzip2-1.0.6`). No ban or source finding involves the vendored crates.
- The Linux clippy and test jobs were run locally. The macOS and Windows clippy and check
  jobs compile cfg-gated code that a Linux run does not: `src/native_window.rs` (AppKit
  window number, an `unsafe` block under a local `#[allow(unsafe_code)]`) and
  `src/service.rs` (named-pipe security descriptor) in `gpui-mcp`, and the CoreGraphics and
  Windows Graphics Capture paths in `gpui-mcp-capture`.

## Refreshing

```sh
vendor/gpui-mcp/refresh.sh <full commit SHA>
```

The script downloads that commit, rebuilds `crates/` and `LICENSE` from it and re-applies
`dbflux-port.patch`, `text-input-automation.patch`, `screenshot-freshness.patch` and then
`text-input-read-only-focus.patch`, leaving a `.rej` file next to any hunk that no longer
applies. Refresh `vendor/gpui-pre/frame-observer.patch` from the same commit first (see
`vendor/gpui-pre/VENDOR.md`); the bridge calls the hooks that patch adds.

Afterwards, update the commit in this file and re-check:

- new or removed dependencies in upstream's crate manifests, and new entries in its
  workspace `[workspace.dependencies]` that the patch has to spell out;
- the `rmcp` version in upstream's `Cargo.lock`: hold DBFlux's lock at the same version
  and update the `rmcp` entries in `.github/dependabot.yml`;
- `Cargo.lock`: `cargo tree -p dbflux` must still show no `rmcp` 3, capture or video
  crates (the command above);
- new integration tests in `gpui-mcp-server/tests/` that need upstream's demo application;
- the `indexing_slicing` sites and the `let _ =` sites listed above;
- `rust-version` against `rust-toolchain.toml`.

Then run:

```sh
cargo nextest run -p gpui-mcp -p gpui-mcp-protocol -p gpui-mcp-server -p gpui-mcp-capture
cargo clippy --workspace -- -D warnings
cargo fmt --all -- --check
cargo machete
```
