# Vendored `gpui-pre`

This directory is the published `gpui-pre` crate source plus three patches. It exists so the
schema visualizer can pan and zoom, and so agents can drive a running DBFlux window, without
DBFlux depending on a fork of Zed. The third patch makes a dropped window-bound subscription
delivery visible in the log.

## Why

### Element transform

`crates/dbflux_ui_document/src/schema_viz/mod.rs` renders the whole diagram in graph
coordinates and applies a single parent transform for pan/zoom:

```rust
let diagram_transform = ElementTransform::default()
    .translate(pan)
    .scale(size(zoom, zoom));
```

`ElementTransform` is a visual parent transform whose hit testing, culling and glyph
rasterization stay coherent with the transformed subtree. Upstream gpui does not have it:
`gpui-pre` 0.3.5 only wires `TransformationMatrix` into sprite primitives, so `div` cannot
be transformed and zooming text would either not render or scale a low-resolution glyph
atlas. The feature is proposed upstream in
[zed-industries/zed#53306](https://github.com/zed-industries/zed/discussions/53306) and
implemented in the branch `feature/53306-gpui-transforms` of `0xErwin1/zed`.

### Frame observer

UI automation lets an agent read the rendered element tree of a DBFlux window, click, type
and dispatch actions the way a browser automation tool drives a page. It is gated behind a
non-default feature in DBFlux, so release builds do not expose it. The in-process bridge
reads each completed frame through GPUI's AccessKit tree and injects synthetic GPUI events,
which needs hooks upstream gpui does not have:

- read-only observation of every completed frame (`FrameObserver`, `Window::observe_frames`)
  with stable element paths, frame-unique node identities, bounds and text provenance, and
  `Window::focus_observed_element` to focus a node by that identity;
- programmatic text entry through the active input handler (`Window::insert_input_text`,
  `Window::replace_input_text`, backed by `replace_all_text` on the platform input handler)
  and `Window::is_prepainting`;
- the `Element::frame_node` and `Element::frame_text` hooks, implemented by `div` and the
  text elements, for details AccessKit does not model;
- the `aria_disabled`, `aria_hidden` and `aria_read_only` builders, which forward to the
  AccessKit node states, and the `frame_metadata`, `frame_action` and `frame_redacted`
  builders; redacted text and values are withheld from observers;
- a `div` with click listeners reports `Role::Button`, and an id-bearing hidden `div` with
  no other role reports `Role::Group`;
- pointer ownership, so a stale native mouse position does not cancel a programmatic hover
  before the physical mouse moves. `Window::dispatch_event` is treated as programmatic
  input; events from the platform go through a separate internal path.

These come from [themixednuts/gpui-mcp](https://github.com/themixednuts/gpui-mcp), which
carries them as a patched copy of Zed's `crates/gpui` and lists them for removal as soon as
upstream gains an equivalent API.

### Subscription drop log

`Context::subscribe_in`, `Context::observe_in` and `Context::defer_in` run their callback
through `App::with_window`, and `Window::subscribe` and `Window::observe` through a window
update. Upstream discards the failure of either: the `_in` variants still report the
subscription as alive, and the `Window` variants silently remove it. Nothing is logged, so a
callback that never ran leaves no trace.

The patch logs a `log::warn!` naming the event, emitter and subscriber types when such a
delivery fails while its window is still open, that is, while the window is on
`App::window_update_stack`. The other failures are normal teardown and stay silent: the
window has been closed, or the entity no longer has a window. Return values and delivery
order are unchanged.

Effects are flushed only after a window update has put its window back, so this case is not
expected in normal operation. The warning exists so that, if it does happen, the dropped
callback shows up instead of being guessed at.

### Why vendor

Depending on either fork would put `main` back on a personal git source for the whole
dependency graph — the same shape of coupling that #639 removed when it dropped the patched
blade fork. Vendoring instead keeps every consumer on the published crate and confines the
delta to this directory.

## Provenance

- Base: the `gpui-pre` 0.3.5 crate published on crates.io (a snapshot of Zed's
  `crates/gpui` at `zed@d89e9c2`), trimmed of `examples/`, `docs/`, `tests/` and their
  target tables.
- Patch 1: `element-transform.patch`, the diff of
  `0xErwin1/zed@978c6b45cb966f2d02bfc11cd8c1c4696b7c3fd8` (branch
  `feature/53306-gpui-transforms`) against Zed's `main` at its merge base — 14 commits over
  seven files, 2533 diff lines.
- Patch 2: `frame-observer.patch`, the GPUI delta of
  `themixednuts/gpui-mcp@7b834c1705cb3353650dfc681f29eaa29e4cf308` (`vendor/gpui`, licensed
  Apache-2.0 like the rest of this crate) against the Zed commit it snapshots,
  `zed@16c9aa7ea6d897a8044d9501cde1b295256722f2`, rebased onto this directory with patch 1
  already applied — eight files, 1585 diff lines.
- Patch 3: `subscription-drop-log.patch`, written for DBFlux against this directory with
  patches 1 and 2 applied — three files, 166 diff lines. It has no upstream counterpart.
- `[workspace]` is appended to `Cargo.toml` so Cargo does not expect this crate in the
  parent workspace's member list.

The patches are applied in that order, and each one is written against the tree the
previous ones produce. All use the same path layout (`a/crates/gpui/src/...`, applied with
`-p3`).

### Manual merges

A refresh needs a human edit in two places, both in `src/window.rs`. **Re-check them after
every refresh.**

1. The `use crate::{...}` import list. Upstream reorders it by rustfmt, so both patches
   reject their hunk there. Add `ElementTransform` (patch 1) and `FrameBuilder`,
   `FrameCheckpoint`, `FrameObserver`, `FrameParent` (patch 2) by hand.
2. The tuple destructured from each `DeferredDraw` in `Window::prepaint_deferred_draws`.
   Patch 1 adds `transform` and `content_mask` to it and patch 2 adds `observed_parent`;
   gpui-mcp's hunk was written against a tuple without the first two, so it was merged by
   hand when patch 2 was rebased. Patch 2 now carries the merged form and applies cleanly
   on top of patch 1, but if patch 1 changes that loop, patch 2 rejects there and needs the
   same merge again.

### Deliberate deviations

The vendored delta is not byte-identical to the fork's branch in one deliberate place:
the `src/text_system/line.rs` hunk imports only `ScaledPixels` and `TransformationMatrix`,
dropping the `ShapedGlyph` and `ShapedRun` that upstream's current code no longer needs.
Without that trim the crate builds with an `unused_imports` warning, and a path dependency
is linted as first-party code, so `cargo clippy --workspace -- -D warnings` would fail.

Patch 2 leaves out two parts of gpui-mcp's GPUI delta:

- Its font-family fallback change in `src/text_system.rs`, which makes a fallback family
  keep the requested weight, style and OpenType features. It has nothing to do with
  automation and would change how text renders in every window of the app, so it does not
  ride along with a feature that is off by default. Nothing in the patch depends on it.
- Its whitespace-only cleanup of two doc comments in `src/_accessibility.rs`.

The manifest also carries a `[package.metadata.cargo-machete]` exemption for `tracing`,
which the published crate does not declare: CI runs `cargo machete` over the whole
directory tree, so it analyses this crate too, and upstream only reaches `tracing` through
a cfg'd path the heuristic does not follow. Upstream records the same crate in its
`[package.metadata.cargo-shear]` list for the same reason. `refresh.sh` appends the block,
so a refresh reproduces it.

## Refreshing

```sh
vendor/gpui-pre/refresh.sh 0.3.6   # new upstream version
```

The script downloads the published crate, rebuilds this directory from it and re-applies
`element-transform.patch`, `frame-observer.patch` and then `subscription-drop-log.patch`.
It leaves a
`<file>.<patch>.rej` file behind for any hunk that no longer applies, for example
`src/window.rs.frame-observer.rej`; resolve them by reading the rejected hunk and porting
it (see [Manual merges](#manual-merges)), then delete the `.rej`.

Afterwards, update the `[patch.crates-io]` version constraint in the root `Cargo.toml`
(`gpui = { package = "gpui-pre", version = "=0.3.6" }`, `gpui-pre-platform`,
`gpui-component` as needed) and run:

```sh
cargo check -p gpui-pre --manifest-path vendor/gpui-pre/Cargo.toml   # patches still compile
cargo nextest run -p dbflux_schema_viz                               # layout is unchanged
cargo check --workspace
```

When one of the features lands in a published `gpui-pre`, drop its patch from the list in
`refresh.sh` and from this file. When both have landed, delete this directory and the
`[patch.crates-io]` entry with it: neither the schema visualizer nor the automation bridge
needs a change.
