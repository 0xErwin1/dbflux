# Vendored `gpui-pre`

This directory is the published `gpui-pre` crate source plus seven patches. It exists so the
schema visualizer can pan and zoom, and so agents can drive a running DBFlux window, without
DBFlux depending on a fork of Zed. The third patch makes a dropped window-bound subscription
delivery visible in the log. The fourth lets the automation bridge fill a text input by its
element id. The fifth lets a wrapper report a text input it does not render as read-only.
The sixth adds letter spacing to text styles. The seventh adjusts key dispatch and focus
visibility for the app keymap.

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
`gpui-pre` 0.3.9 only wires `TransformationMatrix` into sprite primitives, so `div` cannot
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
  input; events from the platform go through a separate internal path;
- frame cost and view-cache observation: `FrameObserver::frame_drawn` receives a
  `DrawnFrame` when `Window::draw` returns, with the draw's wall time, the part of it spent
  only because the frame was observed, and a `ViewDraw` per view (rendered or replayed, and
  the `ViewRenderCause` of a render). `FrameObserver::accessibility_frame` hands observers a
  shared `Arc<AccessibilityFrame>` instead of a borrowed frame;
- `Window::request_frame`, which schedules a frame without the view-cache invalidation of
  `Window::refresh`, and `Window::frame_pending`, which reports whether the window is
  invalidated;
- `Window::focus_observed_element` runs a node's AccessKit `Focus` listener when it has one,
  so a composite control can move focus to its inner editor, and falls back to the node's
  own focus handle otherwise. A wrapped `div` contributes its interaction provenance
  (actions, metadata, redaction) during prepaint, and text inside a redacted element no
  longer reaches ancestor labels.

  The `Focus` redirect changed one observable result with the refresh to `eb248ccf`: focusing
  gpui-component's `Input` frame by its element id now runs the frame's `Focus` listener,
  which focuses the inner editor, so `Window::has_input_handler` is true afterwards and
  `Window::replace_input_text` succeeds. Before, the frame's own focus handle took focus and
  no input handler was installed. Only the automation bridge calls
  `focus_observed_element`, so the app itself behaves as before. `bolt`'s
  `controls::input::tests` pins the new result.

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

### Text input automation

The automation bridge addresses a text input by the element id of gpui-component's `Input`
frame. That frame tracks its own focus handle, while the platform input handler belongs to the
inner editor's focus handle and is only installed when that handle is focused at paint. Focusing
the frame by id and then replacing text through the input handler therefore finds no handler.

The frame already registers an accessibility `SetValue` listener that replaces the whole text,
the path a screen reader uses. The patch adds `Window::set_observed_element_value`, which sends
`SetValue` with an `ActionData::Value` payload to the listeners of a node from the last observed
frame, addressed by element id like `Window::focus_observed_element`. It reuses the listener
dispatch of `Window::handle_a11y_action`, extracted into `dispatch_a11y_action_listeners`, and
returns whether a listener handled the action. The built-in fallbacks of `handle_a11y_action`
(click, focus, blur) are unchanged and are not run by the new method.

It also adds `Window::has_input_handler`, so the bridge can tell apart the two reasons
`Window::replace_input_text` returns `false`: no input handler at all, or a handler that
cannot provide its document range.

### Read-only accessibility

gpui-component's `Input` and `Editor` accept `readonly(true)`, which rejects typing and
pasting, but they never mark their accessibility node read-only, and they register the
`SetValue` listener on every enabled input. A read-only editor therefore looked editable to
assistive technology and to UI automation, and `Window::set_observed_element_value` replaced
its text. The node belongs to gpui-component's input frame, which DBFlux cannot configure.

The patch adds `Window::with_accessibility_read_only`. A wrapper element calls it around the
prepaint of its child, and the first accessibility node the child builds is marked read-only,
as `aria_read_only(true)` would mark it. Its descendants are unaffected, so a search field
inside a read-only editor stays editable. When calls nest, a node built inside the inner call
also satisfies the outer one. `dbflux_components::controls::ReadOnlyEditor` is that wrapper; it
delegates layout, prepaint and paint to the editor and adds no layout node.

Every read-only node of a frame, whether marked this way or with `aria_read_only`, is recorded,
and `SetValue` and `ReplaceSelectedText` requests to it run no listener. This covers
`Window::set_observed_element_value`, which then returns `false`, and requests from an
assistive technology through `Window::handle_a11y_action`.

### Letter spacing

The design system sets its uppercase section labels with tracking (about 0.14em), and
upstream `TextStyle` has no letter spacing. The patch adds `TextStyle::letter_spacing`
(`Pixels`, zero by default) and the `Styled::letter_spacing` builder, so it cascades like
any other text style.

The text element passes the value to the new `WindowTextSystem::shape_text_with_letter_spacing`;
`shape_text` delegates to it with zero, so every existing caller is unchanged. Spacing is
applied after shaping: the wrapped-line cache copies the shaped line, moves each glyph right
by the spacing times the number of characters before it and widens the line by the spacing
times the character count, like CSS. Wrap boundaries are computed from those positions, so
wrapping accounts for the spacing. The spacing is part of the wrapped-line cache key; the
unwrapped `layout_line` cache and its entries are untouched, and a zero spacing takes the
exact code path it took before.

Ellipsis truncation still measures characters without the spacing, so a spaced label that
has to truncate is cut slightly late and relies on its container's `overflow_hidden`. Text
shaped directly through `shape_line` or `shape_text` (canvas painting, the input editor)
ignores the style.

### Keyboard dispatch

The app keymap registers every shortcut as a native GPUI key binding (see
`crates/dbflux_ui_base/src/keymap/mod.rs`). Three behaviours of upstream dispatch get in the
way of that, and the patch changes each of them:

- **A focused clickable element answers Enter and Space.** GPUI turns Enter or Space on a
  focused element with click listeners into a click, but only when the key reaches the
  element's key listeners, and key bindings run before key listeners. A binding for Enter in
  an ancestor context (the keymap binds Enter in many panels) would consume the key first,
  so a focused button could no longer be pressed from the keyboard. The element now marks
  its dispatch node while it is focused and clickable (`Window::claim_activation_keys`), and
  `dispatch_key_event` skips key bindings for an unmodified Enter or Space when the focused
  node carries that mark, the way a focused button behaves in a browser. Keystroke
  interceptors still run first, and a pending key sequence is left alone.
- **An interceptor that stops a key stops every key listener.** When a keystroke
  interceptor calls `stop_propagation`, upstream still calls the first key listener on the
  focus path before it checks the flag. The workspace used to own that first listener, which
  hid the problem; without it, the code editor's Vim key listener saw keys its own
  interceptor had already consumed (Replace mode and a pending `r`). `dispatch_key_down_up_event`
  now returns at once when propagation is already stopped.
- **Focus stays visible while the pointer moves.** `Window::last_input_was_keyboard` flips
  to mouse on every pointer move, which is right for hover suppression but made keyboard
  focus rings disappear as soon as the mouse moved. `Window::keyboard_focus_visible` is set
  by a key press and cleared only by a pointer press or a touch, and the window repaints when
  it changes. `last_input_was_keyboard` and hover behave as before;
  `dbflux_components::primitives::is_keyboard_modality` reads the new flag.

The vendored crate's own test suite does not build outside Zed's tree (it reads assets from
the Zed repository), so the behaviour is covered by DBFlux tests instead:
`controls::button::tests::a_focused_button_takes_enter_before_ancestor_bindings` and
`primitives::focus_ring::tests::moving_the_pointer_keeps_keyboard_focus_visible` in
`dbflux_components`, and the Replace-mode tests in `dbflux_ui_document`'s Vim suite.

### Why vendor

Depending on either fork would put `main` back on a personal git source for the whole
dependency graph — the same shape of coupling that #639 removed when it dropped the patched
blade fork. Vendoring instead keeps every consumer on the published crate and confines the
delta to this directory.

## Provenance

- Base: the `gpui-pre` 0.3.9 crate published on crates.io (a snapshot of Zed's
  `crates/gpui` at `zed@93d7456`), trimmed of `examples/`, `docs/`, `tests/` and their
  target tables.
- Patch 1: `element-transform.patch`, the diff of
  `0xErwin1/zed@978c6b45cb966f2d02bfc11cd8c1c4696b7c3fd8` (branch
  `feature/53306-gpui-transforms`) against Zed's `main` at its merge base — 14 commits over
  seven files, rebased onto gpui-pre 0.3.9, 2532 diff lines.
- Patch 2: `frame-observer.patch`, the GPUI delta of
  `themixednuts/gpui-mcp@eb248ccf43847797b9067c47eeef1dd188dc03a6` (2026-10-09; `vendor/gpui`,
  licensed Apache-2.0 like the rest of this crate) against the Zed commit it snapshots,
  `zed@16c9aa7ea6d897a8044d9501cde1b295256722f2`, rebased onto this directory with patch 1
  already applied — nine files, 2152 diff lines. Refreshed on 2026-10-31 from `7b834c17`
  (2026-09-22, eight files, 1596 diff lines) together with `vendor/gpui-mcp`; the refresh
  added frame cost and view-cache observation, `request_frame` / `frame_pending`, the
  AccessKit `Focus` redirect in `focus_observed_element`, wrapper interaction provenance and
  redaction of text inside redacted elements, and touched `src/view.rs` for the first time.
  Patches 3 to 7 apply on top of it unchanged.
- Patch 3: `subscription-drop-log.patch`, written for DBFlux against this directory with
  patches 1 and 2 applied — three files, 168 diff lines. It has no upstream counterpart.
- Patch 4: `text-input-automation.patch`, written for DBFlux against this directory with
  patches 1 to 3 applied — one file, 105 diff lines. It has no upstream counterpart.
- Patch 5: `read-only-accessibility.patch`, written for DBFlux against this directory with
  patches 1 to 4 applied — three files, 108 diff lines. It has no upstream counterpart.
- Patch 6: `letter-spacing.patch`, written for DBFlux against this directory with
  patches 1 to 5 applied — five files, 279 diff lines. It has no upstream counterpart.
- Patch 7: `keyboard-dispatch.patch`, written for DBFlux against this directory with
  patches 1 to 6 applied — three files, 171 diff lines. It has no upstream counterpart.
- `[workspace]` is appended to `Cargo.toml` so Cargo does not expect this crate in the
  parent workspace's member list.

The patches are applied in that order, and each one is written against the tree the
previous ones produce. They were regenerated against 0.3.9 when the base moved from 0.3.5, so
`refresh.sh 0.3.9` applies all seven without a reject and reproduces `src/` exactly. All use the same path layout (`a/crates/gpui/src/...`, applied with
`-p3`).

### Manual merges

A refresh to a newer base usually needs a human edit in these places, all in `src/window.rs`.
**Re-check them after every refresh**, then regenerate each patch from the merged tree so the
next refresh starts clean again.

1. The `use crate::{...}` import list. Upstream reorders it by rustfmt, so both patches
   reject their hunk there. Add `ElementTransform` (patch 1) and `FrameBuilder`,
   `FrameCheckpoint`, `FrameObserver`, `FrameParent` (patch 2) by hand.
2. The tuple destructured from each `DeferredDraw` in `Window::prepaint_deferred_draws`.
   Patch 1 adds `transform` and `content_mask` to it and patch 2 adds `observed_parent`;
   gpui-mcp's hunk was written against a tuple without the first two, so it was merged by
   hand when patch 2 was rebased. Patch 2 now carries the merged form and applies cleanly
   on top of patch 1, but if patch 1 changes that loop, patch 2 rejects there and needs the
   same merge again.
3. `Window::draw`, where patch 2 moves the AccessKit `end_frame` before painting so frame
   observers receive the tree, sends the platform update only when the platform
   accessibility flag was active for the whole frame, and times the draw for
   `FrameObserver::frame_drawn`. Upstream edits that block often (0.3.9 added draw-phase
   profiling around it, and `draw_duration` is computed outside the `profiler` block), so
   its hunks are ported by hand.
4. `prepaint_view` in `src/view.rs`, whose cache check patch 1 extends with the transform.
   Patch 2 replaces that check with `view_cache_miss`, so the transform comparison is merged
   into it by hand.
5. The activation-key branch of `Window::dispatch_key_event` (patch 7). It calls
   `finish_dispatch_key_event`, whose signature follows upstream: 0.3.9 added
   `recognized_keystroke` and `input_preference`, which the branch passes as the other
   early returns there do.

### Deliberate deviations

The vendored delta is not byte-identical to the fork's branch in one deliberate place:
the `src/text_system/line.rs` hunk imports only `ScaledPixels` and `TransformationMatrix`,
dropping the `ShapedGlyph` and `ShapedRun` that upstream's current code no longer needs.
Without that trim the crate builds with an `unused_imports` warning, and a path dependency
is linted as first-party code, so `cargo clippy --workspace -- -D warnings` would fail.

Patch 2 leaves out three parts of gpui-mcp's GPUI delta:

- Its font-family fallback change in `src/text_system.rs`, which makes a fallback family
  keep the requested weight, style and OpenType features. It has nothing to do with
  automation and would change how text renders in every window of the app, so it does not
  ride along with a feature that is off by default. Nothing in the patch depends on it.
- Its whitespace-only cleanup of two doc comments in `src/_accessibility.rs`.
- Its view-scoped redraws in `src/elements/div.rs`: pressing and releasing an element with
  click or drag handlers, its active state, and showing a tooltip notify the view that
  painted the element instead of refreshing the window, and hiding a tooltip calls
  `request_frame`. Upstream calls it the one change that alters GPUI's behavior rather than
  adding to it. The bridge does not depend on it, and DBFlux keeps upstream gpui's redraws,
  so it stays out of a patch that only ships with the automation feature in mind.

The ported `view_cache_miss` also compares the view's `transform`, which patch 1 added to
the view cache key: a view drawn under a new element transform renders again, as it did
before the refresh, and reports `ViewRenderCause::LayoutChanged`.

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
`element-transform.patch`, `frame-observer.patch`, `subscription-drop-log.patch`,
`text-input-automation.patch`, `read-only-accessibility.patch` and then `letter-spacing.patch`.
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
