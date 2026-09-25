# Vendored `gpui-base`

This directory starts from the complete published `gpui-base` 0.6.1 crate. DBF-279 carries local cursor and rectangular-selection patches.

- Upstream: [`gpui-base` 0.6.1 on crates.io](https://crates.io/crates/gpui-base/0.6.1), from the crates.io registry archive `gpui-base-0.6.1.crate`.
- License: Apache-2.0; see `LICENSE-APACHE` and the published `Cargo.toml`.
- Archive SHA-256: `9d45dcaaeac889bf1e7757db1beb26c9043c8ea3156651facc11c6be56bb6722` (the registry checksum recorded in the original root `Cargo.lock`).

## Local patch list

- `src/input/base/mod.rs`: public `InputCursorShape` (`Bar` / `Block`).
- `src/input/mod.rs`: re-export the cursor shape for application use.
- `src/input/base/state.rs`: per-input cursor shape, Bar default, getter and notifying setter; public `set_columnar_selection(anchor, head, cx)` delegates to the Alt-drag display-row block builder, retaining UTF-8 clipping and selecting the head row as active. Block columns count Unicode scalars within each wrapped display row, not byte offsets; tabs and wide or combining graphemes are not visual-cell-perfect. `selected_nonempty_ranges()` returns ordered, nonempty fragments for selected-query execution. `set_visual_caret` holds a presentation-only active caret offset tied to the current selection; native selection changes, mouse navigation, blur and IME composition invalidate it without changing selected bytes or the IME caret.
- `src/input/base/state.rs`: editor-owned opaque edit anchors at UTF-8 byte boundaries, with process-wide unique handles and left/right insertion affinity. Accepted replacements transform anchors in application order; positions inside replaced or deleted content collapse to the replacement start regardless of affinity, while anchors at the replaced range end shift by the edit delta. Undo/redo replay edits rather than restoring deleted interior positions. Full `set_value` resets and mask-driven whole-text rewrites invalidate anchors.
- `src/input/base/cursor.rs`: activate a block head row without discarding the other selections.
- `src/input/base/element.rs`: the active caret and cursor-follow scroll use the visual-caret offset when its selection still matches, leaving selection geometry and IME unchanged; block geometry from shaped advances (with soft-wrap boundary affinity matching layout), space-width fallback, right-edge clamp using the painted block width, and contrasting foreground for simple ASCII glyphs; retain the existing blink, scroll and IME paths. The foreground is reshaped as a single character using the current window font, so ligatures, contextual shaping, syntax-specific font substitutions, combining clusters and colored emoji are not inverted. Unsupported non-ASCII glyphs use a translucent block so the original glyph remains visible rather than being covered by an opaque caret. ASCII ligatures and contextual shaping can still differ from the reshaped foreground; the block is not a general glyph-color inversion. When the buffer is empty, placeholder glyphs are never repainted as buffer text inside the block.

Live visual verification of block placement and glyph contrast is still required.

- `src/input/base/undo_manager.rs` and `src/input/base/state.rs`: public editor-owned `begin_edit_group(id)` / `end_edit_group(id)` combine adjacent committed edits with the same ID across edit intents, without nesting an outer native IME transaction. A different ID or explicit end creates an undo boundary. Begin/end return false when another group or a native composition is outstanding; the caller must retry after composition commit and end its own group on blur. Native `unmark_text` does not end the editor group. This does not attribute callbacks across platforms or distinguish a late unmark from a currently active native composition.

The `state.rs` tests also cover a test-local outer undo bracket around programmatic deletion and native typing or IME composition; those older brackets are regression evidence rather than the public editor-owned grouping API.

- `src/text/text_view.rs`: the stateless Markdown parser convergence regression uses a deterministic, resource-free fixture over 4 KiB to retain asynchronous parsing. The README fixture triggered unrelated embedded-content load notifications and inflated the root render count; the test still rebuilds its parser callback on each render and requires at most two renders.
- `src/motion.rs` and `src/motion/presence.rs`: preserve exact transition duration when the reversal factor is 1.0 instead of passing it through `Duration::mul_f32`, which can round a 100 ms duration up by one nanosecond. Other reversal factors still scale normally.

## Refresh

Retrieve the desired published `.crate` archive into the Cargo registry cache, verify its SHA-256 against the registry checksum in `Cargo.lock` (or the crates.io index for a new version), and stop if it differs. Extract the archive's single versioned root into `vendor/gpui-base/`, retaining its manifest, license, and source files. Update the root `[patch.crates-io]` entry and resolve the lockfile with Cargo; check the diff against the published archive to identify every local deviation. Run `cargo check -p dbflux_ui_document` and `cargo nextest run -p dbflux_ui_document vim` after refreshing. A version change requires revalidating all future local patches against the new published source.
