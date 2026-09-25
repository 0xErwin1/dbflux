# Vendored `gpui-base`

This directory starts from the complete published `gpui-base` 0.6.1 crate. DBF-279 carries local cursor and rectangular-selection patches.

- Upstream: [`gpui-base` 0.6.1 on crates.io](https://crates.io/crates/gpui-base/0.6.1), from the crates.io registry archive `gpui-base-0.6.1.crate`.
- License: Apache-2.0; see `LICENSE-APACHE` and the published `Cargo.toml`.
- Archive SHA-256: `9d45dcaaeac889bf1e7757db1beb26c9043c8ea3156651facc11c6be56bb6722` (the registry checksum recorded in the original root `Cargo.lock`).

## Local patch list

- `src/input/base/mod.rs`: public `InputCursorShape` (`Bar` / `Block`).
- `src/input/mod.rs`: re-export the cursor shape for application use.
- `src/input/base/state.rs`: per-input cursor shape, Bar default, getter and notifying setter; public `set_columnar_selection(anchor, head, cx)` delegates to the Alt-drag display-row block builder, retaining UTF-8 clipping and selecting the head row as active. Block columns count Unicode scalars within each wrapped display row, not byte offsets; tabs and wide or combining graphemes are not visual-cell-perfect. `selected_nonempty_ranges()` returns ordered, nonempty fragments for selected-query execution.
- `src/input/base/cursor.rs`: activate a block head row without discarding the other selections.
- `src/input/base/element.rs`: block geometry from shaped advances (with soft-wrap boundary affinity matching layout), space-width fallback, right-edge clamp using the painted block width, and contrasting foreground for simple ASCII glyphs; retain the existing blink, scroll and IME paths. The foreground is reshaped as a single character using the current window font, so ligatures, contextual shaping, syntax-specific font substitutions, combining clusters and colored emoji are not inverted. Unsupported non-ASCII glyphs use a translucent block so the original glyph remains visible rather than being covered by an opaque caret. ASCII ligatures and contextual shaping can still differ from the reshaped foreground; the block is not a general glyph-color inversion. When the buffer is empty, placeholder glyphs are never repainted as buffer text inside the block.

Live visual verification of block placement and glyph contrast is still required.

## Refresh

Retrieve the desired published `.crate` archive into the Cargo registry cache, verify its SHA-256 against the registry checksum in `Cargo.lock` (or the crates.io index for a new version), and stop if it differs. Extract the archive's single versioned root into `vendor/gpui-base/`, retaining its manifest, license, and source files. Update the root `[patch.crates-io]` entry and resolve the lockfile with Cargo; check the diff against the published archive to identify every local deviation. Run `cargo check -p dbflux_ui_document` and `cargo nextest run -p dbflux_ui_document vim` after refreshing. A version change requires revalidating all future local patches against the new published source.
