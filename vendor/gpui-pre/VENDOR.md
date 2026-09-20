# Vendored `gpui-pre`

This directory is the published `gpui-pre` crate source plus one patch. It exists so the
schema visualizer can pan and zoom without DBFlux depending on a fork of Zed.

## Why

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

Depending on that fork would put `main` back on a personal git source for the whole
dependency graph — the same shape of coupling that #639 removed when it dropped the patched
blade fork. Vendoring instead keeps every consumer on the published crate and confines the
delta to this directory.

## Provenance

- Base: the `gpui-pre` 0.3.5 crate published on crates.io (a snapshot of Zed's
  `crates/gpui` at `zed@d89e9c2`), trimmed of `examples/`, `docs/`, `tests/` and their
  target tables.
- Patch: `element-transform.patch`, the diff of
  `0xErwin1/zed@978c6b45cb966f2d02bfc11cd8c1c4696b7c3fd8` (branch
  `feature/53306-gpui-transforms`) against Zed's `main` at its merge base — 14 commits over
  seven files, 2533 diff lines.
- `[workspace]` is appended to `Cargo.toml` so Cargo does not expect this crate in the
  parent workspace's member list.

The patch applies to the published source with all but one hunk accepted; the one reject is
the `use crate::{...}` import list in `src/window.rs`, because upstream reorders it by
rustfmt. `ElementTransform` was added to that list by hand. **Re-check that line after every
refresh** — it is the only place where a refresh needs a human edit.

The vendored delta is not byte-identical to the fork's branch in one deliberate place:
the `src/text_system/line.rs` hunk imports only `ScaledPixels` and `TransformationMatrix`,
dropping the `ShapedGlyph` and `ShapedRun` that upstream's current code no longer needs.
Without that trim the crate builds with an `unused_imports` warning, and a path dependency
is linted as first-party code, so `cargo clippy --workspace -- -D warnings` would fail.

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
`element-transform.patch`. It leaves `.rej` files behind for any hunk that no longer
applies; resolve them by reading the rejected hunk and porting it, then delete the `.rej`.

Afterwards, update the `[patch.crates-io]` version constraint in the root `Cargo.toml`
(`gpui = { package = "gpui-pre", version = "=0.3.6" }`, `gpui-pre-platform`,
`gpui-component` as needed) and run:

```sh
cargo check -p gpui-pre --manifest-path vendor/gpui-pre/Cargo.toml   # patch still compiles
cargo nextest run -p dbflux_schema_viz                               # layout is unchanged
cargo check --workspace
```

When the transform lands in a published `gpui-pre`, delete this directory and the
`[patch.crates-io]` entry with it: the schema visualizer needs no change.
