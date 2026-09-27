# Vendored `gpui-component`

This directory starts from the complete published `gpui-component` 0.6.1 crate. It is vendored so DBFlux can adapt the editor's find/replace panel and the component key bindings to its keyboard-first navigation.

- Upstream: [`gpui-component` 0.6.1 on crates.io](https://crates.io/crates/gpui-component/0.6.1), from the crates.io registry archive `gpui-component-0.6.1.crate`.
- License: Apache-2.0; see `LICENSE-APACHE` and the published `Cargo.toml`.
- Archive SHA-256: `52b7ab4921dc9d2624648fb40580067bfe0f47dd14f5f8f6e070d40c6d4246d4` (the registry checksum recorded in the root `Cargo.lock` before vendoring).

## Local patch list

Every local change is listed here with the file it touches and the behaviour it changes.

- `src/input/overlay.rs` and `src/input/search.rs`: when the overlay sync sees the search session close, it returns focus to the editor only if the panel's search or replace field still has focus (`SearchPanel::has_focus`). Upstream refocused the editor unconditionally, which took focus back from wherever it had moved when the session closed after focus left the panel, for example a pane move out of the find panel while a language-feature popover kept the overlay host alive. Closing from inside the panel (Escape, the close button) still returns focus to the editor. The crate's own tests never apply overlay focus (`!cfg!(test)`), so this path has no test here.

## Refresh

Retrieve the desired published `.crate` archive into the Cargo registry cache, verify its SHA-256 against the registry checksum in `Cargo.lock` (or the crates.io index for a new version), and stop if it differs. Extract the archive's single versioned root into `vendor/gpui-component/`, retaining its manifest, license, and source files. Keep the root `[patch.crates-io]` entry and resolve the lockfile with Cargo; check the diff against the published archive to identify every local deviation. Run `cargo check -p dbflux_ui_document` and `cargo nextest run -p dbflux_ui_document` after refreshing. A version change requires revalidating every local patch against the new published source.
