# Vendored `gpui-base`

This directory contains the complete published `gpui-base` 0.6.1 crate, without source changes. It establishes a local dependency boundary for future DBF-279 editor work; no caret or selection behavior is changed here.

- Upstream: [`gpui-base` 0.6.1 on crates.io](https://crates.io/crates/gpui-base/0.6.1), from the crates.io registry archive `gpui-base-0.6.1.crate`.
- License: Apache-2.0; see `LICENSE-APACHE` and the published `Cargo.toml`.
- Archive SHA-256: `9d45dcaaeac889bf1e7757db1beb26c9043c8ea3156651facc11c6be56bb6722` (the registry checksum recorded in the original root `Cargo.lock`).

## Refresh

Retrieve the desired published `.crate` archive into the Cargo registry cache, verify its SHA-256 against the registry checksum in `Cargo.lock` (or the crates.io index for a new version), and stop if it differs. Extract the archive's single versioned root into `vendor/gpui-base/`, retaining its manifest, license, and source files. Update the root `[patch.crates-io]` entry and resolve the lockfile with Cargo; check the diff against the published archive to identify every local deviation. Run `cargo check -p dbflux_ui_document` and `cargo nextest run -p dbflux_ui_document vim` after refreshing. A version change requires revalidating all future local patches against the new published source.
