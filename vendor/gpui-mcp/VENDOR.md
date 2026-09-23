# Vendored `gpui-mcp`

This directory holds two crates from
[themixednuts/gpui-mcp](https://github.com/themixednuts/gpui-mcp), unmodified except for
their manifests. They are the in-process half of UI automation: the bridge that reads each
rendered frame of a GPUI window, injects synthetic input and answers requests from an
automation client over an owner-only local socket.

- `crates/gpui-mcp`: the bridge installed into an application's windows.
- `crates/gpui-mcp-protocol`: the authenticated wire protocol and endpoint discovery shared
  by the bridge and its clients.

Upstream's `gpui-mcp-server`, `gpui-mcp-capture` and `gpui-mcp-html` crates are not
vendored. They carry the MCP server stack (`rmcp`, a different major version than DBFlux
uses) and screenshot capture, which are separate decisions.

## Why vendor

The bridge is written against the frame-observer hooks that `vendor/gpui-pre` carries as
`frame-observer.patch` (see `vendor/gpui-pre/VENDOR.md`). No published crate provides it,
and a git dependency on the upstream repository would put `main` on a personal git source
and pull in its own patched copy of Zed's gpui. Vendoring builds the two crates against
DBFlux's own `gpui` (`gpui-pre` 0.3.5 with both patches applied), so the app links one gpui.

## Provenance

- Repository: `https://github.com/themixednuts/gpui-mcp`
- Commit: `7b834c1705cb3353650dfc681f29eaa29e4cf308` (2026-09-22, "Refresh vendored GPUI and
  harden frame observation"). `vendor/gpui-pre/frame-observer.patch` is taken from the same
  commit, so the bridge and the gpui hooks it calls come from one upstream snapshot.
- License: Apache-2.0. `LICENSE` is upstream's license file, copied verbatim.
- Contents: `crates/gpui-mcp/` and `crates/gpui-mcp-protocol/` as they are at that commit,
  with `dbflux-port.patch` applied. `refresh.sh` rebuilds exactly this tree.

## The DBFlux delta

`dbflux-port.patch` changes only the two `Cargo.toml` files. No Rust source is modified.

- Package metadata. Upstream inherits `version`, `edition`, `license`, `repository` and
  `homepage` from its own workspace root, which is not vendored. Inheriting from DBFlux's
  root instead would silently relabel the crates as version `0.8.0-dev.0` under
  `MIT OR Apache-2.0`, so the upstream values are written out. `publish = false` is added,
  as in every DBFlux crate.
- `rust-version`: upstream declares 1.96; DBFlux pins 1.95.0 in `rust-toolchain.toml`, and
  both crates build and pass their tests on it, so the patch declares 1.95.
- Dependencies. Upstream's `x.workspace = true` entries are replaced with the requirement
  and features its workspace root declares, copied verbatim. Every one resolves to a
  version already in DBFlux's `Cargo.lock`; the only crate the vendoring adds to the lock
  is `directories` 6.0.0 (whose only dependency, `dirs-sys` 0.5, was already there).
- `gpui` stays `gpui.workspace = true`, as upstream wrote it. In DBFlux that resolves to
  `gpui = { package = "gpui-pre", version = "=0.3.5" }` from the root `Cargo.toml`, which
  `[patch.crates-io]` points at `vendor/gpui-pre`. No API fixups were needed: the
  frame-observer patch gives gpui-pre 0.3.5 the API this commit expects.
- Lints: see below.

## Lints and AGENTS.md

Both crates are workspace members, so `cargo clippy --workspace -- -D warnings`,
`cargo fmt --all --check`, `cargo test --workspace` and `cargo nextest run --workspace` all
cover them, and they are linted as first-party code.

`gpui-mcp-protocol` keeps `[lints] workspace = true` and passes DBFlux's full lint set.

`gpui-mcp` needs one exemption, `clippy::indexing_slicing`, which fires on five sites in
library code (`src/registry.rs` lines 430, 531 and 568, `src/service.rs` lines 1413 and
1414) and more in its tests. Each is bounded by the surrounding code (a slice up to the
current enumerate index, a position recorded from the same vector, a key taken from the
map's own order list, a nibble indexing a 16-entry table). Rewriting them to `.get()` would
be a refactor of upstream code for no behavior change. Cargo cannot relax one inherited
lint, so the manifest restates the root `[workspace.lints]` without `indexing_slicing`.
**Keep that table in step with the root when the workspace lints change.**

Upstream code is exempt from the AGENTS.md rule against `let _ =` on fallible
expressions, as `vendor/gpui-pre` is. `src/service.rs` has nine `let _ =` sites: six
discard a fallible result during best-effort cleanup (removing the endpoint or temporary
descriptor file after a startup error, removing the owned descriptor, joining the network
thread on shutdown), when replying to a caller that may have gone, or when reading the
peer PID on Windows; three silence parameters unused on the current platform. They stay as upstream wrote them so the vendored source remains a byte-identical
copy. DBFlux-authored code, including the code that installs the bridge, still follows
AGENTS.md.

## CI

- `cargo fmt --all -- --check`: the upstream source already matches DBFlux's
  `rustfmt.toml` (both use edition 2024 and `max_width = 100`).
- `cargo machete`: no unused dependencies reported.
- `cargo deny check`: the crates are Apache-2.0 and `directories` is MIT OR Apache-2.0, both
  allowed. No advisory, ban or source finding involves the vendored crates or
  `directories`.
- The Linux clippy and test jobs were run locally. The macOS and Windows clippy and check
  jobs compile the cfg-gated code in `src/native_window.rs` (AppKit window number, an
  `unsafe` block under a local `#[allow(unsafe_code)]`) and `src/service.rs` (named-pipe
  security descriptor), which a Linux run does not.

## Refreshing

```sh
vendor/gpui-mcp/refresh.sh <full commit SHA>
```

The script downloads that commit, rebuilds `crates/` and `LICENSE` from it and re-applies
`dbflux-port.patch`, leaving a `.rej` file next to any manifest hunk that no longer
applies. Refresh `vendor/gpui-pre/frame-observer.patch` from the same commit first (see
`vendor/gpui-pre/VENDOR.md`); the bridge calls the hooks that patch adds.

Afterwards, update the commit in this file and re-check:

- new or removed dependencies in upstream's crate manifests, and new entries in its
  workspace `[workspace.dependencies]` that the patch has to spell out;
- `Cargo.lock`: a refresh should not add crates beyond what the bridge needs, and must not
  add the `rmcp` or capture stack;
- the `indexing_slicing` sites and the `let _ =` sites listed above;
- `rust-version` against `rust-toolchain.toml`.

Then run:

```sh
cargo nextest run -p gpui-mcp -p gpui-mcp-protocol
cargo clippy --workspace -- -D warnings
cargo fmt --all -- --check
cargo machete
```
