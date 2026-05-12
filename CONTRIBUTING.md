# Contributing to DBFlux

Thanks for considering a contribution. This guide explains how to file issues, open pull requests, and follow the conventions DBFlux uses for releases and labels.

## Quick Links

- [Architecture overview](ARCHITECTURE.md)
- [Release process and branching model](docs/RELEASE.md)
- [Audit event schema](docs/AUDIT.md)
- [Driver RPC protocol](docs/DRIVER_RPC_PROTOCOL.md)
- [Lua scripting](docs/LUA.md)
- [MCP / AI integration](docs/MCP_AI_INTEGRATION.md)

## Project Setup

DBFlux is a Rust workspace using [GPUI](https://github.com/zed-industries/zed) for the UI. The full feature set requires the database driver feature flags:

```bash
cargo check --workspace
cargo build
cargo run
```

Before opening a PR run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

A Nix dev shell is available: `nix develop`.

## Branching Model

DBFlux uses **trunk-based development with short-lived release branches**:

- `main` is the only long-lived branch. All work targets `main`.
- `release/vX.Y` branches are cut from `main` only when a minor needs to be stabilized for a stable release. They accept cherry-picked fixes from `main` only — no new features.

Contributors should **always** target `main` with their PRs. Backporting to a release branch is a maintainer responsibility.

The full rules (tags, version bumps, cut procedure, CHANGELOG discipline) live in [`docs/RELEASE.md`](docs/RELEASE.md).

## Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/) where it fits naturally:

- `feat(scope): …` — new user-facing capability
- `fix(scope): …` — bug fix
- `refactor(scope): …` — internal change with no behavior change
- `perf(scope): …` — performance improvement
- `docs(scope): …` — documentation only
- `test(scope): …` — tests only
- `ci(scope): …` — CI / release workflow changes
- `chore(scope): …` — repo plumbing (deps, tooling, version bumps)

Scope is the affected area: a driver name (`postgres`, `mongodb`), `ui`, `mcp`, `audit`, `rpc`, `release`, etc. Keep the subject under 70 chars; explain the *why* in the body when non-obvious.

## Pull Requests

1. Branch from `main`. Keep PRs focused on a single concern.
2. Fill in the [PR template](.github/pull_request_template.md): summary, what it resolves, how it was solved, validation evidence, and where it was tested.
3. Link the issue it closes with `Resolves #N` in the description.
4. Apply the labels that describe the change. See [Label Guide](#label-guide) below.
5. Keep diffs reviewable. PRs over ~400 changed lines should be split into stacked/chained PRs unless the maintainer approves a `size:exception`.
6. CI must pass (`tests.yml`, `style.yml`). Re-run locally before pushing if anything fails.

### Updating the CHANGELOG

Any user-visible change must add an entry to the `## [Unreleased]` block in [`CHANGELOG.md`](CHANGELOG.md). Use the existing categories (`Added`, `Changed`, `Fixed`, `Removed`, etc.). Internal-only changes (refactors with no behavior change, CI plumbing, tests) do not need a changelog entry.

## Issues

Before opening an issue:

- Search existing issues to avoid duplicates.
- Reproduce against a recent build if you can.

Include:

- Version of DBFlux (`dbflux --version`), OS / display server (X11 vs Wayland on Linux), and database engine + version.
- Steps to reproduce.
- Expected vs actual behavior.
- Logs if relevant. Redact secrets.

Apply the labels that describe the issue. See [Label Guide](#label-guide).

## Label Guide

The repo uses a structured label taxonomy. Apply **one label from each applicable axis** when opening an issue or PR. Maintainers may adjust during triage.

### Kind (one of `*:bug` or `*:feature` per affected area)

Areas that have a bug/feature split:

| Area      | Bug                | Feature              |
|-----------|--------------------|----------------------|
| AWS       | `aws:bug`          | `aws:feature`        |
| Audit     | `audit:bug`        | `audit:feature`      |
| Driver    | `driver:bug`       | `driver:feature`     |
| MCP       | `mcp:bug`          | `mcp:feature`        |
| Pipeline  | `pipeline:bug`     | `pipeline:feature`   |
| Proxy     | `proxy:bug`        | `proxy:feature`      |
| Query     | `query:bug`        | `query:feature`      |
| RPC       | `rpc:bug`          | `rpc:feature`        |
| SSH       | `ssh:bug`          | `ssh:feature`        |
| Storage   | `storage:bug`      | `storage:feature`    |
| UI        | `ui:bug`           | `ui:feature`         |

Plus the generic GitHub-default `bug`, `documentation`, `question`, `help wanted`, `good first issue`, `invalid`.

### Subsystem flags (apply when relevant)

- `aws`, `proxy`, `ssh`, `query`, `driver`, `mcp`

### Driver (when the change is driver-specific)

`driver:mongodb`, `driver:postgres`, `driver:sqlite`, `driver:mysql/mariadb`, `driver:dynamodb`, `driver:redis`

### Data model kind (for store/driver-level work)

`kind:sql`, `kind:document`, `kind:kv`, `kind:log`

### Platform / Arch (when behavior is platform-specific)

- Platform: `platform:linux`, `platform:macos`, `platform:windows`
- Arch: `arch:amd64`, `arch:arm64`

### RPC subtype (when touching RPC-backed services)

`rpc:auth`, `rpc:driver` (in addition to `rpc:bug`/`rpc:feature`)

### Priority

`priority:high`, `priority:medium`, `priority:low` — usually applied by maintainers during triage.

### Status (applied by maintainers)

`status:needs-review`, `status:approved`, `status:rejected`

### Example combinations

- A PostgreSQL JSON query bug on Linux:
  `driver:bug`, `driver:postgres`, `query:bug`, `platform:linux`, `kind:sql`
- A new Redis pub/sub feature:
  `driver:feature`, `driver:redis`, `kind:kv`
- An MCP approval-flow regression on Windows:
  `mcp:bug`, `platform:windows`
- An SSH tunnel UI improvement:
  `ui:feature`, `ssh:feature`, `ssh`

If you're unsure, label as best you can — maintainers will refine during triage.

## Security

Do not file security issues publicly. Email the maintainer or use a private channel. Logs and reproductions must be redacted of secrets (tokens, passwords, connection strings).

## License

By contributing you agree your contributions are licensed under the project's dual MIT / Apache-2.0 license.
