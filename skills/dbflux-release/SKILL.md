---
name: dbflux-release
description: >
  Prepare and create DBFlux releases using the repository's real version files, changelog flow, and GitHub release workflow.
  Trigger: When user asks to create a release, bump version, tag a release, or prepare release notes for DBFlux.
license: MIT
---

## When to Use

- Preparing a new DBFlux release
- Bumping the project version
- Creating release notes or a release tag

## Repository Facts

- The release workflow is `.github/workflows/release.yml`
- Releases are triggered by `v*.*.*` tags or by `workflow_dispatch`
- The GitHub release body is composed from the matching `CHANGELOG.md` section, `.github/release-template.md`, and auto-generated notes
- The workspace version lives in `Cargo.toml` under `[workspace.package].version`
- Workspace crates inherit the version with `version.workspace = true`
- `examples/custom_driver/Cargo.toml` is standalone and does not inherit the workspace version

## Critical Patterns

- Do not assume release steps from another repo; use `.github/workflows/release.yml` as the source of truth
- Use `vX.Y.Z` for git tags, but keep file versions as `X.Y.Z` or the repo's existing dev format such as `0.5.0-dev.0`
- Before tagging, update all real versioned artifacts that apply: `Cargo.toml`, `flake.nix`, `resources/windows/installer.iss`, `scripts/PKGBUILD`, and `CHANGELOG.md`
- Check whether any standalone manifests outside workspace inheritance also need manual review, especially `examples/custom_driver/Cargo.toml`
- Keep the `CHANGELOG.md` header format aligned with the release workflow, which extracts sections like `## [X.Y.Z]`
- Only create commits, tags, or GitHub releases when the user explicitly asks
- Prefer running local validation before tagging: `cargo check --workspace`, `cargo fmt --all -- --check`, `cargo clippy --workspace -- -D warnings`, and `cargo test --workspace`

## Suggested Release Flow

```text
1. Inspect current version and latest tags
2. Decide the next version with the user
3. Update versioned files and changelog
4. Run local validation commands
5. Create commit and annotated tag if requested
6. Let GitHub Actions build artifacts and draft the release
7. Review the generated release body before publishing
```

## Commands

```bash
# Inspect current version and recent tags
git describe --tags --abbrev=0
git tag --sort=-v:refname | head -n 10

# Inspect changes since the last tag
git log $(git describe --tags --abbrev=0)..HEAD --oneline

# Local validation before tagging
cargo check --workspace
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

## Resources

- `.github/workflows/release.yml`
- `.github/release-template.md`
- `CHANGELOG.md`
- `Cargo.toml`
- `flake.nix`
- `resources/windows/installer.iss`
- `scripts/PKGBUILD`
