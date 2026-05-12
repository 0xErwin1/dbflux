---
name: dbflux-release
description: >
  Prepare and create DBFlux releases under the trunk + short-lived release-branch model.
  Trigger: When user asks to create a release, bump version, tag a release, cut a release branch,
  or prepare release notes for DBFlux.
license: MIT
---

## When to Use

- Cutting a release branch from `main`
- Tagging a development prerelease (`-dev.N`) from `main`
- Tagging a release candidate (`-rc.N`) or stable version (`vX.Y.Z`) from a `release/vX.Y` branch
- Tagging a patch (`vX.Y.(Z+1)`) from a `release/vX.Y` branch
- Bumping versions in versioned artifacts (Cargo, Nix, Windows installer) and, on stable releases, the external AUR PKGBUILD
- Preparing changelog entries for any of the above

## Branching Model (read first — every action depends on this)

DBFlux uses **trunk + short-lived release branches**. There is exactly one long-lived branch (`main`); release branches live only as long as a minor line is supported.

| Branch                | Lifetime         | Accepts                                        | Tags produced                            |
|-----------------------|------------------|------------------------------------------------|------------------------------------------|
| `main`                | permanent        | every new commit (features, fixes, refactors)  | `vX.Y.Z-dev.N`                           |
| `release/vX.Y`        | until EOL        | cherry-picked fixes only (no new features)     | `vX.Y.Z-rc.N`, `vX.Y.Z`, `vX.Y.(Z+1)`    |

**Inviolable rules:**

- A commit is never authored on a release branch. It always lands on `main` first and is then `git cherry-pick -x <sha>` into the release branch.
- A release branch is never merged back into `main`.
- No new features enter a release branch once cut. Only bugfixes and the version/changelog bumps that the release itself requires.
- `main` always carries the next minor's `-dev` version in its manifests (`vX.(Y+1).0-dev.N`). The release branch carries the version it is stabilizing.

## Tag → Source-Branch → GitHub Release Mapping

The release workflow already classifies tags (see `.github/workflows/release.yml` — `Classify release` step):

| Tag pattern          | Allowed source branch | GitHub release kind |
|----------------------|-----------------------|---------------------|
| `vX.Y.Z-dev.N`       | `main`                | prerelease          |
| `vX.Y.Z-rc.N`        | `release/vX.Y`        | prerelease          |
| `vX.Y.Z`             | `release/vX.Y`        | stable (published)  |
| anything else        | (refuse)              | draft (safety net)  |

The skill MUST refuse to create a tag whose pattern does not match the current branch.

## Context Detection (always do this first)

Before any tagging or bumping action:

```bash
git rev-parse --abbrev-ref HEAD
git status --porcelain
git describe --tags --abbrev=0
git tag --sort=-v:refname | head -n 10
```

Resolve the context:

- Branch is `main` → "dev mode". Allowed: `-dev.N` tag, cut a new `release/vX.Y`.
- Branch matches `^release/v\d+\.\d+$` → "stabilization mode". Allowed: `-rc.N`, stable `vX.Y.Z`, patch bump.
- Any other branch → abort. Ask the user to switch to `main` or the appropriate release branch.

Working tree must be clean before any tag.

## Versioning Rules

Source-of-truth for the workspace version is `Cargo.toml` (`[workspace.package].version`). All other manifests must be kept in lockstep.

**On `main`:**
- The manifest version is always a `-dev.N` of the next minor (e.g. `0.7.0-dev.5`).
- Next dev bump: if last main tag is `vX.Y.Z-dev.N` → `vX.Y.Z-dev.(N+1)`. If the previous tag was stable → start at `vX.(Y+1).0-dev.0`.

**On `release/vX.Y`:**
- Next RC: if last tag on the branch is `vX.Y.Z-rc.N` → `-rc.(N+1)`. If none → `-rc.1`.
- Promote to stable: drop the `-rc.N` suffix → `vX.Y.0`.
- Patch: increment `Z` → `vX.Y.(Z+1)`. Never bump the minor on a release branch.

## Cut Procedure: `main` → `release/vX.Y`

When stabilization for a minor begins:

1. Verify you are on `main`, clean tree, up to date with `origin/main`.
2. Confirm the target minor `vX.Y` with the user.
3. Create the branch:
   ```bash
   git checkout -b release/vX.Y
   ```
4. On `release/vX.Y`:
   - In `CHANGELOG.md`, rename the `## [Unreleased]` section to `## [X.Y.0] - YYYY-MM-DD`.
   - Bump every versioned artifact to `X.Y.0-rc.1` (see "Files to Bump").
   - Commit: `chore(release): cut release/vX.Y at vX.Y.0-rc.1`.
   - Push: `git push -u origin release/vX.Y`.
5. Back on `main`:
   ```bash
   git checkout main
   ```
   - Open a fresh `## [Unreleased]` block in `CHANGELOG.md`.
   - Bump every versioned artifact to `X.(Y+1).0-dev.0`.
   - Commit: `chore(version): begin X.(Y+1).0-dev cycle`.
   - Push.
6. Tag `vX.Y.0-rc.1` on the release branch (see "Tag Procedure").

## Tag Procedure (any tag)

1. Run local validation:
   ```bash
   cargo check --workspace
   cargo fmt --all -- --check
   cargo clippy --workspace -- -D warnings
   cargo test --workspace
   ```
2. Confirm the versioned files match the tag exactly. The git tag is `vX.Y.Z[-suffix.N]`; the file versions drop the leading `v` but keep the suffix verbatim — except AUR `pkgver` (see "AUR Bump").
3. Create an annotated tag:
   ```bash
   git tag -a vX.Y.Z[-suffix.N] -m "vX.Y.Z[-suffix.N]"
   git push origin vX.Y.Z[-suffix.N]
   ```
4. The GitHub Actions release workflow takes over (artifacts, classify, publish/prerelease).
5. Only after the workflow completes and the release is verified, do the post-release steps applicable to the tag kind (see "Post-Release Channels").

## Files to Bump

These must all carry the exact same version number per release (modulo the AUR translation below):

- `Cargo.toml` — `[workspace.package].version`. Workspace crates inherit via `version.workspace = true`.
- `flake.nix`
- `resources/windows/installer.iss`
- `CHANGELOG.md` — header for the version + entries.
- Review (does not inherit workspace version): `examples/custom_driver/Cargo.toml`.

## CHANGELOG Discipline

- A single `## [Unreleased]` block on `main`. Every commit that introduces user-visible behavior appends an entry to it.
- When `release/vX.Y` is cut, the `Unreleased` block on `main` is "split": the snapshot becomes `[X.Y.0]` on the release branch; `main` opens a new empty `Unreleased`.
- A cherry-pick into `release/vX.Y` should bring its changelog entry too. If the entry already exists in `main`'s new `Unreleased`, leave it; the duplication is intentional (one entry per shipped tag).
- The release workflow extracts the section by header (`## [X.Y.Z]`) — keep that format exactly.

## Cherry-Pick Discipline

A release branch should never contain commits that don't exist on `main`, except the release-only commits (`chore(release): ...`, `chore(version): ...`).

```bash
# On main: land the fix.
git checkout main
# ...commit, push...

# On release branch: cherry-pick with -x to record the source SHA.
git checkout release/vX.Y
git cherry-pick -x <sha>
```

Sanity check: every non-release commit on `release/vX.Y` since branch-off should mention `(cherry picked from commit ...)` in its message.

## AUR Bump

AUR is bumped manually today, in an **external AUR repository** (not this repo — no `PKGBUILD` lives here). Until automation lands, only **stable** tags (`vX.Y.Z` without suffix) are published to AUR. Skip AUR for `-dev.N` and `-rc.N`.

**Important constraint:** AUR `pkgver` does **not** allow `-` (hyphen is reserved for `pkgrel`). For stable releases the translation is a no-op (`pkgver=X.Y.Z`). If a prerelease ever needs to be published on AUR in the future:

- `vX.Y.Z-dev.N` → `pkgver=X.Y.Z.dev.N` (dots only).
- `vX.Y.Z-rc.N` → `pkgver=X.Y.Z.rc.N`.

Steps for a stable AUR bump (run in the AUR repo clone, not in dbflux):

1. Update `PKGBUILD`:
   - `pkgver=X.Y.Z`
   - Reset `pkgrel=1`
2. Regenerate `.SRCINFO`:
   ```bash
   makepkg --printsrcinfo > .SRCINFO
   ```
3. Validate locally:
   ```bash
   namcap PKGBUILD
   makepkg -si --noconfirm   # optional, on Arch only
   ```
4. Commit + push to the AUR remote:
   ```bash
   git commit -am "release: vX.Y.Z"
   git push origin master
   ```
5. Future hardening: replace `sha256sums_*=('SKIP')` with real hashes computed from the published GitHub artifacts.

## nixpkgs Bump (future, not active yet)

When DBFlux lands in `NixOS/nixpkgs`:

- Only stable tags get bumped there.
- The flow is a PR to `NixOS/nixpkgs` updating `pkgs/by-name/db/dbflux/package.nix` (or wherever it lives at the time):
  - `version = "X.Y.Z";`
  - `src.hash = "sha256-..."` (recompute via `nix-prefetch-github` or `nix-prefetch-url`).
  - `cargoHash = "sha256-..."` if it uses `buildRustPackage`.
- Open the PR with title `dbflux: X.Y.(Z-1) -> X.Y.Z` per nixpkgs convention.

Mark this as TODO in the skill until the package is upstreamed; do not invent the path before then.

## Post-Release Channels (what to do after the GitHub release publishes)

| Tag kind          | GitHub Release | AUR              | nixpkgs (future)  |
|-------------------|----------------|------------------|-------------------|
| `-dev.N` (main)   | prerelease     | skip             | skip              |
| `-rc.N` (release) | prerelease     | skip             | skip              |
| Stable `vX.Y.Z`   | published      | bump + push      | bump + PR         |

## Anti-Patterns (explicit refusals)

Refuse, with a clear message, if any of these are requested:

- Tagging `vX.Y.Z` or `vX.Y.Z-rc.N` while HEAD is on `main`.
- Tagging `vX.Y.Z-dev.N` while HEAD is on a `release/*` branch.
- Merging `release/vX.Y` back into `main`.
- Creating new features (non-fix commits) on a `release/*` branch.
- Bumping minor or major version inside a `release/*` branch.
- Pushing a tag without the working tree being clean.
- Pushing the AUR bump with `pkgver` containing a hyphen.

## Local Validation Commands

```bash
cargo check --workspace
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

## Inspection Commands

```bash
# Current branch and cleanliness
git rev-parse --abbrev-ref HEAD
git status --porcelain

# Most recent tag and last 10 tags by version
git describe --tags --abbrev=0
git tag --sort=-v:refname | head -n 10

# Commits since last tag
git log "$(git describe --tags --abbrev=0)"..HEAD --oneline

# Cherry-pick provenance audit on a release branch
git log --grep='cherry picked from' release/vX.Y
```

## Resources

- `.github/workflows/release.yml` — classification logic and artifact publishing
- `.github/release-template.md` — installation section appended to every release body
- `CHANGELOG.md` — single source of truth for release notes
- `Cargo.toml`, `flake.nix`, `resources/windows/installer.iss`
- `examples/custom_driver/Cargo.toml` (standalone, review manually)
