# Release Process

DBFlux uses **trunk-based development with short-lived release branches**. This is the same model used by Rust, Chromium, Node, and VSCode: one long-lived branch (`main`) plus an ephemeral `release/vX.Y` branch per minor while it is being stabilized.

This document is the human-facing reference. The automated `dbflux-release` skill (`skills/dbflux-release/SKILL.md`) follows these same rules.

## Branches

| Branch          | Lifetime    | Accepts                                       | Tags produced                          |
|-----------------|-------------|-----------------------------------------------|----------------------------------------|
| `main`          | permanent   | every new commit (features, fixes, refactors) | `vX.Y.Z-dev.N`                         |
| `release/vX.Y`  | until EOL   | cherry-picked fixes only (no new features)    | `vX.Y.Z-rc.N`, `vX.Y.Z`, `vX.Y.(Z+1)`  |

### Inviolable rules

- A commit is **never** authored on a release branch. It always lands on `main` first and is then `git cherry-pick -x <sha>` into the release branch.
- A release branch is **never** merged back into `main`.
- **No new features** on a release branch once cut. Only bugfixes and the release's own version/changelog bumps.
- `main` always carries the next minor's `-dev` version in its manifests (`vX.(Y+1).0-dev.N`). The release branch carries the version it is stabilizing.

## Tags

The release workflow (`.github/workflows/release.yml`) classifies tags automatically:

| Tag pattern    | Allowed source branch | GitHub release kind |
|----------------|-----------------------|---------------------|
| `vX.Y.Z-dev.N` | `main`                | prerelease          |
| `vX.Y.Z-rc.N`  | `release/vX.Y`        | prerelease          |
| `vX.Y.Z`       | `release/vX.Y`        | stable (published)  |
| anything else  | (refuse)              | draft (safety net)  |

Tags must be annotated:

```bash
git tag -a vX.Y.Z[-suffix.N] -m "vX.Y.Z[-suffix.N]"
git push origin vX.Y.Z[-suffix.N]
```

## Versioning Rules

The workspace version (`Cargo.toml` `[workspace.package].version`) is the source of truth. All other manifests must stay in lockstep.

**On `main`:**
- The manifest version is always a `-dev.N` of the next minor (e.g. `0.7.0-dev.5`).
- Next dev bump: if last main tag is `vX.Y.Z-dev.N` → `vX.Y.Z-dev.(N+1)`. If the previous tag was stable → start at `vX.(Y+1).0-dev.0`.

**On `release/vX.Y`:**
- Next RC: if last tag on the branch is `vX.Y.Z-rc.N` → `-rc.(N+1)`. If none → `-rc.1`.
- Promote to stable: drop the `-rc.N` suffix → `vX.Y.0`.
- Patch: increment `Z` → `vX.Y.(Z+1)`. **Never bump the minor on a release branch.**

## Cycle Example: `0.6.0`

1. Features keep landing on `main`. Every so often, tag `v0.6.0-dev.N` directly from main's HEAD.
2. When ready to stabilize, cut `release/v0.6` from the chosen main commit. Tag `v0.6.0-rc.1` there.
3. A bug appears during RC:
   - Commit the fix on `main` first.
   - `git cherry-pick -x <sha>` into `release/v0.6`.
   - Tag `v0.6.0-rc.2`.
4. When clean, tag `v0.6.0` on `release/v0.6`. This is the stable release.
5. Patches (`v0.6.1`, `v0.6.2`, ...) come from the same release branch (cherry-picks from main).
6. Meanwhile, `main` is already producing `v0.7.0-dev.N`.

## Cut Procedure: `main` → `release/vX.Y`

When stabilization begins:

1. Verify you are on `main`, clean tree, up to date with `origin/main`.
2. Confirm the target minor `vX.Y`.
3. Create the branch:
   ```bash
   git checkout -b release/vX.Y
   ```
4. On `release/vX.Y`:
   - In `CHANGELOG.md`, rename `## [Unreleased]` to `## [X.Y.0] - YYYY-MM-DD`.
   - Bump every versioned artifact to `X.Y.0-rc.1` (see below).
   - Commit: `chore(release): cut release/vX.Y at vX.Y.0-rc.1`.
   - Push: `git push -u origin release/vX.Y`.
5. Back on `main`:
   - Open a fresh `## [Unreleased]` block in `CHANGELOG.md`.
   - Bump every versioned artifact to `X.(Y+1).0-dev.0`.
   - Commit: `chore(version): begin X.(Y+1).0-dev cycle`.
   - Push.
6. Tag `vX.Y.0-rc.1` on the release branch.

## Files to Bump

Per release, update all of the following to the exact same version:

- `Cargo.toml` — `[workspace.package].version`. Workspace crates inherit via `version.workspace = true`.
- `flake.nix`
- `resources/windows/installer.iss`
- `CHANGELOG.md` — header for the version and entries.
- Manual review (does not inherit): `examples/custom_driver/Cargo.toml`.

For **stable** releases only, also update:

- `nix/release-info.nix` — `version` + both prebuilt-tarball hashes (see "Nix" under "Downstream Channels").

The AUR `PKGBUILD` lives in an **external AUR repository**, not in this repo. It is bumped only for stable tags.

## CHANGELOG Discipline

- A single `## [Unreleased]` block lives on `main`. Every commit that introduces user-visible behavior appends an entry to it.
- When `release/vX.Y` is cut, the `Unreleased` block is "split":
  - On the release branch, the snapshot becomes `## [X.Y.0] - YYYY-MM-DD`.
  - On `main`, a new empty `Unreleased` block opens.
- Cherry-picks into `release/vX.Y` bring their changelog entry too. If the entry was added back to `main`'s new `Unreleased`, leave it there as well — duplication is intentional, since each shipped tag advertises its own notes.
- The release workflow extracts the section by header (`## [X.Y.Z]`). Keep that format exactly.

## Cherry-Pick Discipline

A release branch should never contain commits absent from `main`, except the release-only commits (`chore(release): ...`, `chore(version): ...`).

```bash
# On main: land the fix.
git checkout main
# ...commit, push...

# On release branch: cherry-pick with -x to record the source SHA.
git checkout release/vX.Y
git cherry-pick -x <sha>
```

Audit: every non-release commit on `release/vX.Y` since branch-off should mention `(cherry picked from commit ...)` in its message.

```bash
git log --grep='cherry picked from' release/vX.Y
```

## Downstream Channels

| Tag kind          | GitHub Release | AUR              | Nix flake (this repo) | nixpkgs (future)  |
|-------------------|----------------|------------------|-----------------------|-------------------|
| `-dev.N` (main)   | prerelease     | skip             | skip                  | skip              |
| `-rc.N` (release) | prerelease     | skip             | skip                  | skip              |
| Stable `vX.Y.Z`   | published      | bump + push      | bump `release-info`   | bump + PR         |

### AUR

The PKGBUILD is maintained in an external AUR repository. AUR `pkgver` does **not** allow `-` (hyphens are reserved for `pkgrel`). For stable releases the translation is a no-op (`pkgver=X.Y.Z`). For hypothetical AUR prereleases:

- `vX.Y.Z-dev.N` → `pkgver=X.Y.Z.dev.N`
- `vX.Y.Z-rc.N`  → `pkgver=X.Y.Z.rc.N`

### Nix (this repo's flake)

The flake at the root exposes both a prebuilt-binary package (`dbflux-bin`, default) and a from-source build (`dbflux-source`). The prebuilt package reads `nix/release-info.nix`, which pins each supported system to the GitHub Release tarball it should fetch.

On every **stable** release, refresh `nix/release-info.nix`:

```bash
ver=X.Y.Z
for arch in amd64 arm64; do
  curl -fsSL -o /tmp/dbflux-$arch.tar.gz \
    "https://github.com/0xErwin1/dbflux/releases/download/v$ver/dbflux-linux-$arch.tar.gz"
  nix-hash --to-sri --type sha256 \
    "$(sha256sum /tmp/dbflux-$arch.tar.gz | cut -d' ' -f1)"
done
```

Update `version`, both `url`s, and both `hash`es in `nix/release-info.nix`. Verify locally:

```bash
nix build .#dbflux-bin --no-link --print-out-paths
```

Skip Nix bumps for `-dev.N` and `-rc.N` — only stable releases update the flake.

### nixpkgs (future)

Not yet upstream. When it is, only stable tags will get a PR to `NixOS/nixpkgs` with `version`, `src.hash`, and (if `buildRustPackage`) `cargoHash` bumped. PR title convention: `dbflux: A -> B`.

## Anti-Patterns (refuse these)

- Tagging `vX.Y.Z` or `vX.Y.Z-rc.N` while HEAD is on `main`.
- Tagging `vX.Y.Z-dev.N` while HEAD is on a `release/*` branch.
- Merging `release/vX.Y` back into `main`.
- Creating new features (non-fix commits) on a `release/*` branch.
- Bumping minor or major version inside a `release/*` branch.
- Pushing a tag without a clean working tree.
- Pushing the AUR bump with `pkgver` containing a hyphen.

## Local Validation Before Tagging

```bash
cargo check --workspace
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

## Related

- `.github/workflows/release.yml` — classification logic and artifact publishing
- `.github/release-template.md` — installation section appended to every release body
- `CHANGELOG.md` — release notes source
- `skills/dbflux-release/SKILL.md` — agent-facing skill that automates this process
