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
- During normal development `main` carries the next minor's `-dev` version (`vX.(Y+1).0-dev.N`). **Exception — the RC stabilization window:** from the moment `release/vX.Y` is cut until `vX.Y.0` ships stable, `main` stays on the `vX.Y.0-rc.0` marker and tracks the release line. Stabilization fixes land on `main` first and are cherry-picked into the release branch; no new `-dev` is cut while a release is stabilizing. `main` opens the next `-dev` cycle only **after** the stable `vX.Y.0` tag. The release branch carries the exact version it is stabilizing (`-rc.N`, then `vX.Y.0`).

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
- Outside an RC window, the manifest version is a `-dev.N` of the next minor (e.g. `0.7.0-dev.5`).
- Next dev bump: if last main tag is `vX.Y.Z-dev.N` → `vX.Y.Z-dev.(N+1)`. If the previous tag was stable → start at `vX.(Y+1).0-dev.0`.
- **Inside an RC window** (a `release/vX.Y` exists and `vX.Y.0` has not shipped): `main` holds the `vX.Y.0-rc.0` marker. It is **not** re-bumped as new RCs are tagged — `-rc.N` tags live only on the release branch. `main` leaves this marker only when it opens `vX.(Y+1).0-dev.0` after the stable release.

**On `release/vX.Y`:**
- Next RC: if last tag on the branch is `vX.Y.Z-rc.N` → `-rc.(N+1)`. If none → `-rc.0`.
- Promote to stable: drop the `-rc.N` suffix → `vX.Y.0`.
- Patch: increment `Z` → `vX.Y.(Z+1)`. **Never bump the minor on a release branch.**

## Cycle Example: `0.6.0`

1. Features keep landing on `main` (carrying `v0.6.0-dev.N`). Every so often, tag `v0.6.0-dev.N` directly from main's HEAD.
2. When ready to stabilize, cut `release/v0.6` from the chosen main commit at `v0.6.0-rc.0`, and set `main` to the same `v0.6.0-rc.0` marker (close `[Unreleased]` as `[0.6.0]`). Tag `v0.6.0-rc.0` on the release branch.
3. A bug appears during RC:
   - Commit the fix on `main` first (its entry goes under `[0.6.0]`; `main` stays at the `v0.6.0-rc.0` marker).
   - `git cherry-pick -x <sha>` into `release/v0.6`.
   - Bump the release branch to `v0.6.0-rc.1` and tag `v0.6.0-rc.1`.
4. When clean, drop the suffix on `release/v0.6` → tag `v0.6.0`. This is the stable release.
5. **Only now** does `main` open the next cycle: bump to `v0.7.0-dev.0`, open a fresh `[Unreleased]`.
6. Patches (`v0.6.1`, `v0.6.2`, ...) come from the same release branch (cherry-picks from main, which is now on `v0.7.0-dev.N`).

## Cut Procedure: `main` → `release/vX.Y`

When stabilization begins:

1. Verify you are on `main`, clean tree, up to date with `origin/main`.
2. Verify `.github/workflows/release.yml` on `main` contains the `Classify release` step. Without it, stable tags from the new release branch will publish as drafts (this happened to `v0.5.1`). If the step is missing, fix on `main` first.
3. Confirm the target minor `vX.Y`.
4. Create the branch (in a dedicated worktree if you use the bare-repo + worktrees layout, so `main` stays checked out):
   ```bash
   git worktree add ../release-vX.Y -b release/vX.Y main
   # or, in a single-checkout repo:
   git checkout -b release/vX.Y
   ```
5. On `release/vX.Y`:
   - In `CHANGELOG.md`, rename `## [Unreleased]` to `## [X.Y.0] - YYYY-MM-DD`. The release branch carries **no** `[Unreleased]` block.
   - Bump every versioned artifact to `X.Y.0-rc.0` (see below).
   - Commit: `chore(release): cut release/vX.Y at vX.Y.0-rc.0`.
   - Push: `git push -u origin release/vX.Y`.
6. Back on `main` (it stays on the release line during the RC window):
   - In `CHANGELOG.md`, close `## [Unreleased]` as `## [X.Y.0] - YYYY-MM-DD` (same content as the release branch). Do **not** open a fresh `[Unreleased]` or bump to `-dev` yet — that happens only after the stable release.
   - Bump every versioned artifact to `X.Y.0-rc.0` (mirrors the release branch).
   - Commit: `chore(release): align main to vX.Y.0-rc.0`.
   - Push.
7. Tag `vX.Y.0-rc.0` on the release branch.

At this point `main` and `release/vX.Y` have identical trees; they diverge only as stabilization fixes are tagged on the branch. When the stable `vX.Y.0` ships, run **Begin Next Dev Cycle** below.

## Begin Next Dev Cycle: after stable `vX.Y.0`

Run this **only after** the stable `vX.Y.0` tag is pushed from `release/vX.Y`. Until then `main` stays on the RC marker.

1. On `main`:
   - Open a fresh `## [Unreleased]` block in `CHANGELOG.md` above the `## [X.Y.0]` section. If the date on `## [X.Y.0]` is the RC-cut date, correct it to the stable release date to match the release branch.
   - Bump every versioned artifact to `X.(Y+1).0-dev.0`.
   - Commit: `chore(version): begin X.(Y+1).0-dev cycle`.
   - Push.
2. `main` now produces `vX.(Y+1).0-dev.N`; the standard cherry-pick + `[Unreleased]` discipline (below) resumes for any further `vX.Y.Z` patches.

## Files to Bump

Per release, update all of the following to the exact same version:

- `Cargo.toml` — `[workspace.package].version`. Workspace crates inherit via `version.workspace = true`.
- `flake.nix`
- `resources/windows/installer.iss`
- `CHANGELOG.md` — header for the version and entries.
- Manual review (does not inherit): `examples/custom_driver/Cargo.toml`.

After the GitHub Release artifacts for the tag are published, also update:

- `nix/release-info.nix` — `version` + both prebuilt-tarball `url`s and `hash`es (see "Nix" under "Downstream Channels"). This is a per-branch channel pointer: `main` tracks the newest published tag of any kind, each `release/vX.Y` tracks its own line's newest. It requires the published artifacts, so it lands as a follow-up commit (one per affected branch) once the release workflow finishes.

The AUR `PKGBUILD` lives in an **external AUR repository**, not in this repo. It is bumped only for stable tags.

## CHANGELOG Discipline

The discipline differs by phase. The key idea: an entry lives under exactly one header, and the header reflects the version that ships it.

**Normal development (no active RC window).** `main` carries a single `## [Unreleased]` block; every commit with user-visible behavior appends an entry to it.

**Cutting `release/vX.Y`.** The `[Unreleased]` snapshot becomes `## [X.Y.0] - YYYY-MM-DD` on **both** the release branch and `main` (their trees are identical at the cut). `main` does **not** open a fresh `[Unreleased]` — while the release stabilizes, new fix entries are appended under `## [X.Y.0]`, because those fixes ship in `X.Y.0`.

**During the RC window.** A stabilization fix lands on `main` (entry under `## [X.Y.0]`), then is cherry-picked into `release/vX.Y` (which carries the same `## [X.Y.0]`). No `[Unreleased]`/removal dance is needed here — both branches are the same release line.

**After the stable `vX.Y.0` and the next dev cycle opens.** `main` returns to a single `## [Unreleased]` on top. From here, a fix destined for a `vX.Y.(Z+1)` patch lands on main's `[Unreleased]`, is cherry-picked into `release/vX.Y` under `## [X.Y.(Z+1)]`, and **the same entry must then be removed from main's `[Unreleased]`** — once shipped on a release branch it is no longer "unreleased" in main's history. Skipping this is what left main's `[Unreleased]` carrying stale `v0.5.1` content after that release.

**Always:**
- The release workflow's `Extract changelog section` step matches the **exact tag version** as the header (`## [X.Y.Z-suffix]`, with the leading `v` stripped). Keep that header format exactly.
  - `-dev.N` releases carry their own dated `## [X.Y.Z-dev.N]` section, so they match and surface their curated notes.
  - An `-rc.N` tag looks up `## [X.Y.0-rc.N]`, which the cut procedure does **not** create (it uses the shared `## [X.Y.0]`). So RC release bodies omit the curated changelog and fall back to the installation template plus auto-generated contributor notes. The curated `## [X.Y.0]` section surfaces on the **stable** `vX.Y.0` release, whose tag matches the header exactly. (To also surface it on RC bodies, the extractor would need a fallback from `## [X.Y.0-rc.N]` to `## [X.Y.0]` — not yet implemented.)
- Invariant after every stable release: no entry present in `[X.Y.Z]` (on the release branch) should still appear in main's `[Unreleased]`.

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

### CHANGELOG cleanup after cherry-pick (MANDATORY)

When the cherry-picked commit carries a `CHANGELOG.md` entry, finish the cherry-pick with a follow-up commit on `main` that **removes the same entry from `[Unreleased]`**:

```bash
git checkout main
# edit CHANGELOG.md: delete the matching bullet(s) under [Unreleased]
git commit -am "chore(changelog): move <short summary> to vX.Y.(Z+1)"
git push
```

Batch the cleanup into a single commit on `main` once the release branch is tagged if several entries cherry-picked together.

## Downstream Channels

| Tag kind          | GitHub Release | AUR              | Nix flake (this repo)        | nixpkgs (future)  |
|-------------------|----------------|------------------|------------------------------|-------------------|
| `-dev.N` (main)   | prerelease     | skip             | bump main's `release-info` ¹             | skip              |
| `-rc.N` (release) | prerelease     | skip             | bump release branch's + main's `release-info` ¹ | skip       |
| Stable `vX.Y.Z`   | published      | bump + push      | bump release branch's + main's `release-info` ¹ | bump + PR  |

¹ `nix/release-info.nix` is **per-branch**: each branch's flake pins to its own channel (refreshed after that tag's artifacts publish). `main`'s release-info is the **latest** channel — it tracks the newest published tag of any kind (`-dev.N`, `-rc.N`, or stable). `release/vX.Y`'s release-info is that minor's channel — it tracks the branch's newest tag (`-rc.N`, then `vX.Y.0`, then `vX.Y.(Z+1)`). During the RC window both channels point at the same `-rc.N` artifacts; they diverge again once `main` opens the next `-dev` cycle.

### AUR

The PKGBUILD is maintained in an external AUR repository. AUR `pkgver` does **not** allow `-` (hyphens are reserved for `pkgrel`). For stable releases the translation is a no-op (`pkgver=X.Y.Z`). For hypothetical AUR prereleases:

- `vX.Y.Z-dev.N` → `pkgver=X.Y.Z.dev.N`
- `vX.Y.Z-rc.N`  → `pkgver=X.Y.Z.rc.N`

### Nix (this repo's flake)

The flake at the root exposes both a prebuilt-binary package (`dbflux-bin`, default) and a from-source build (`dbflux-source`). The prebuilt package reads `nix/release-info.nix`, which pins each supported system to the GitHub Release tarball it should fetch.

`release-info.nix` is per-branch and pins that branch's flake to its channel (see the footnote under the Downstream Channels table). `main` tracks the newest published tag of any kind; each `release/vX.Y` tracks its own line's newest tag. After a tag's artifacts publish, refresh `release-info.nix` on **every** branch whose channel that tag advances — for an `-rc.N` or stable `vX.Y.Z` that means both `release/vX.Y` and `main`. The release publishes a `.sha256` next to each tarball, so you can read the digest without downloading the full artifact:

```bash
ver=X.Y.Z          # or X.Y.Z-dev.N / X.Y.Z-rc.N
for arch in amd64 arm64; do
  hex=$(curl -fsSL "https://github.com/0xErwin1/dbflux/releases/download/v$ver/dbflux-linux-$arch.tar.gz.sha256" | awk '{print $1}')
  nix-hash --to-sri --type sha256 "$hex"
done
```

Update `version`, both `url`s, and both `hash`es in `nix/release-info.nix`. Verify locally:

```bash
nix build .#dbflux-bin --no-link --print-out-paths
```

Every `-rc.N` (including `N > 0`) advances both channels: bump `release-info` on `release/vX.Y` (its line's newest) and on `main` (the latest overall). The two land as separate per-branch commits, never as a cherry-pick.

### nixpkgs (future)

Not yet upstream. When it is, only stable tags will get a PR to `NixOS/nixpkgs` with `version`, `src.hash`, and (if `buildRustPackage`) `cargoHash` bumped. PR title convention: `dbflux: A -> B`.

## Anti-Patterns (refuse these)

- Tagging `vX.Y.Z` or `vX.Y.Z-rc.N` while HEAD is on `main`.
- Tagging `vX.Y.Z-dev.N` while HEAD is on a `release/*` branch.
- Merging `release/vX.Y` back into `main`.
- Creating new features (non-fix commits) on a `release/*` branch.
- Bumping minor or major version inside a `release/*` branch.
- Opening the next `-dev` cycle on `main` (bumping to `vX.(Y+1).0-dev.0`) before the stable `vX.Y.0` has shipped. During the RC window `main` stays on the `vX.Y.0-rc.0` marker.
- Pushing a tag without a clean working tree.
- Pushing the AUR bump with `pkgver` containing a hyphen.
- Cherry-picking a commit with a `CHANGELOG.md` entry into a release branch without then removing that entry from main's `[Unreleased]` block.
- Cutting `release/vX.Y` from a `main` HEAD that does not contain the `Classify release` step in `release.yml` (stable tags from that branch will publish as drafts).

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
