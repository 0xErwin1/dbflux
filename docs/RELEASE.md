# Release Process

DBFlux uses **trunk-based development with short-lived release branches**. One long-lived branch (`main`) is the integration target; a `release/vX.Y` branch is cut per minor during stabilization and discarded after EOL.

This document is the human-facing reference. The automated `dbflux-release` skill (`skills/dbflux-release/SKILL.md`) follows these same rules.

## Channels

| Channel     | Source branch   | Tag pattern             | GitHub release kind | Built by          |
|-------------|-----------------|-------------------------|---------------------|-------------------|
| **nightly** | `main` HEAD     | `nightly` (rolling)     | prerelease          | Cron — daily      |
| **rc**      | `release/vX.Y`  | `vX.Y.Z-rc.N`           | prerelease          | Tag push          |
| **stable**  | `release/vX.Y`  | `vX.Y.Z`                | published           | Tag push          |

The `-dev.N` channel is **retired**. Nightly replaces it. Old `-dev.N` tags remain on GitHub but no new ones are created.

Per-channel application icons are tracked in [issue #183](https://github.com/0xErwin1/dbflux/issues/183). Do not implement them here.

## Changelog Model (git-cliff, Model B)

The changelog is **derived from git history** by [git-cliff](https://git-cliff.org). Do not hand-edit `[Unreleased]`.

- `cliff.toml` at the repo root configures the generator.
- `[Unreleased]` means "every user-visible conventional commit since the last **stable** tag." rc and nightly tags are transparent: they do not close the `[Unreleased]` window (`skip_tags` in `cliff.toml`).
- **Commit messages are load-bearing.** A `feat`, `fix`, or `perf` commit surfaces in the changelog; a `chore`, `ci`, `docs`, `test`, `refactor`, or `style` commit is dropped. Security-relevant changes use `fix(security):` or a `Security:` footer.
- The `[Unreleased]` block closes **only at stable**. When a stable tag is pushed, git-cliff renders the full set of user-visible commits since the previous stable as the release notes for that tag.
- Do not hand-rename `[Unreleased]` when cutting an RC or a nightly. The RC cut procedure is simpler under this model — see below.

`CHANGELOG.md` is kept in the repository and updated at release time by **prepending** the new version's section with `git-cliff --prepend`. It is never hand-edited and never fully regenerated — a full regeneration (`git-cliff -o CHANGELOG.md`) would collapse all historical sections into one range from the last stable tag, destroying the `## [0.6.0]` and `## [0.6.0-dev.N]` entries.

> **v0.7.0 transition:** git-cliff changelog generation applies from v0.7.0 onward. The `## [0.6.0]` and `## [0.6.0-dev.N]` sections are hand-written baselines committed to `CHANGELOG.md`. They must never be regenerated — doing so would duplicate or collapse them. The prepend workflow begins with the first v0.7.0 RC.

## Branches

| Branch         | Lifetime  | Accepts                                          | Tags produced            |
|----------------|-----------|--------------------------------------------------|--------------------------|
| `main`         | permanent | every new commit (features, fixes, refactors)    | (none — nightly rolling) |
| `release/vX.Y` | until EOL | cherry-picked fixes only (no new features)       | `vX.Y.Z-rc.N`, `vX.Y.Z`, `vX.Y.(Z+1)` |

### Inviolable rules

- A commit is **never** authored on a release branch. It always lands on `main` first, then is `git cherry-pick -x <sha>` into the release branch.
- A release branch is **never** merged back into `main`.
- **No new features** on a release branch once cut. Only bugfixes and the release's own version-artifact bumps.
- `main` is always open for development. There is no manual CHANGELOG entry required on `main` — commit messages carry the information.

## Tags

Tags must be annotated:

```bash
git tag -a vX.Y.Z[-suffix.N] -m "vX.Y.Z[-suffix.N]"
git push origin vX.Y.Z[-suffix.N]
```

The release workflow (`.github/workflows/release.yml`) classifies tags automatically:

| Tag pattern    | Allowed source branch | GitHub release kind |
|----------------|-----------------------|---------------------|
| `vX.Y.Z-rc.N`  | `release/vX.Y`        | prerelease          |
| `vX.Y.Z`       | `release/vX.Y`        | stable (published)  |
| anything else  | (safety net)          | draft               |

## Versioning Rules

The workspace version (`Cargo.toml` `[workspace.package].version`) is the source of truth. All other manifests must stay in lockstep.

**On `main`:**

The manifest version is a `-rc.0` marker for the next minor during the RC stabilization window, and is bumped to the next minor's base (`X.(Y+1).0`) only after the stable `vX.Y.0` ships. Between stable releases, `main` carries whatever the current minor's next version will be.

**On `release/vX.Y`:**

- Next RC: if the last tag is `vX.Y.Z-rc.N` → `-rc.(N+1)`. If none → `-rc.0`.
- Promote to stable: drop the RC suffix → `vX.Y.0`.
- Patch: increment `Z` → `vX.Y.(Z+1)`. Never bump the minor on a release branch.

## Cycle Example: `0.7.0`

1. Features land on `main`. No manual changelog entries required.
2. When ready to stabilize, cut `release/v0.7` from `main` HEAD.
   - Bump every versioned artifact to `0.7.0-rc.0`.
   - Commit on the release branch: `chore(release): cut release/v0.7 at v0.7.0-rc.0`.
   - Tag `v0.7.0-rc.0` on the release branch. git-cliff renders the unreleased range as the RC body automatically.
3. A bug is found during RC:
   - Commit the fix on `main`.
   - `git cherry-pick -x <sha>` into `release/v0.7`.
   - Bump to `v0.7.0-rc.1` and tag.
4. When clean, bump the release branch from `v0.7.0-rc.N` to `v0.7.0`. Tag `v0.7.0`. git-cliff renders the full unreleased range (since `v0.6.0`) as the stable release notes.
5. On `main`, bump to `v0.8.0-rc.0` and open the next cycle.
6. Patches (`v0.7.1`, `v0.7.2`, …) come from the same release branch via cherry-picks from `main`.

## Cut Procedure: `main` → `release/vX.Y`

1. Verify you are on `main`, clean tree, up to date with `origin/main`.
2. Verify `.github/workflows/release.yml` on `main` contains the `Classify release` job. If missing, fix on `main` first — otherwise stable tags will publish as drafts.
3. Create the branch (use a dedicated worktree if you use the bare-repo layout so `main` stays checked out):

   ```bash
   git worktree add ../release-vX.Y -b release/vX.Y main
   # or in a single-checkout repo:
   git checkout -b release/vX.Y
   ```

4. On `release/vX.Y`:
   - Bump every versioned artifact to `X.Y.0-rc.0` (see [Files to Bump](#files-to-bump)).
   - Prepend the new RC section to `CHANGELOG.md`:

     ```bash
     git-cliff --tag vX.Y.0-rc.0 --unreleased --prepend CHANGELOG.md
     git add CHANGELOG.md
     # fold into the same chore(release) commit as the version bump
     ```

     > **Warning:** do NOT use `git-cliff -o CHANGELOG.md`. That fully regenerates the file and collapses all historical sections since the last stable tag into a single block.

   - Commit: `chore(release): cut release/vX.Y at vX.Y.0-rc.0`.
   - Push: `git push -u origin release/vX.Y`.

5. Back on `main`:
   - Bump every versioned artifact to `X.Y.0-rc.0` (mirrors the release branch — main stays on the RC marker during the stabilization window).
   - Commit: `chore(release): align main to vX.Y.0-rc.0`.
   - Push.

6. Tag `vX.Y.0-rc.0` on the release branch.

There is **no CHANGELOG rename step** under the git-cliff model. The RC release body is generated from conventional commits automatically.

## Promote to Stable: `release/vX.Y` → `vX.Y.0`

Run on `release/vX.Y` when the RC is clean:

1. Bump every versioned artifact from `X.Y.0-rc.N` to `X.Y.0`.
2. Prepend the stable section to `CHANGELOG.md`:

   ```bash
   git-cliff --tag vX.Y.0 --unreleased --prepend CHANGELOG.md
   git add CHANGELOG.md
   # fold into the same chore(release) commit as the version bump
   ```

   > **Warning:** do NOT use `git-cliff -o CHANGELOG.md`. That fully regenerates the file and collapses all historical sections since the last stable tag into a single block.

3. Commit: `chore(release): promote release/vX.Y to vX.Y.0`.
4. Tag `vX.Y.0` on the release branch and push branch + tag.

git-cliff generates the curated release notes from all user-visible commits since the previous stable tag. There is no manual CHANGELOG curation step.

> **Optional curation:** if you want to add a human-written intro or editorial note to the stable release body, you can do so directly in the GitHub Release edit UI after the workflow publishes it. This does not touch CHANGELOG.md.

## Begin Next Dev Cycle: after stable `vX.Y.0`

Run this **only after** the stable `vX.Y.0` tag is pushed from `release/vX.Y`.

On `main`:
- Bump every versioned artifact to `X.(Y+1).0-rc.0`.
- Commit: `chore(version): begin X.(Y+1).0 cycle`.
- Push.

`main` now targets the next minor. Nightly builds continue from `main` HEAD automatically.

## Files to Bump

Per release, update all of the following to the exact same version:

- `Cargo.toml` — `[workspace.package].version`. Workspace crates inherit via `version.workspace = true`.
- `flake.nix`
- `resources/windows/installer.iss`
- Manual review (does not inherit): `examples/custom_driver/Cargo.toml`.

After the GitHub Release artifacts for the tag are published, also update:

- `nix/release-info.nix` — `version` + both prebuilt-tarball `url`s and `hash`es (see [Nix](#nix-this-repos-flake) below). This is a per-branch channel pointer. It requires the published artifacts, so it lands as a follow-up commit once the release workflow finishes.

The AUR `PKGBUILD` lives in an **external AUR repository**, not in this repo. It is bumped only for stable tags.

## How Nightly Works

`.github/workflows/nightly.yml` runs daily at 03:17 UTC:

1. Reads the workspace version from `Cargo.toml`, strips any existing pre-release suffix, and appends `-nightly+<short-sha>` (e.g. `0.7.0-nightly+abc1234`). No `Cargo.toml` commit required.
2. Calls `build.yml` with `channel: nightly`.
3. Computes the SHA256 SRI hash of each Linux tarball and regenerates `nix/nightly-info.nix` with the real hashes and the rolling release URLs.
4. Commits the updated `nix/nightly-info.nix` on top of the current `main` HEAD. This commit is **not pushed to `main`** — it becomes the sole target of the `nightly` tag.
5. Force-moves the `nightly` tag to the pin commit and pushes the tag. Pushing the tag is sufficient to make the commit reachable on the remote; no branch push is required.
6. Publishes or updates the rolling `nightly` GitHub prerelease with the new artifacts and a git-cliff-generated body covering commits since the last stable tag. The release's tag points at the pin commit, so `nix/nightly-info.nix` at `nightly` ref always matches the published artifacts.

The nightly tag is force-pushed and the release is replaced on every run. Only the canonical repository (`0xErwin1/dbflux`) runs the schedule.

**Skip when `main` has not advanced.** A scheduled run first compares the current `main` HEAD against the commit the last nightly was built from (`git rev-parse nightly^`, the pin commit's first parent). If they match, the run skips entirely: no rebuild, no tag move, no release churn. This avoids republishing an identical build under a fresh, non-reproducible hash that would needlessly break Nix pins. A manual `workflow_dispatch` run always builds, even with no new commits.

### Nix nightly package

The workflow pins `nix/nightly-info.nix` at the `nightly` ref on every run. Downstream users get the prebuilt nightly binary without compiling from source:

```bash
# Run nightly directly
nix run github:0xErwin1/dbflux/nightly#dbflux-nightly

# Install into a profile
nix profile install github:0xErwin1/dbflux/nightly#dbflux-nightly
```

A from-source nightly (no hash pinning required) also works:

```bash
nix run github:0xErwin1/dbflux/nightly#dbflux-source
```

**Do not consume `#dbflux-nightly` from `main`.** On `main`, `nix/nightly-info.nix` contains placeholder hashes that will not fetch. Always use the `nightly` ref as shown above.

## Cherry-Pick Discipline

A release branch should never contain commits absent from `main`, except release-only commits (`chore(release): ...`, `chore(version): ...`).

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

| Tag kind        | GitHub Release | AUR         | Nix flake (this repo)                         | nixpkgs (future) |
|-----------------|----------------|-------------|-----------------------------------------------|------------------|
| nightly         | prerelease     | skip        | auto-pinned — `#dbflux-nightly` on nightly ref | skip            |
| `-rc.N`         | prerelease     | skip        | bump release branch's + main's `release-info` | skip             |
| Stable `vX.Y.Z` | published      | bump + push | bump release branch's + main's `release-info` | bump + PR        |

### AUR

AUR `pkgver` does not allow `-` (reserved for `pkgrel`). For stable releases the translation is a no-op (`pkgver=X.Y.Z`). For hypothetical AUR prereleases:

- `vX.Y.Z-rc.N` → `pkgver=X.Y.Z.rc.N`

### Nix (this repo's flake)

The flake exposes several packages on Linux (x86_64 and aarch64):

| Package           | What it provides                                     |
|-------------------|------------------------------------------------------|
| `dbflux` (default) | Prebuilt stable/rc binary when available, source otherwise |
| `dbflux-bin`      | Explicit prebuilt from `nix/release-info.nix`        |
| `dbflux-source`   | Source build via crane (all platforms)               |
| `dbflux-nightly`  | Rolling nightly prebuilt from `nix/nightly-info.nix` (use `nightly` ref) |

**Stable / RC (`nix/release-info.nix`):** per-branch channel pointer. `main` tracks the newest published tag of any kind; each `release/vX.Y` tracks its own line's newest. After a tag's artifacts publish, refresh `release-info.nix` on every branch whose channel that tag advances.

```bash
ver=X.Y.Z
for arch in amd64 arm64; do
  hex=$(curl -fsSL "https://github.com/0xErwin1/dbflux/releases/download/v$ver/dbflux-linux-$arch.tar.gz.sha256" | awk '{print $1}')
  nix-hash --to-sri --type sha256 "$hex"
done
```

Update `version`, both `url`s, and both `hash`es in `nix/release-info.nix`. Verify locally:

```bash
nix build .#dbflux-bin --no-link --print-out-paths
```

**Nightly (`nix/nightly-info.nix`):** auto-updated by the nightly workflow on the `nightly` ref. Do not update this file manually. Consume via:

```bash
nix run github:0xErwin1/dbflux/nightly#dbflux-nightly
```

### nixpkgs (future)

Not yet upstream. When it is, only stable tags will get a PR to `NixOS/nixpkgs`. PR title convention: `dbflux: A -> B`.

## Anti-Patterns (refuse these)

- Tagging `vX.Y.Z` or `vX.Y.Z-rc.N` while HEAD is on `main`.
- Tagging an RC while HEAD is on `main`.
- Merging `release/vX.Y` back into `main`.
- Creating new features (non-fix commits) on a `release/*` branch.
- Bumping minor or major version inside a `release/*` branch.
- Pushing a tag without a clean working tree.
- Pushing the AUR bump with `pkgver` containing a hyphen.
- Cutting `release/vX.Y` from a `main` HEAD that does not contain the `Classify release` job in `release.yml`.
- Creating new `-dev.N` tags (the channel is retired; use nightly instead).

## Local Validation Before Tagging

```bash
cargo check --workspace
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

## Related

- `.github/workflows/release.yml` — classification logic and artifact publishing
- `.github/workflows/nightly.yml` — daily nightly build
- `.github/workflows/build.yml` — reusable build jobs (called by release and nightly)
- `.github/release-template.md` — installation section appended to every release body
- `cliff.toml` — git-cliff configuration for changelog generation
- `skills/dbflux-release/SKILL.md` — agent-facing skill that automates this process
