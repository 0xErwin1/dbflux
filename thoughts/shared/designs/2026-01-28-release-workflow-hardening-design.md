# Release Workflow Hardening Design

**Status**: `validated`
**Date**: 2026-01-28
**Author**: System
**Related Files**: `.github/workflows/release.yml`

## Problem Statement

The current GitHub Actions release workflow has several issues that prevent reliable releases:

1. **Incomplete aarch64 cross-compilation**: The workflow installs `gcc-aarch64-linux-gnu` but fails to install required cross-compilation libc headers and does not configure the target linker environment, causing linking failures.

2. **Shared cargo build cache**: The workflow caches the entire `target/` directory, which is shared between matrix targets (x86_64 and aarch64). This causes cache conflicts since different targets produce incompatible binary artifacts in the same cache key namespace.

3. **Redundant conditional logic**: The build step contains an if/else block with identical cargo build commands for both branches, creating unnecessary complexity.

4. **Fragile release notes generation**: Release notes are constructed via bash string concatenation and the previous tag selection logic can be unstable, potentially failing or producing incorrect output in edge cases.

## Constraints

- Must maintain existing job structure (build job produces artifacts; release job depends on build)
- Must continue to build both x86_64 and aarch64 Linux targets
- Must preserve artifact generation format (tar.gz and SHA256 checksums)
- Must continue to support both tag-triggered and manual workflow dispatch releases
- Must work with ubuntu-latest runners only (no additional OS costs)
- Must not introduce breaking changes to the release process

## Approach

Fix the release workflow through targeted, minimal changes:

1. **Complete aarch64 cross-compilation**: Install required cross libc headers (`libc6-dev-arm64-cross`) and configure the target linker environment (`CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc`) to enable successful cross-compilation.

2. **Fix cargo build caching**: Cache only the per-target directory (`target/${{ matrix.target }}`) instead of the entire `target/` directory. This prevents cache pollution between different architecture targets while still providing build speedups.

3. **Remove redundant conditional**: Simplify the build step to a single cargo build command without the if/else block, as both branches execute identical commands.

4. **Robust release notes generation**: Replace bash string concatenation with a single heredoc multiline output. Improve previous-tag selection by using `git describe --tags --abbrev=0 --exclude="*"` which handles edge cases more reliably, and fall back to the first commit if no previous tag exists.

## Architecture

The workflow maintains its two-job structure:

```
┌─────────────────────────────────────────┐
│         build (matrix: 2 jobs)          │
│  ┌───────────────────────────────────┐  │
│  │ x86_64-unknown-linux-gnu         │  │
│  ├───────────────────────────────────┤  │
│  │ aarch64-unknown-linux-gnu         │  │
│  │ (fixed: libc headers + linker)    │  │
│  └───────────────────────────────────┘  │
│         ↓ artifacts                      │
└─────────────────────────────────────────┘
                ↓
┌─────────────────────────────────────────┐
│            release                      │
│  • Download artifacts                   │
│  • Generate notes (fixed: heredoc)      │
│  • Create GitHub release                │
└─────────────────────────────────────────┘
```

## Components

### Build Job Matrix

**x86_64-unknown-linux-gnu** (no changes):
- Uses native toolchain
- Caches `target/x86_64-unknown-linux-gnu`

**aarch64-unknown-linux-gnu** (fixed):
- Installs `gcc-aarch64-linux-gnu` and `libc6-dev-arm64-cross`
- Sets `CC_aarch64_unknown_linux_gun=aarch64-linux-gnu-gcc`
- Caches `target/aarch64-unknown-linux-gnu`

### Cache Strategy

**Before**: Single cache key `target` shared across matrix
```
target/
├── x86_64-unknown-linux-gnu/
└── aarch64-unknown-linux-gnu/
```

**After**: Per-target cache keys
```
Cache key 1: target/x86_64-unknown-linux-gnu
Cache key 2: target/aarch64-unknown-linux-gnu
```

### Release Notes Generation

**Before**: Bash concatenation
```bash
NOTES="### Changelog\n\nChanges since $PREV_TAG:\n\n"
NOTES+=$(git log $PREV_TAG..HEAD --pretty=format:"- %s" | head -20)
echo "notes<<EOF" >> $GITHUB_OUTPUT
echo "$NOTES" >> $GITHUB_OUTPUT
echo "EOF" >> $GITHUB_OUTPUT
```

**After**: Single heredoc
```bash
NOTES=$(cat <<'EOF'
### Changelog

Changes since $PREV_TAG:

$(git log $PREV_TAG..HEAD --pretty=format:"- %s" | head -20)
EOF
)
echo "notes<<EOF" >> $GITHUB_OUTPUT
echo "$NOTES" >> $GITHUB_OUTPUT
echo "EOF" >> $GITHUB_OUTPUT
```

## Data Flow

1. **Trigger**: Tag push (`v*.*.*`) or workflow_dispatch with version input
2. **Build (parallel)**:
   - Checkout code
   - Install Rust toolchain with target
   - Install cross-compilation tools (aarch64 only)
   - Restore per-target cargo cache
   - Build release binary
   - Package and upload artifacts
3. **Release**:
   - Download all build artifacts
   - Determine version (input or tag)
   - Generate release notes from git history
   - Create GitHub release with artifacts and notes

## Error Handling

### Cross-compilation Failures
- **Detection**: Cargo will fail at linking stage if libc headers or linker are misconfigured
- **Mitigation**: Explicitly install required packages and set CC environment variable before build
- **No fallback**: Fail fast with clear error from cargo

### Cache Key Conflicts
- **Detection**: Cache hits for wrong target would cause "file not found" or corrupted build artifacts
- **Mitigation**: Use per-target cache paths to isolate caches completely
- **No fallback**: If cache is corrupted, rebuild from scratch

### Release Notes Edge Cases
- **First release**: No previous tag exists → `git describe` fails → use first commit as base
- **Empty history**: No commits since previous tag → produce "No changes" message
- **Invalid tags**: Tags not matching `v*.*.*` are ignored by `--exclude="*"` to avoid confusion

## Testing Strategy

### Manual Testing
1. Trigger workflow via `workflow_dispatch` with test version
2. Verify both matrix targets complete successfully
3. Check that aarch64 binary is correctly cross-compiled (file output)
4. Verify generated release notes contain correct commit history
5. Test first release scenario (no previous tags) manually
6. Test subsequent release to verify previous-tag selection

### Automated Validation
- The fixed workflow will be validated by running a test release (or dry-run if possible)
- Confirm aarch64 binary links successfully
- Verify cache keys are isolated between targets
- Check release notes output format

### Smoke Tests After Deploy
- Run actual tag-triggered release
- Verify both artifacts download and install correctly
- Check SHA256 checksums match
- Confirm release notes display properly on GitHub

## Open Questions

None. All issues are well-understood with clear fixes.
