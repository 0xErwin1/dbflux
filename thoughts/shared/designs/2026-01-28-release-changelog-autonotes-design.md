---
title: PR-Based Release Notes Auto-Generation
date: 2026-01-28
status: validated
author: DBFlux Team
---

## Overview

This design proposes switching the DBFlux release notes generation from git-log based changelogs to GitHub's PR-based auto-generated release notes. The approach preserves the ability to add human-written prefatory content (installation instructions, project context) while leveraging GitHub's built-in PR summarization.

## Goals

- Automate release notes generation based on merged pull requests.
- Reduce manual maintenance of changelog formatting and PR tracking.
- Provide structured, contributor-attributed release notes.
- Preserve human-written preface with installation instructions for both linux-amd64 and linux-arm64.
- Maintain existing GitHub Actions workflow structure (build artifacts + release job).

## Problem

The current release workflow generates notes manually using `git log` to extract commit messages. This produces a simple bullet list but lacks PR-specific context like titles, numbers, and contributor attribution.

GitHub provides `generate_release_notes: true` in `softprops/action-gh-release`, which produces PR-based notes automatically. However, this option overrides the `body` parameter entirely, making it impossible to prepend a custom preface (installation instructions, context, etc.).

## Approaches

### Approach 1: Use softprops `generate_release_notes` with post-release comment (rejected)

Call GitHub's auto-notes generation during release creation, then add a comment to the release with the preface and installation instructions after creation.

**Pros:**
- Minimal workflow changes.
- Uses `generate_release_notes` directly.

**Cons:**
- Preface appears as a comment, not part of the release body.
- Users may miss installation instructions buried in comments.
- Inconsistent formatting—release body vs comment.
- Poor user experience; installation guidance should be in the release body.

**Rejected** because installation instructions must be visible directly in the release body for clear guidance.

### Approach 2: Create draft release with auto-notes, then update body (rejected)

Create the release as a draft with `generate_release_notes: true`, read the generated body back via GitHub API, compose the final body (preface + generated notes), and update the release before publishing.

**Pros:**
- Keeps preface in release body.
- Uses GitHub's auto-notes generation.

**Cons:**
- Requires extra API calls (create draft → read → update).
- Complex workflow with multiple steps and error handling.
- Race conditions if multiple releases triggered concurrently.
- Draft creation may cause confusion if update fails.

**Rejected** due to unnecessary complexity and fragility.

### Approach 3: Call GitHub API directly to fetch notes, compose body, create release (chosen)

Call GitHub's "generate release notes" REST API endpoint (or equivalent) to obtain the PR-based notes text as a string. Then compose the final release body in the workflow as: Preface (installation instructions + context) + separator + generated notes. Finally, create the release with the composed body as a draft, then publish.

**Pros:**
- Full control over release body composition.
- Preface and auto-notes both in release body.
- Single release creation step (no draft-update dance).
- Clean, straightforward workflow.
- API approach is flexible and can be adapted if GitHub's tooling changes.

**Cons:**
- Requires an additional API call before release creation.
- Must handle API errors and missing response data.

**Chosen** because it provides the best user experience (preface visible in release body) with minimal workflow complexity.

## Implementation Details

### Chosen Approach: API-Based Notes Composition

#### Preface Content

The preface will include:

- Release title and version.
- Installation instructions for both linux-amd64 and linux-arm64.
- Download and verification steps using SHA256 checksums.
- Brief project context (e.g., "keyboard-first database client").
- System requirements (Linux x86_64/ARM64, no external dependencies).

#### Notes Generation

Use GitHub's API to fetch auto-generated release notes between tags:

```bash
curl \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $GITHUB_TOKEN" \
  https://api.github.com/repos/${{ github.repository }}/releases/generate-notes \
  -d '{"tag_name": "${{ steps.version.outputs.version }}", "previous_tag_name": "$PREV_TAG"}'
```

The API returns a `body` field with the PR-based notes in GitHub's markdown format.

#### Body Composition

Compose the final release body as:

```
## DBFlux $VERSION

### Installation

#### Linux (x86_64)

```bash
# Download and extract
wget https://github.com/owner/repo/releases/download/$VERSION/dbflux-linux-amd64.tar.gz
tar -xzf dbflux-linux-amd64.tar.gz

# Run installer
sudo ./scripts/install.sh
```

#### Linux (ARM64)

```bash
# Download and extract
wget https://github.com/owner/repo/releases/download/$VERSION/dbflux-linux-arm64.tar.gz
tar -xzf dbflux-linux-arm64.tar.gz

# Run installer
sudo ./scripts/install.sh
```

### Checksums

Verify download integrity using the provided SHA256 checksums:

```bash
sha256sum -c dbflux-linux-amd64.tar.gz.sha256
sha256sum -c dbflux-linux-arm64.tar.gz.sha256
```

---

$GENERATED_NOTES
```

#### Release Creation

Create the release with the composed body as a draft to allow final inspection, then publish. Use `softprops/action-gh-release` with `generate_release_notes: false` and `draft: true` (initially). After creation, update `draft: false` to publish, or use `draft: false` directly if confidence is high.

#### Workflow Changes

Modify the `release` job in `.github/workflows/release.yml`:

1. Add a step to call GitHub's `releases/generate-notes` API.
2. Store the returned `body` in a workflow output.
3. Compose the final body with preface + separator + notes.
4. Pass the composed body to `softprops/action-gh-release` via `body` parameter.
5. Set `generate_release_notes: false`.

#### Error Handling

- If API call fails, fall back to simple git-log based notes (current behavior).
- If API returns empty or malformed body, use fallback notes and log a warning.
- Ensure `GITHUB_TOKEN` has `contents:write` permission (already present in workflow).

## Future Enhancements

- Allow custom preface content via a file in the repository (e.g., `RELEASE_PREFACE.md`).
- Support for release-specific custom notes in addition to auto-generated ones.
- Automated release announcement to other channels (e.g., Twitter, blog).
- Integration with issue trackers for referencing fixed issues in notes.
