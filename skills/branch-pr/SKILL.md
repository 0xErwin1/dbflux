---
name: branch-pr
description: >
  Create pull requests for DBFlux using the repository's actual PR template and review expectations.
  Trigger: When creating a pull request, opening a PR, or preparing a branch for review.
license: MIT
---

## When to Use

- Opening a new PR for DBFlux
- Preparing branch changes for review
- Turning local work into a reviewable GitHub PR

## Repository Facts

- The PR template lives at `.github/pull_request_template.md`
- The template sections are: `Summary`, `What does this resolve?`, `How was this solved?`, `Validation`, `Where was this tested?`, and `Checklist`
- The PR template does not define or require labels
- This repository does not appear to use an `area:*` label convention; live GitHub labels are domain-based (`mcp`, `query`, `aws`, `ssh`, `proxy`, `driver`, `driver:<backend>`, plus `:bug` / `:feature` variants)
- Do not assume issue-first enforcement, approval labels, or mandatory PR labels unless the user explicitly asks for them
- Use project guidance from `AGENTS.md`, `ARCHITECTURE.md`, `CODE_STYLE.md`, and `CLAUDE.md`

## Critical Patterns

- Inspect `git status`, branch state, recent commits, and the diff against the base branch before drafting the PR
- Base the PR description on the full branch delta, not only on the latest commit
- Follow the repository template sections exactly when composing the PR body
- In `Validation`, include only commands or scenarios that were actually run
- In `Where was this tested?`, mark only real environments; do not claim coverage that did not happen
- Reference linked issues only when they actually exist; do not invent issue requirements
- Do not auto-apply or mention `area:*` labels in PR creation
- Only suggest or apply labels when the user explicitly asks, and only from the repository's existing GitHub labels
- Push with `git push -u origin <branch>` if the branch is not yet tracked

## Suggested PR Body

```markdown
## Summary

- Brief summary of the change

## What does this resolve?

- Resolves #123

## How was this solved?

Short explanation of the approach and tradeoffs.

## Validation

- `cargo check --workspace`
- Manual scenario: ...

## Where was this tested?

- [x] Local development environment
- [ ] Automated tests
- [ ] Linux
- [ ] macOS
- [ ] Windows
- [ ] X11
- [ ] Wayland
- [ ] Other:

## Checklist

- [x] I verified the change against the affected user flow(s)
- [x] I added or updated tests when needed
- [x] I documented follow-up work or known limitations when applicable
```

## Commands

```bash
# Inspect branch state before creating the PR
git status
git diff --stat
git log --oneline --decorate --graph origin/main..HEAD

# Push branch if needed
git push -u origin <branch>

# Create the PR
gh pr create
```

## Resources

- `.github/pull_request_template.md`
- `AGENTS.md`
- `ARCHITECTURE.md`
- `CODE_STYLE.md`
- `CLAUDE.md`
