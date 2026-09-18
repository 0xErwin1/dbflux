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
- The template sections are: `Summary`, `What does this resolve?`, `How was this solved?`, `Validation`, `Where was this tested?`, `Checklist`, and `Labels to apply`
- The checklist in the template has five items, including the `CHANGELOG.md` and label ones — carry all of them over
- `Labels to apply` is part of the template: it is where the PR names the labels the change needs
- Labels come from the taxonomy in `CONTRIBUTING.md` § Label Guide (`ui:bug`, `ui:feature`, `driver:bug`, `driver:postgres`, `platform:linux`, `kind:sql`, …); there is no `area:*` convention
- Labels live on the upstream repository, so a contributor pushing from a fork cannot apply them: state them in the body and let a maintainer apply them during triage
- Do not assume issue-first enforcement or approval labels unless the user explicitly asks for them
- Use project guidance from `AGENTS.md`, `ARCHITECTURE.md`, and `CODE_STYLE.md`

## Critical Patterns

- Inspect `git status`, branch state, recent commits, and the diff against the base branch before drafting the PR
- Base the PR description on the full branch delta, not only on the latest commit
- Follow the repository template sections exactly when composing the PR body
- In `Validation`, include only commands or scenarios that were actually run
- In `Where was this tested?`, mark only real environments; do not claim coverage that did not happen
- Reference linked issues only when they actually exist; do not invent issue requirements
- Do not invent labels such as `area:*`; take them from the taxonomy in `CONTRIBUTING.md`
- State the labels the change needs in `Labels to apply`, and do not silently drop the section when the author has no write access to apply them
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
- [x] I updated `CHANGELOG.md` under `## [Unreleased]` if this is user-visible
- [x] I applied the appropriate labels (see `CONTRIBUTING.md` § Label Guide)

## Labels to apply

`ui:feature`, `platform:linux`
```

Pushing from a fork, the last checklist item cannot be ticked by the author: say so where the PR states its labels, and ask a maintainer to apply them.

## Commands

```bash
# Inspect branch state before creating the PR
git status
git diff --stat
git log --oneline --decorate --graph origin/main..HEAD

# Push branch if needed (to the fork when the author has no write access upstream)
git push -u origin <branch>

# Create the PR against the upstream repository
gh pr create --repo 0xErwin1/dbflux --base main --head <owner>:<branch> --body-file /tmp/pr.md
```

## Resources

- `.github/pull_request_template.md`
- `.github/ISSUE_TEMPLATE/`
- `CONTRIBUTING.md`
- `AGENTS.md`
- `ARCHITECTURE.md`
- `CODE_STYLE.md`
