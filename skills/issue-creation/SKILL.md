---
name: issue-creation
description: >
  Create GitHub issues for DBFlux using the repository's real conventions.
  Trigger: When creating a GitHub issue, reporting a bug, requesting a feature, or documenting work that needs tracking.
license: MIT
---

## When to Use

- Creating a bug report for DBFlux
- Proposing a feature or improvement
- Opening a tracking issue before implementation work

## Repository Facts

- There is no `.github/ISSUE_TEMPLATE/` directory in this repo
- There is no `CONTRIBUTING.md` defining an issue workflow
- The repository does not document an `area:*` label taxonomy in `.github/` or docs
- Live GitHub labels are domain-based, including `mcp`, `query`, `aws`, `ssh`, `proxy`, `driver`, variants like `:bug` / `:feature`, and driver labels such as `driver:postgres` and `driver:sqlite`
- Do not assume approval labels, mandatory labels, or template-driven issue creation
- Use repository docs as context: `README.md`, `AGENTS.md`, `ARCHITECTURE.md`, `CODE_STYLE.md`

## Critical Patterns

- Search for duplicates before creating a new issue
- Use `gh issue create` with a manually written body; do not reference non-existent templates
- Keep the issue grounded in observed repo behavior, failing commands, or a concrete user/problem statement
- Do not require `status:approved`, `status:needs-review`, or any other label before work can start
- Do not invent `area:*` labels such as `area:ui` or `area:mcp`
- If labels are requested, use only labels that actually exist in GitHub for this repo, preferably confirmed with `gh label list`
- Do not invent milestones, assignees, labels, or project board fields unless the user explicitly asks
- If key reproduction details or scope are missing, ask one concise follow-up question instead of guessing

## Suggested Issue Body

```markdown
## Summary

Short description of the request or bug.

## Problem

What is broken, missing, or painful today?

## Proposed Direction

What should change? For bugs, describe the expected behavior. For features, describe the desired outcome.

## Validation / Impact

How can we confirm the issue is solved, and who is affected?
```

## Commands

```bash
# Search for possible duplicates
gh issue list --search "keyword"

# Create an issue interactively
gh issue create

# Create an issue with a title and body file
gh issue create --title "fix(scope): short summary" --body-file /tmp/issue.md
```

## Resources

- `README.md`
- `AGENTS.md`
- `ARCHITECTURE.md`
- `CODE_STYLE.md`
