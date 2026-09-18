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

- Issues are created from the forms in `.github/ISSUE_TEMPLATE/`: `bug_report.yml` for a defect, `feature_request.yml` for a capability. They are the only issue templates in the repo.
- A form supplies its own title prefix (`[bug] ` or `[feature] `) and renders each field as a `### <label>` section, ending with a `Labels I plan to apply` checklist.
- `CONTRIBUTING.md` defines the contributor workflow (`## Issues`) and the label taxonomy (`## Label Guide`).
- Labels live on the upstream repository, so a reporter without write access cannot apply them from a fork: fill in the label section the form provides, name the labels the issue needs, and ask a maintainer to apply them during triage.
- Use only labels from the taxonomy (`*:bug` / `*:feature` per area, driver, platform, data-model kind). There is no `area:*` convention.

## Critical Patterns

- Search for duplicates before creating a new issue
- Choose the form that matches the request: a defect goes to `bug_report.yml`, a new capability to `feature_request.yml`. Do not write a free-form body that skips the form's fields.
- Fill every required field. `bug_report.yml` asks for the DBFlux version, operating system, OS detail, architecture, the affected driver, reproducible steps, expected and actual behavior, and logs; `feature_request.yml` asks for the problem, the proposed solution, alternatives considered, and the primary area.
- Keep the form's shape when filing through `gh` (the CLI shows no form): keep the field labels as `### <label>` headings, and keep the `Labels I plan to apply` checklist in the body.
- Keep the issue grounded in observed repo behavior, failing commands, or a concrete user/problem statement
- Do not require `status:approved`, `status:needs-review`, or any other label before work can start
- Do not invent milestones, assignees, labels, or project board fields unless the user explicitly asks
- If key reproduction details or scope are missing, ask one concise follow-up question instead of guessing

## Composing the Body

A GitHub issue form turns each field into a `### <label>` section and each checkbox list into a Markdown checklist, so the filed issue keeps that shape. Compose the body from the form's YAML rather than from memory — read `.github/ISSUE_TEMPLATE/bug_report.yml` or `feature_request.yml` and write the fields in the order it declares them.

A bug report has: `DBFlux version`, `Operating system`, `OS / distro detail`, `Architecture`, `Affected driver (if any)`, `Steps to reproduce`, `Expected behavior`, `Actual behavior`, `Logs / stack traces`, `Labels I plan to apply`.

A feature request has: `Problem`, `Proposed solution`, `Alternatives considered`, `Primary area`, `Labels I plan to apply`.

## Commands

```bash
# Search for possible duplicates
gh issue list --search "keyword"

# Read the fields of the form being used
sed -n '1,200p' .github/ISSUE_TEMPLATE/bug_report.yml

# Create the issue: the title carries the form's prefix, the body its fields
gh issue create --repo 0xErwin1/dbflux --title "[bug] <summary>" --body-file /tmp/issue.md
```

## Resources

- `.github/ISSUE_TEMPLATE/bug_report.yml`
- `.github/ISSUE_TEMPLATE/feature_request.yml`
- `CONTRIBUTING.md`
- `AGENTS.md`
- `README.md`
- `ARCHITECTURE.md`
- `CODE_STYLE.md`
