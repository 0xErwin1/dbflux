# Skill Registry

**Delegator use only.** Any agent that launches sub-agents reads this registry to resolve compact rules, then injects them directly into sub-agent prompts. Sub-agents do NOT read this registry or individual SKILL.md files.

## User Skills

| Trigger | Skill | Path |
|---------|-------|------|
| When creating a pull request | branch-pr | /home/iperez/.config/opencode/skills/branch-pr/SKILL.md |
| When user asks to create a release, bump version, or tag | dbflux-release | /home/iperez/.config/opencode/skills/dbflux-release/SKILL.md |
| When writing Go tests, using teatest, or adding test coverage | go-testing | /home/iperez/.config/opencode/skills/go-testing/SKILL.md |
| When creating a GitHub issue, reporting a bug, or requesting a feature | issue-creation | /home/iperez/.config/opencode/skills/issue-creation/SKILL.md |
| When user says "judgment day", "review adversarial", "dual review" | judgment-day | /home/iperez/.config/opencode/skills/judgment-day/SKILL.md |
| When the orchestrator launches to implement tasks from SDD | sdd-apply | /home/iperez/.config/opencode/skills/sdd-apply/SKILL.md |
| When the orchestrator launches to archive a completed SDD change | sdd-archive | /home/iperez/.config/opencode/skills/sdd-archive/SKILL.md |
| When the orchestrator launches to write technical design | sdd-design | /home/iperez/.config/opencode/skills/sdd-design/SKILL.md |
| When the orchestrator launches to explore or investigate ideas | sdd-explore | /home/iperez/.config/opencode/skills/sdd-explore/SKILL.md |
| When user wants to initialize SDD in a project | sdd-init | /home/iperez/.config/opencode/skills/sdd-init/SKILL.md |
| When the orchestrator launches to create a change proposal | sdd-propose | /home/iperez/.config/opencode/skills/sdd-propose/SKILL.md |
| When the orchestrator launches to write SDD specifications | sdd-spec | /home/iperez/.config/opencode/skills/sdd-spec/SKILL.md |
| When the orchestrator launches to break down a change into tasks | sdd-tasks | /home/iperez/.config/opencode/skills/sdd-tasks/SKILL.md |
| When the orchestrator launches to verify a completed change | sdd-verify | /home/iperez/.config/opencode/skills/sdd-verify/SKILL.md |
| When user asks to create a new skill or add agent instructions | skill-creator | /home/iperez/.config/opencode/skills/skill-creator/SKILL.md |
| When user says "update skills" or after installing/removing skills | skill-registry | /home/iperez/.config/opencode/skills/skill-registry/SKILL.md |

## Compact Rules

### branch-pr
- Use issue-creation skill BEFORE creating any PR (issue-first enforcement)
- Never create PRs without linked issue
- Follow conventional commits: feat/, fix/, docs/, refactor/, test/, chore/

### dbflux-release
- Bump version in ALL Cargo.toml files across workspace
- Update CHANGELOG.md with version and date
- Create git tag after all changes committed
- Run tests before tagging

### go-testing
- Use teatest for Bubbletea TUI testing
- Use hammer for integration tests
- Follow Go naming conventions: TestXxx, BenchmarkXxx

### judgment-day
- Launch TWO independent blind judge sub-agents simultaneously
- Synthesize findings, apply fixes, re-judge until both pass
- Escalate after 2 iterations if not passing

### sdd-apply
- Read design artifact before implementing
- Implement one task at a time
- Run cargo check after each crate change
- Never break the build - cargo check must pass

### sdd-archive
- Verify all phases complete before archiving
- Update main specs with delta specs
- Clean up change directory after archiving

### sdd-design
- Include architecture decision rationale
- Document error handling approach
- Include sequence diagrams for complex flows
- Reference spec requirements explicitly

### sdd-explore
- Research codebase before proposing changes
- Identify affected modules/packages
- Document findings as exploration artifact
- Do NOT make implementation decisions in explore

### sdd-init
- Scan project stack from package files
- Build skill registry from skills directories
- Create .atl/ directory for skill registry
- Persist context to engram

### sdd-propose
- Include intent, scope, and approach
- Identify risks and rollback plan
- Reference existing specs/designs when applicable
- Use proposal artifact type

### sdd-spec
- Use Given/When/Then format for scenarios
- Use RFC 2119 keywords (MUST, SHALL, SHOULD, MAY)
- Cover happy path AND error cases
- Each spec domain gets its own section

### sdd-tasks
- Group by phase: infrastructure, implementation, testing
- Use hierarchical numbering (1.1, 1.2, etc.)
- Keep tasks small enough to complete in one session
- Identify dependencies between tasks

### sdd-verify
- Compare implementation against every spec scenario
- Run tests if test infrastructure exists
- Check cargo clippy warnings
- Document any gaps found

### skill-creator
- Follow Agent Skills spec format
- Include trigger in description
- Define clear patterns and rules
- Test skill before finalizing

### skill-registry
- Scan all skill directories for updates
- Generate compact rules (5-15 lines per skill)
- Write .atl/skill-registry.md (mandatory)
- Also save to engram when available

## Project Conventions

| File | Path | Notes |
|------|------|-------|
| AGENTS.md | ./AGENTS.md | Main project conventions |
| Project Skills | ~/.config/opencode/skills/ | specific skills |

Read the convention files listed above for project-specific patterns and rules.
