# AI Agent Operating Instructions

This repository is designed for AI-assisted software engineering workflows.
Agents must preserve repository contracts, avoid unreviewed scope expansion,
and keep changes deterministic and issue-scoped.

---

## Context Loading Strategy

Do not load all repository documents at task start.

Context discovery must be incremental and task-driven.

Only load documents that are directly relevant to the current issue,
subsystem, workflow step, or implementation task.

Agents must load `AGENTS.md` first.

For documentation work, agents must load
`docs/governance/documentation.md` before editing or auditing
documentation.

After that, load additional documents incrementally according to the task.

Guidelines:

- Start with the assigned GitHub issue and directly referenced files.
- Load architecture or workflow documents only when required.
- Avoid recursively reading entire directories.
- Avoid loading unrelated proposals, ADRs, or workflow documents.
- Prefer minimal sufficient context.
- Expand context only when implementation uncertainty requires it.
- Do not assume cross-repository or hidden context.

Examples:

- A transformation-layer issue should not load messaging or storage documents.
- A documentation-only issue should not load implementation modules unnecessarily.
- A workflow-related task may require workflow docs but not architecture docs.

The goal is to minimize irrelevant context, reduce token usage,
and prevent architectural drift from unrelated repository areas.

---

## Documentation Governance

`docs/governance/documentation.md` is the canonical authority for
documentation governance.

Documentation-related tasks must follow the ownership model, lifecycle rules,
audit rules, and status taxonomy defined there. `AGENTS.md` intentionally
provides only operational guidance and must not duplicate governance policy.

---

## Required Context Discovery

Before starting work, discover only the minimum documentation required for the
assigned task. Context loading must remain incremental and task-driven.

### Workflow Documents

- `docs/workflows/issue-lifecycle.md`
- `docs/workflows/branch-strategy.md`
- `docs/workflows/commit-conventions.md`
- `docs/workflows/validation-gates.md`

### Architecture Documents

- `README.md`

### Planning Documents

- `README.md`
- GitHub Issues
- GitHub Milestones

---

## Core Rules

- Keep changes minimal and issue-scoped.
- Do not widen implementation scope beyond the assigned issue.
- Do not invent behavior when repository contracts are missing or ambiguous.
- Preserve deterministic behavior and output formats.
- Prefer explicit validation failures over implicit fallback behavior.
- Do not silently rewrite repository conventions.
- Do not invent new governance rules, lifecycle states, documentation ownership
  rules, or repository conventions unless explicitly requested.
- Preserve documented architecture boundaries.
- For documentation work, treat `docs/governance/documentation.md` as the
  canonical governance authority.

---

## Validation Requirements

Refer to:

- `docs/workflows/validation-gates.md`

Agents must:

- run all required validation commands defined by repository workflow contracts
- report validation failures explicitly
- include an acceptance-criteria audit before reporting issue completion

Documentation-only tasks that modify Markdown links should validate internal
Markdown links before reporting completion.

---

## GitHub Safety Rules

- Do not perform destructive GitHub operations unless explicitly instructed.
- Prefer dry-run workflows whenever available.
- Keep reports human-reviewable and machine-diffable.

---

## Implementation Discipline

- Preserve backward compatibility unless explicitly allowed by the issue.
- Avoid hidden fallback behavior.
- Prefer explicit metadata contracts over inference.
- Preserve deterministic ordering where applicable.

---

## Branch And Commit Discipline

Refer to:

- `docs/workflows/branch-strategy.md`
- `docs/workflows/commit-conventions.md`
