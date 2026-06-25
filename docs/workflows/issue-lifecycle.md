# Issue Lifecycle

## Status

Accepted

## Lifecycle

Active

## Workflow States

GitHub Project issues move through the following lifecycle:

- Backlog
- Todo
- In Progress
- Review
- Done
- Blocked

## Workflow Rules

### Backlog

The issue exists but is not yet prioritized for implementation.

### Todo

The issue is implementation-ready.

Requirements:

- scope is sufficiently defined
- milestone assignment exists
- labels are correct
- implementation dependencies are understood

### In Progress

Implementation work is actively underway.

Requirements:

- create a dedicated feature branch from `development`
- branch naming must follow repository branch strategy
- implementation must remain issue-scoped

### Review

Implementation is complete and awaiting review.

Requirements:

- all validation commands pass
- commit message contract is satisfied
- no unrelated files modified
- documentation updated where applicable

### Done

The issue has been validated and merged.

### Blocked

The issue cannot progress due to dependency, ambiguity, or external constraint.

Blocked issues should explicitly document:

- blocker source
- required resolution
- dependent issue or subsystem
