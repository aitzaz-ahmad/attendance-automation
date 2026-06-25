# Branch Strategy

## Status

Accepted

## Lifecycle

Active

## Primary Branches

### main

Production-quality stable branch.

### development

Primary integration branch for ongoing implementation work.
All issue branches must originate from `development`.

---

## Feature Branch Naming

Branch format:

    ETLP-<issue_number>

Examples:

    ETLP-6
    ETLP-21
    ETLP-33

Rules:

- one feature branch per GitHub issue
- avoid multi-issue branches
- avoid long-lived divergent branches
- branch names must preserve the exact GitHub issue identifier

---

## Pull Request Rules

- feature branches target `development`
- avoid direct commits to `main`
- avoid unrelated scope inside the same PR
- preserve deterministic history
- validation must pass before merge
