# Validation Gates

All required validation commands must pass before implementation completion,
commit creation, or pull request review.

## Required Commands

Always run:

    git diff --check

Pending validation commands:

    ./scripts/validate.sh
    # Pending: ETLP-8 / ETLP-9
    # Enable once the CI and validation tooling are implemented.

    .venv/bin/python -m pytest -q
    # Pending: ETLP-7
    # Enable once the repository test structure is implemented.

    .venv/bin/python -m ruff check .
    # Pending: ETLP-9
    # Enable once formatting and linting checks are implemented.

    .venv/bin/python -m mypy src
    # Pending: ETLP-9
    # Enable once typing checks are implemented.

## Rules

- do not claim success if validation fails
- do not suppress failing validation output
- validation failures must be resolved before review
- preserve deterministic validation behavior
- avoid introducing flaky tests
- once a pending validation command is implemented, this file must be updated and the command becomes mandatory

## Formatting And Typing

The repository treats formatting, linting, and typing as mandatory quality gates.

## Scope Discipline

Validation fixes must remain tightly scoped to the issue being implemented.
Avoid introducing unrelated refactors during validation cleanup.

## Acceptance Criteria Audit

Before reporting task completion, agents must perform an acceptance-criteria audit.

The audit must confirm:

- every acceptance criterion is satisfied
- every “Should have” item is either implemented or explicitly reported as not done
- every “Nice to have” item is either implemented, deferred, or explicitly reported as not done
- validation gates were run according to this document
- no unrelated scope was introduced

Implementation reports must include:

- an “Acceptance Criteria Audit” section
- explicit PASS/FAIL status

Tasks with incomplete acceptance criteria must not be reported as complete.
