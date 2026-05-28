# Validation Gates

All required validation commands must pass before implementation completion,
commit creation, or pull request review.

## Required Commands

Always run:

    .venv/bin/python -m ruff check .

    .venv/bin/python -m black --check .

    .venv/bin/python -m mypy src

    git diff --check

Pending validation commands:

    ./scripts/validate.sh
    # Pending: dedicated validation runner
    # Enable once the repository-level validation runner is implemented.

    .venv/bin/python -m pytest -q
    # Pending: ETLP-7
    # Enable once the repository test structure is promoted beyond scaffolding.

## Rules

- do not claim success if validation fails
- do not suppress failing validation output
- validation failures must be resolved before review
- preserve deterministic validation behavior
- avoid introducing flaky tests
- once a pending validation command is implemented, this file must be updated and the command becomes mandatory

## Formatting And Typing

The repository treats formatting, linting, and typing as mandatory quality gates.

## Pre-Commit Enforcement

Pre-commit hooks are recommended for local enforcement before commit creation.
They mirror the required Ruff, Black, and MyPy commands while keeping the
required validation commands in this document as the source of truth.

Install and run the hooks from the project development environment:

    pre-commit install

    pre-commit run --all-files

## Continuous Integration

The repository CI workflow is `.github/workflows/ci.yml`.
CI must run on push and pull request events and enforce the mandatory lint,
format, and type gates listed above.

Test execution remains pending until the pytest command is promoted from the
pending validation commands.

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
