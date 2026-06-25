# Local Development

## Status

Accepted

## Lifecycle

Active

This workflow covers fresh-clone setup and local validation before implementation
work starts. The mandatory validation source of truth is
[Validation Gates](validation-gates.md).

## Python Version

`pyproject.toml` declares support for Python `>=3.8`. CI currently runs the
quality gates with Python `3.13`, so contributors should prefer a modern
supported Python version that matches CI where possible.

## Quick Start

From a fresh clone on macOS or Linux:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
pre-commit install
./scripts/validate.sh
```

## Setup

Create and activate a virtual environment:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

Install the project with development tooling:

```sh
python -m pip install -e ".[dev]"
```

The `dev` extra is defined in `pyproject.toml` and installs the configured
local quality tools, including `pre-commit`. Do not invent additional
dependency groups.

## Pre-Commit Hooks

Install the local Git hook after installing the development tooling:

```sh
pre-commit install
```

The hook configuration lives in `.pre-commit-config.yaml`. Ruff, Black, and
MyPy are invoked through the same project virtual environment commands used by
the required validation gates, so their configuration remains centralized in
`pyproject.toml`.

Run the hooks manually against the files selected by pre-commit:

```sh
pre-commit run
```

Run the hooks across the full repository:

```sh
pre-commit run --all-files
```

The Black hook runs in check mode and does not rewrite files. Hygiene hooks may
update whitespace or final newlines; if they do, review the resulting diff and
rerun the hooks before committing.

## Local Validation

Run the canonical local validation runner before reporting implementation
completion:

```sh
./scripts/validate.sh
```

The runner executes the mandatory local gates listed in
`validation-gates.md`. It resolves the repository root before running, uses the
active virtual environment when one is available, falls back to `.venv`, and
then falls back to `python3`.

Pre-commit remains useful before individual commits:

```sh
pre-commit run
```

`pytest` is pending repository test-tooling promotion and is not mandatory yet.
Do not report it as a required local gate until `validation-gates.md` promotes
it from pending to required.

An optional import/bytecode sanity check may be useful after broad packaging or
module-layout changes:

```sh
python -m compileall src
```

This compile check is optional and does not replace the required validation
gates.

## Version Management

The canonical project version is `project.version` in `pyproject.toml`.
Version updates are managed manually with Bump My Version through the
configuration in `pyproject.toml`.

After installing the development tooling, bump the version with one of:

```sh
.venv/bin/bump-my-version bump patch
.venv/bin/bump-my-version bump minor
.venv/bin/bump-my-version bump major
```

Review the resulting diff before committing, then run:

```sh
./scripts/validate.sh
```

To inspect the configured bump without applying it, run:

```sh
.venv/bin/bump-my-version bump --dry-run --allow-dirty -vv patch
```

The verbose dry run prints the planned version change and file update while
leaving the working tree unchanged. Version consistency should be checked by
reviewing the `pyproject.toml` diff and by running the canonical validation
runner.

This setup is local/manual version management only. Future release automation
may reuse the same configuration, but no release workflow, automatic tagging,
publishing, or push behavior is configured.

## Common Pitfalls

- Forgetting to activate `.venv` and accidentally using global tools.
- Using a Python version older than the `>=3.8` project requirement.
- Installing only runtime dependencies instead of `python -m pip install -e ".[dev]"`.
- Treating `pytest` as mandatory before project test tooling is configured.
- Assuming every Cloud Function dependency is a root package dependency; some
  function dependencies remain per-function.
- Running a version bump without reviewing the diff before commit creation.
