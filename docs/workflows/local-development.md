# Local Development

This workflow covers fresh-clone setup and local validation before implementation
work starts. The mandatory validation source of truth is
`docs/workflows/validation-gates.md`.

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
python -m ruff check .
python -m black --check .
python -m mypy src
git diff --check
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

Run the required validation gates before reporting implementation completion:

```sh
python -m ruff check .
python -m black --check .
python -m mypy src
git diff --check
```

If the virtual environment is not activated, run the Python tools through the
environment interpreter instead:

```sh
.venv/bin/python -m ruff check .
.venv/bin/python -m black --check .
.venv/bin/python -m mypy src
git diff --check
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

## Common Pitfalls

- Forgetting to activate `.venv` and accidentally using global tools.
- Using a Python version older than the `>=3.8` project requirement.
- Installing only runtime dependencies instead of `python -m pip install -e ".[dev]"`.
- Treating `pytest` as mandatory before project test tooling is configured.
- Assuming every Cloud Function dependency is a root package dependency; some
  function dependencies remain per-function.
