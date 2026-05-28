#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

if [[ -n "${VIRTUAL_ENV:-}" && -x "${VIRTUAL_ENV}/bin/python" ]]; then
  PYTHON="${VIRTUAL_ENV}/bin/python"
elif [[ -x ".venv/bin/python" ]]; then
  PYTHON=".venv/bin/python"
else
  PYTHON="python3"
fi

section() {
  printf '\n==> %s\n' "$1"
}

section "Ruff lint"
"$PYTHON" -m ruff check .

section "Black format check"
"$PYTHON" -m black --check .

section "MyPy type check"
"$PYTHON" -m mypy src

section "Git whitespace check"
git diff --check

section "Validation complete"
