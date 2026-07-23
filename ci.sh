#!/bin/bash
# Run locally everything CI runs, so a push won't go red. Mirrors
# .github/workflows/tests.yml (pre-commit + tests). Usage: ./ci.sh
#
# Gotcha this script exists to close: `pre-commit run --all-files` only
# sees git-TRACKED files — a brand-new untracked file sails through it and
# then fails in CI. `ruff check .` on the directory sees everything.
set -euo pipefail

step() { printf '\n=== %s ===\n' "$1"; shift; "$@"; }

step "uv sync (locked)" uv sync --locked

# Directory-form lint first: catches untracked files pre-commit misses.
step "ruff check" uv run ruff check --no-fix .
step "ruff format --check" uv run ruff format --check .

# The actual CI lint job.
step "pre-commit" uv run pre-commit run --all-files

# The tests job. The liveness test needs the gwbase dev broker
# (gw-dev-rabbit, gridworks-base ./arm.sh or ./x86.sh); without one, run
# the suite in its CI self-skip mode rather than failing on liveness.
if nc -z localhost 5672 2>/dev/null; then
  step "tests (with broker)" uv run pytest
else
  echo "NOTE: no broker on localhost:5672 — liveness test will self-skip."
  step "tests (no broker: CI self-skip mode)" env GITHUB_ACTIONS=true uv run pytest
fi

printf '\nAll CI checks passed.\n'
