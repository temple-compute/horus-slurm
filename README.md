# horus-slurm

[![Python 3.13+](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Overview

Slurm target for the Horus Runtime.

WIP

`SlurmTarget.inner` is the target that reaches the login node: `LocalTarget()` by
default (the orchestrator runs on the login node), or an `SSHTarget`. On a host that
runs workflows written by others, set `HORUS_SLURM_ALLOW_LOCAL_INNER=false`: a local
`inner`, set or defaulted, is then refused.

---

## Development

### Requirements

- Python ≥ 3.13
- `horus-runtime` ≥ 0.1.1 (install from source or PyPI)
- [uv](https://docs.astral.sh/uv/) for managing the virtual environment and dependencies

### Setup

```bash
# Install dependencies (creates .venv automatically)
uv sync

# Install pre-commit hooks
uv run pre-commit install
```

### Common commands

| Command | Description |
|---|---|
| `make test` | Run the full test suite with coverage |
| `make lint` | ruff + mypy |
| `make format` | Auto-fix with ruff |
| `make type-check` | mypy only |
| `make babel-extract` | Update `messages.pot` |
| `make babel-add LANG=es` | Add a new language |
| `make babel-check` | Verify all strings are translated |
| `make clean` | Remove build artefacts and caches |

---

## License

MIT — see [LICENSE](LICENSE).
