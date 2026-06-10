# horus-slurm

[![Python 3.13+](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Overview

Slurm target for the Horus Runtime.

WIP

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
