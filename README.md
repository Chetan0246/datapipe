# datapipe

[![CI](https://img.shields.io/badge/CI-GitHub_Actions-blue)](.github/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.12%2B-3776AB)](pyproject.toml)

Concurrent async data-processing pipeline with a bounded worker pool, jittered
exponential-backoff retries, JSONL sink + error dead-letter file, and a
**Typer + Rich** CLI with live progress.

## Features

- Bounded `asyncio.Queue`-based worker pool (`WorkerPool`)
- Retry policy with jittered exponential backoff (pluggable)
- Sources: CSV / JSON / JSONL → Sink: JSONL + `.errors.jsonl` dead-letter
- Live Rich progress bar and final report table
- Full type annotations, pytest-asyncio coverage

## Usage

```bash
cd datapipe
python -m venv .venv && source .venv/Scripts/activate
pip install -e ".[dev]"

# generate a sample input
python -c "import json;print(json.dumps([{'id':i,'name':f'row-{i}'} for i in range(100)]))" > in.json

datapipe run in.json out.jsonl --workers 16 --retries 3
# or: python -m datapipe.cli run in.json out.jsonl
```

Exit code is `1` if any item failed (after retries); failures land in `out.errors.jsonl`.

## Tests

```bash
pytest
```
