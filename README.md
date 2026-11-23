# XYOR CZ Case-Law Bridge

**Research-only Czech legal retrieval shell. No local corpus required.**
This service retrieves candidates from Hugging Face `overthelex/cz-court-decisions`; it is not an authoritative judicial-source or verified-holding service.

## Recommended deployment: remote first

```bash
pip install -r requirements.txt
XYOR_INDEX_MODE=remote uvicorn app:app --host 0.0.0.0 --port 8000
```

`remote` is the **default**, designed for small/cloud instances. It does **not** download the 871K court decisions or construct a large in-memory index. HF Dataset Viewer search and exact filter power the lookup endpoints. This changes search semantics relative to the legacy Polars metadata-only index: HF searches string columns including decision text; results are **unverified candidates** only.

`XYOR_INDEX_MODE=memory` enables the older optional Parquet metadata index, but **is not production-validated**. Any alignment mismatch fails closed, and the index is not published before validation. Do not enable in a low-memory Render instance without sizing and full checks.

## API v0.9.0

| HTTP route | Purpose |
| --- | --- |
| `GET /health` | Process health; does not prove search or legal validation |
| `GET /ready` | Probe upstream HF viewer/search/filter availability; returns 503 if a required feature is unavailable |
| `GET /capabilities` | Exact API contract, source gate and operating mode |
| `GET /splits` | HF source subsets |
| `GET /rows?config=justice&offset=0&length=5` | Slim metadata; not full texts |
| `GET /search?q=nájem&config=justice&limit=10` | HF text search |
| `GET /filter?config=justice&case_number=10%20C%2073%2F2020-127` | Safe exact filter (or `court_name`) |
| `GET /resolve?case_number=...` | Resolve by case number using HF filter in remote mode |
| `GET /candidates?q=...` | HF text candidates in remote mode |
| `GET /provisions?q=§%202291` | *Non-exhaustive* provision leads only; remote mode post-filters the current HF text-search page |
| `GET /case?config=justice&row_idx=0` | HF viewer source row, including text if served by upstream; **not official original** |
| `GET /quality?config=justice&row_idx=0` | Encoding heuristic and text-truncation marker |
| `GET /evidence?config=justice&row_idx=0&needle=nájem` | Context windows blocked when text is empty/truncated or encoding damaged |
| `GET /index/status` | Index mode and, if memory mode, progress and integrity status |

All candidate endpoints strip full text from search results, return `CZ_SEARCH_LEAD_ONLY`, and set `exhaustive_legal_corpus_search=false`. The HF viewer may report `partial=true`; **no result is not proof that a decision does not exist**. Never turn a context window into a court quotation without official-source verification. HF full-text truncation detection is a separate retrieval gate.

Example:

```bash
curl 'http://localhost:8000/capabilities'
curl 'http://localhost:8000/ready'
curl --get 'http://localhost:8000/search' --data-urlencode 'config=justice' --data-urlencode 'q=nájem' --data-urlencode 'limit=3'
curl --get 'http://localhost:8000/resolve' --data-urlencode 'case_number=10 C 73/2020-127'
```

## Tests

```bash
python -m unittest discover -s tests -v          # offline regression tests
XYOR_INDEX_MODE=remote python scripts/live_smoke.py # strict live HF integration check
```

GitHub Actions workflows: `cz-offline-tests.yml` and `cz-live-smoke.yml`. Only passing tests from the same revision count as a technical check. The **deployed Render revision** must be checked separately; Render `autoDeploy` is disabled. A GitHub PR cannot automatically deploy this service.

## Legislation (separate process)
`legislation_app.py` provides a separate **unofficial** legislation bridge to GitHub `legalize-dev/legalize-cz`, including Git snapshots. It is not launched by the current Render caselaw service. Git commit chronology is *not* legal effective-date verification. This legislation line requires its own deployment and temporal checks.

## Release controls
- Source: Hugging Face, **not** the official court record.
- Court: NS, NSS, ÚS and lower court identity must be verified from official original.
- Encoding/complete text, judicial voice and precise holding must pass before legal citation.
- `czcdc` and other subsets may have different redistribution and commercial-use license constraints. No commercial license is presumed.
- No local Czech corpus exists in this repo; future independent local storage/import may be added without changing the HTTP contract.
- The read-only service is public and has no auth; do not connect private sources without explicit security work.
- `PRODUCTION_PASS` requires recorded deployment, upstream functionality, source, license and end-to-end checks. **Current legal release: BLOCKED**.

See `skills/xyor-cz-caselaw/SKILL.md` and `docs/CZ_CONTOUR_ARCHITECTURE.md`.
