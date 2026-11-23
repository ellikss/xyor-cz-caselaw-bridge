# XYOR CZ Case-Law Bridge

Research-only, read-only HTTP bridge for the Czech judicial decision dataset `overthelex/cz-court-decisions`. **Not an official court source and not a verified holding engine.**

## Real case-law routes (app.py v0.8.1)
- `GET /health`: process health, not index readiness. `legal_release=BLOCKED`.
- `GET /index/status`: status of the Parquet candidate index, with source-alignment probes.
- `GET /splits`, `GET /rows`: raw HF Dataset Viewer retrieval.
- `GET /resolve?config=justice&case_number=...`: exact case number candidate lookup.
- `GET /candidates?config=justice&q=...`: topic/keyphrase candidates.
- `GET /provisions?q=...`: cited provision candidates (`justice` only).
- `GET /case?config=justice&row_idx=...`: viewer row for a dataset position.
- `GET /quality?config=czcdc&row_idx=...`: encoding heuristic.
- `GET /evidence?config=justice&row_idx=...&needle=...`: context windows, *not* verified quotations.

Previously documented `/search` and `/filter` endpoints are **not implemented**.

## Separate legislation bridge
`legislation_app.py` contains a separate FastAPI app for the public `legalize-dev/legalize-cz` repo with current and historical Git snapshots, but **is not started by the current Render service**. Git revisions are not official effective-date evidence.

## Run
```bash
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```
Optional legislation app (separate process):
```bash
uvicorn legislation_app:app --host 127.0.0.1 --port 8001
```

Render currently starts only `app:app`; `autoDeploy: false`. A successful CI probe does not establish that Render is serving the latest revision.

## Legal and integrity gates
- Parquet candidate indices are exposed only after multiple deterministic Hugging Face row identity probes pass for each configuration.
- Any partial index build or alignment mismatch clears all indices and prevents indexed search (`503`).
- `/case` and `/evidence` are still retrieval endpoints, not legal-source verification.
- Passing a basic encoding heuristic does not establish that text is complete, official or attributable to the identified court.
- Apex court coverage after 2023 and actual licensing for commercial uses require separate verification.
- **No automatic citation or legal release** until official full text, identity, holding, temporal application, license and regression tests are confirmed.

Read `skills/xyor-cz-caselaw/SKILL.md` and `docs/CZ_CONTOUR_ARCHITECTURE.md` for the independent XYOR LEX CZ methodology and release checklist.

## Security
Public cross-origin GET endpoints, no auth. Do not expose private XYOR sources through this service. No local Czech source corpus is included. Treat upstream dataset files as untrusted.
