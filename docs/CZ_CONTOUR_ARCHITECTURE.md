# XYOR LEX CZ | Independent contour and acceptance criteria

Status: design baseline / NOT production-ready. Branch work cannot imply deployment.

## Independent lines
- `CZ-CASELAW` = `app.py`, default remote HF Dataset Viewer API (no local corpus), optional `memory` Parquet index, official source verification layer (to build).
- `CZ-LEGISLATION` = `legislation_app.py`, unofficial current/historical snapshots; official version/effective-date gate (to build).
- `CZ-SKILL` = `skills/xyor-cz-caselaw/SKILL.md`, legal-source and holding method, not a deployed plugin.
- `CZ-LEGAL-GATE` = licensing, temporal checks, citation validation, output blocking.
- `CROSS-JURISDICTION` = optional comparison only, without changing the LV Senāts pipeline.

## Trust boundaries
HF Dataset Viewer/Parquet -> CANDIDATE only.
Index verified against HF rows -> internally consistent candidate, NOT official source.
Official court decision verified -> source/identity gates.
Court reasoning isolated and contextualised -> holding gate.
Norm, time, applicability, license verified -> legal document eligibility.

## Critical known defects in main (2026-11-23)
- README documents `/search` and `/filter`, routes absent from `app.py`.
- Alignment probe in `app.py` reports a boolean but does not reject mismatch.
- Index is published before validation.
- `/case` requests by row without guarding against parquet vs HF row misalignment.
- CI quality probe catches connection errors and can end with success despite no live service.
- `legislation_app.py` exists but Render start command launches only `app:app`.
- Official CZ originals/holdings, service health and licensing are not certified.

## Acceptance tests
1. Correct index ordering at first, middle, last and at least one other deterministic position, with stable record IDs, full row count and observed upstream row index. Mismatch => no ready index.
2. Any HF row fetched by index checks configuration, bounds, row index, case number, ID and available ECLI against verified index. Mismatch => HTTP 503, not silent wrong decision.
3. `/resolve` or `/candidates` before verified index readiness => 503; never return partially checked records.
4. Partial/pending/failed Parquet conversions => no index.
5. `/evidence` corrupt text => zero quote windows, explicit block. Clean text remains `CZ_SEARCH_LEAD_ONLY` until official check.
6. Empty text, missing court/case metadata, HTML error pages, cross-court identity conflict => fail relevant legal gates.
7. Source and license gates per config, not a single broad commercial permission.
8. Regression tests run in CI; network checks must fail on errors and cannot count `except: print` as success.
9. Official CZ court datasets/portals queried for at least 3 known fixtures per court family with citations and hash/pinpoint log.
10. Run verification against live deployed revision after PR merge, if approved; main branch remains untouched during review.

## Non-goals
No hardcoded Ellikss litigation examples in corpus or tests. No assumption that Czech judgments bind a Latvian court. No legal release merely because an API returns HTTP 200.

## 2026-11-23 remote-first MVP
- Default `XYOR_INDEX_MODE=remote` and Render environment uses it explicitly.
- No Mac sorting, Google Drive sync, corpus download, or in-memory index is required to start the API.
- Expose `/search`, bounded `/filter`, `/resolve`, `/candidates`, `/provisions` (partial), `/case`, `/quality`, `/evidence`.
- If HF search or rows fail, API must return a clear 502/503, never a fabricated empty search result.
- A later verified local-provider adapter may be added behind the same API, independently tested, without rewriting the CZ skill.
- Online smoke CI must pass against real provider. CI passing for offline tests does not override online smoke failures.
