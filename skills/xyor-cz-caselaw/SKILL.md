---
name: xyor-cz-caselaw
description: Independent Czech case-law research and verification skill for XYOR LEX. Search leads are never verified holdings.
version: 0.1.0
jurisdiction: CZ
status: RESEARCH_ONLY
---

# XYOR LEX CZ | Case-law Skill

## Mission and boundaries
Use this skill for research into Czech judicial decisions and legal norms. It is jurisdiction-agnostic as to clients or cases, but jurisdiction-specific as to **CZ**. It operates independently of the Latvian Senāts, LV corpus, and any individual litigation project. All connectors and datasets are **untrusted retrieval aids** until checked against the competent official Czech source.

The existing `app.py` serves a Hugging Face search bridge. The existing `legislation_app.py` serves a separate unofficial historical/current GitHub legislation bridge. Neither is itself a legal authority or a holding validation engine. A runnable server, verified index, source match and legal release are separate gates.

## Authority routing
1. `Ústavní soud` (Constitutional Court): official court and its NALUS decision search.
2. `Nejvyšší správní soud` (Supreme Administrative Court): official NSS judgments/search.
3. `Nejvyšší soud` (Supreme Court): official NS decisions and the court's published collection, when relevant.
4. Lower courts: Ministry of Justice decisions portal and original decision/document where available.
5. Legislation: official Czech legal texts including e-Sbírka; determine temporal wording as of the material date. A GitHub commit date is NOT proof of legal effective date.
Official discovery directory: https://msp.gov.cz/web/msp/rozhodnuti-soudu-judikatura-
ECLI may aid identity but is not sufficient proof of provenance or the holding.

## Retrieval chain (mandatory)
`issue/norm -> candidate -> court/instance -> spisová značka/ECLI/date/type -> source URL/document -> official full text -> SHA-256 of retrieved original -> pinpoint -> exact passage -> judicial voice -> reasoning/holding -> conditions/exceptions -> operative result -> appeal/overruling status -> material-fact comparability -> jurisdiction/authority weight -> proposed use`.

Distinguish: reasoning of court; party submission; lower-court finding quoted by appellate court; dissent; court headnote (`právní věta`); procedural summary; dictum. A `právní věta` is a navigation aid until checked against the complete original.

Always record: title, exact URL/ID, court, chamber/senate if shown, date, case number, ECLI if provided, source publication/retrieval date, original document hash when obtained, exact page/paragraph or stable section anchor, source version and access limitations.

## Source and safety labels
- `CZ_SEARCH_LEAD_ONLY`: dataset hit, keyword match, `/resolve`, `/candidates`, `/provisions`, `/case`, `/evidence`, unofficial statute repository, or secondary citation.
- `CZ_IDENTITY_VERIFIED`: official original corroborates case number, court, date, document identity.
- `CZ_FULLTEXT_VERIFIED`: complete readable official original inspected; version and source recorded.
- `CZ_HOLDING_VERIFIED`: exact court reasoning with pinpoint, question, voice, posture, conditions and countercontext evaluated.
- `CZ_TEMPORAL_VERIFIED`: governing statutory wording and decision timing tested.
- `CZ_APPLICABILITY_REVIEWED`: material similarities and differences to the target dispute checked.
- `CZ_DOC_ELIGIBLE`: all applicable prior gates passed, usage/license approved, precise quotation/pinpoint preserved.
- `NOT_GIVEN`, `PARTIAL`, `DISPUTED`, `CONTRADICTORY`, `INFERRED` as appropriate. Missing official document, source ID, pinpoint or complete holding automatically demotes to `CZ_SEARCH_LEAD_ONLY` or `NOT_GIVEN`, never silently passes.

Do not automatically infer precedent strength from court level, date or similarity score. Do not make mandatory-application claims across jurisdictions. Czech law is usually comparative context for LV disputes, unless independently established otherwise.

## Technical guards
- `/health` indicates process only. `/ready` must probe actual external HF rows/search/filter for the read-only `remote` mode, and must return 503 if upstream is unavailable. HF `/is-valid` alone is insufficient. `/index/status` and configuration-specific identity checks govern the optional `memory` mode.
- Default deployment: `XYOR_INDEX_MODE=remote`, with no corpus download, no large startup index. `/search` and `/filter` are delegated to HF and are not exhaustive legal searches; `/provisions` in remote mode is only a post-filtered page of HF text search, not a provision corpus index.
- Optional memory index: require consistent ordering between parquet-index row and Hugging Face viewer row, multiple deterministic probes, identity comparison, total-row-count checks and per-row retrieval verification.
- Any shard/index/identity mismatch, missing mandatory field, upstream failure, partial Parquet conversion, corrupted original, or unverified court identity => **FAIL CLOSED**; never issue a holding or quote gate.
- Dataset revisions/fingerprints must be pinned for reproducibility before legal release. Until then: non-deterministic upstream dependency.
- Encoding heuristics detect only obvious damage. Passing them is not proof of correctness, source fidelity, official status or complete text.
- Search relevance score is navigation only.
- A runnable endpoint or CI that merely logs exceptions does **not** prove production readiness.

## Licensing and corpus coverage
The current Hugging Face card lists `justice` as CC BY 4.0 (lower courts, 2020-2026) and `czcdc` as CC BY-NC-SA 4.0 (apex courts, 1993-2023); its top-level label is CC BY-NC-SA 4.0. Validate rights for **each source and operation**, upstream attribution and dataset terms before commercial use. Public nature of a judgment does not automatically solve database/content licensing questions. Neither the datasets nor this skill certify copyright/usage rights. Supplement post-2023 apex decisions from official portals.

## Output contract (RU analytical output; CZ national documents when requested)
`FACT -> DOCUMENT -> SOURCE STATUS -> EXACT PASSAGE -> COURT HOLDING -> LIMITS/CONTEXT -> MATERIAL COMPARISON -> LEGAL WEIGHT -> CONCLUSION -> REQUIRED NEXT VERIFICATION`.

Every cited case: `court | case no. | date | ECLI (if verified) | official URL | pinpoint | exact holding | safety label`.
End with **Проверено / Нужно уточнить**.
Never assert that this skill is installed or active merely because this file exists in GitHub. Activation, runtime integration, full-corpus and live tests require independent evidence.

## Release gates
R0 code available; R1 index tested; R2 official provenance; R3 complete original and encoding; R4 holding and context; R5 temporal applicability; R6 license for the actual use; R7 end-to-end integration and regression tests. `PRODUCTION_PASS` only if each required gate has recorded evidence; otherwise `BLOCKED`.

## Future Mac corpus import (not required for this release)
When independently sorted official originals become available, ingest them as immutable files with SHA-256, exact source URL, court, date, case number, optional ECLI, OCR/encoding and import provenance. The future local provider MUST be optional and preserve the public HTTP contract. Never mark an uploaded or synced file as official until provenance is validated. No Google Drive full-corpus sync is assumed.
