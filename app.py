import asyncio
import json
import re
from typing import Any

import httpx
import polars as pl
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

DATASET = "overthelex/cz-court-decisions"
HF_BASE = "https://datasets-server.huggingface.co"
CONFIG_COLUMNS = {
    "justice": [
        "case_number", "court_name", "court_type", "decision_date", "ecli",
        "subject", "cited_provisions",
    ],
    "czcdc": [
        "case_number", "court_name", "court_type", "decision_date", "ecli", "subject",
    ],
}
TOPIC_COLUMNS = ["case_number", "court_name", "ecli", "subject"]

app = FastAPI(title="XYOR CZ Case-Law Bridge", version="0.6.3")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

INDEX: dict[str, pl.DataFrame] = {}
INDEX_STATUS: dict[str, Any] = {"state": "starting", "dataset": DATASET, "configs": {}, "error": None}


async def hf_raw(endpoint: str, params: dict[str, Any], timeout: float = 45.0):
    clean = {k: v for k, v in params.items() if v is not None}
    clean["dataset"] = DATASET
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        return await client.get(f"{HF_BASE}/{endpoint}", params=clean)


async def hf_get(endpoint: str, params: dict[str, Any], timeout: float = 45.0):
    response = await hf_raw(endpoint, params, timeout=timeout)
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail={
            "upstream_status": response.status_code,
            "upstream_url": str(response.request.url),
            "upstream_body": response.text[:2000],
        })
    return response.json()


def _build_one_config(config: str, files: list[dict[str, Any]]) -> pl.DataFrame:
    wanted = CONFIG_COLUMNS[config]
    scans: list[pl.LazyFrame] = []
    for item in sorted(files, key=lambda x: x.get("filename") or ""):
        lf = pl.scan_parquet(item["url"])
        schema = lf.collect_schema()
        cols = [c for c in wanted if c in schema]
        scans.append(lf.select(cols))
    if not scans:
        raise RuntimeError(f"no parquet files for config={config}")
    return pl.concat(scans, how="vertical_relaxed").with_row_index("row_idx").collect(engine="streaming")


def _index_summary(df: pl.DataFrame) -> dict[str, Any]:
    sizes = sorted(((name, df[name].estimated_size()) for name in df.columns), key=lambda x: x[1], reverse=True)
    return {
        "rows": df.height,
        "columns": df.columns,
        "estimated_size_bytes": df.estimated_size(),
        "largest_columns_bytes": dict(sizes[:5]),
        "first_case_number": df[0, "case_number"] if df.height and "case_number" in df.columns else None,
    }


def _tokenize_query(q: str) -> list[str]:
    raw = re.findall(r"[0-9A-Za-zÀ-ž§./-]+", q.lower())
    stop = {
        "the", "and", "for", "with", "from", "that", "this", "what", "when", "where",
        "který", "která", "které", "jako", "nebo", "pro", "při", "podle", "je", "jsou",
        "это", "как", "что", "для", "при", "или", "по", "на", "из", "с", "и", "в",
    }
    out: list[str] = []
    for tok in raw:
        if len(tok) < 2 or tok in stop:
            continue
        if tok not in out:
            out.append(tok)
    return out[:8]


def _candidate_search(df: pl.DataFrame, q: str, limit: int) -> pl.DataFrame:
    tokens = _tokenize_query(q)
    if not tokens:
        return df.head(0)
    score = pl.lit(0)
    for token in tokens:
        pattern = "(?i)" + re.escape(token)
        hit = pl.lit(False)
        for col in TOPIC_COLUMNS:
            if col in df.columns:
                hit = hit | pl.col(col).cast(pl.String, strict=False).fill_null("").str.contains(pattern)
        score = score + hit.cast(pl.Int16)
    sort_cols = ["_score"] + (["decision_date"] if "decision_date" in df.columns else [])
    return (
        df.lazy()
        .with_columns(score.alias("_score"))
        .filter(pl.col("_score") > 0)
        .sort(sort_cols, descending=[True] * len(sort_cols), nulls_last=True)
        .head(limit)
        .collect()
    )


def _provision_search(df: pl.DataFrame, q: str, limit: int) -> pl.DataFrame:
    if "cited_provisions" not in df.columns:
        return df.head(0)
    pattern = "(?i)" + re.escape(q.strip())
    mask = pl.col("cited_provisions").list.eval(pl.element().str.contains(pattern)).list.any()
    return (
        df.lazy()
        .filter(mask.fill_null(False))
        .sort("decision_date", descending=True, nulls_last=True)
        .head(limit)
        .collect()
    )


async def build_candidate_index():
    INDEX_STATUS["state"] = "building"
    print("XYOR_INDEX_START", flush=True)
    try:
        parquet = await hf_get("parquet", {}, timeout=30.0)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for item in parquet.get("parquet_files", []):
            if item.get("split") == "train" and item.get("config") in CONFIG_COLUMNS:
                grouped.setdefault(item["config"], []).append(item)

        for config in ("justice", "czcdc"):
            files = grouped.get(config, [])
            INDEX_STATUS["configs"][config] = {"state": "building", "parquet_files": len(files)}
            print("XYOR_INDEX_CONFIG_START", config, len(files), flush=True)
            df = await asyncio.to_thread(_build_one_config, config, files)
            INDEX[config] = df
            summary = _index_summary(df)

            probe_idx = 1000 if df.height > 1000 else 0
            if df.height:
                probe = await hf_get("rows", {"config": config, "split": "train", "offset": probe_idx, "length": 1}, timeout=20.0)
                rows = probe.get("rows", [])
                if rows:
                    hf_case = (rows[0].get("row") or {}).get("case_number")
                    idx_case = df[probe_idx, "case_number"]
                    summary["alignment_probe"] = {
                        "row_idx": probe_idx,
                        "hf_case_number": hf_case,
                        "index_case_number": idx_case,
                        "match": hf_case == idx_case,
                    }

            INDEX_STATUS["configs"][config] = {"state": "ready", "parquet_files": len(files), **summary}
            print("XYOR_INDEX_CONFIG_READY", config, json.dumps(INDEX_STATUS["configs"][config], ensure_ascii=False), flush=True)

        exact = INDEX["justice"].filter(pl.col("case_number") == "10 C 73/2020-127").head(1)
        print("XYOR_INDEX_EXACT_TEST", json.dumps(exact.to_dicts(), ensure_ascii=False), flush=True)

        topical = await asyncio.to_thread(_candidate_search, INDEX["justice"], "výpověď nájmu bytu", 3)
        print("XYOR_INDEX_TOPIC_TEST", json.dumps(topical.to_dicts(), ensure_ascii=False), flush=True)

        provision = await asyncio.to_thread(_provision_search, INDEX["justice"], "§ 2291", 3)
        print("XYOR_INDEX_PROVISION_TEST", json.dumps(provision.to_dicts(), ensure_ascii=False), flush=True)

        if topical.height:
            row_idx = int(topical[0, "row_idx"])
            row_payload = await hf_get("rows", {"config": "justice", "split": "train", "offset": row_idx, "length": 1}, timeout=30.0)
            row_items = row_payload.get("rows", [])
            if row_items:
                row = row_items[0].get("row") or {}
                full_text = row.get("full_text") or ""
                print("XYOR_INDEX_FULLTEXT_TEST", json.dumps({
                    "row_idx": row_idx,
                    "case_number": row.get("case_number"),
                    "court_name": row.get("court_name"),
                    "source_url": row.get("source_url"),
                    "full_text_chars": len(full_text),
                    "full_text_nonempty": bool(full_text.strip()),
                }, ensure_ascii=False), flush=True)

        INDEX_STATUS["state"] = "ready"
        INDEX_STATUS["error"] = None
        print("XYOR_INDEX_READY", flush=True)
    except Exception as exc:
        INDEX_STATUS["state"] = "error"
        INDEX_STATUS["error"] = repr(exc)
        print("XYOR_INDEX_ERROR", repr(exc), flush=True)


@app.on_event("startup")
async def startup():
    asyncio.create_task(build_candidate_index())


@app.get("/")
async def root():
    return {"service": "XYOR CZ Case-Law Bridge", "version": "0.6.3", "dataset": DATASET, "index_state": INDEX_STATUS["state"]}


@app.get("/health")
async def health():
    return {"ok": True, "service": "XYOR CZ Case-Law Bridge", "dataset": DATASET, "mode": "read-only", "version": "0.6.3", "index_state": INDEX_STATUS["state"]}


@app.get("/index/status")
async def index_status():
    return INDEX_STATUS


@app.get("/splits")
async def splits():
    return await hf_get("splits", {})


@app.get("/rows")
async def rows(config: str, split: str = "train", offset: int = Query(0, ge=0), length: int = Query(10, ge=1, le=100)):
    return await hf_get("rows", {"config": config, "split": split, "offset": offset, "length": length})


@app.get("/case")
async def case_by_row(config: str, row_idx: int = Query(..., ge=0)):
    payload = await hf_get("rows", {"config": config, "split": "train", "offset": row_idx, "length": 1})
    rows = payload.get("rows", [])
    if not rows:
        raise HTTPException(status_code=404, detail="row not found")
    return rows[0]


@app.get("/resolve")
async def resolve_case(case_number: str, config: str = "justice", limit: int = Query(10, ge=1, le=50)):
    df = INDEX.get(config)
    if df is None:
        raise HTTPException(status_code=503, detail={"index_state": INDEX_STATUS["state"], "config": config})
    result = df.filter(pl.col("case_number") == case_number).head(limit)
    return {"config": config, "case_number": case_number, "count": result.height, "candidates": result.to_dicts()}


@app.get("/candidates")
async def candidates(q: str, config: str = "justice", limit: int = Query(20, ge=1, le=50)):
    df = INDEX.get(config)
    if df is None:
        raise HTTPException(status_code=503, detail={"index_state": INDEX_STATUS["state"], "config": config})
    result = await asyncio.to_thread(_candidate_search, df, q, limit)
    return {"config": config, "query": q, "tokens": _tokenize_query(q), "count": result.height, "candidates": result.to_dicts()}


@app.get("/provisions")
async def provisions(q: str, limit: int = Query(20, ge=1, le=50)):
    df = INDEX.get("justice")
    if df is None:
        raise HTTPException(status_code=503, detail={"index_state": INDEX_STATUS["state"], "config": "justice"})
    result = await asyncio.to_thread(_provision_search, df, q, limit)
    return {"config": "justice", "query": q, "count": result.height, "candidates": result.to_dicts()}
