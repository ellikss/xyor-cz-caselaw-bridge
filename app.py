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
INDEX_COLUMNS = [
    "id",
    "case_number",
    "court_name",
    "court_type",
    "decision_date",
    "decision_type",
    "ecli",
    "keywords",
    "cited_provisions",
    "subject",
    "source_url",
]
SEARCH_COLUMNS = ["case_number", "court_name", "ecli", "subject", "keywords", "cited_provisions"]

app = FastAPI(title="XYOR CZ Case-Law Bridge", version="0.5.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

INDEX: dict[str, pl.DataFrame] = {}
INDEX_STATUS: dict[str, Any] = {
    "state": "starting",
    "dataset": DATASET,
    "configs": {},
    "error": None,
}


async def hf_raw(endpoint: str, params: dict[str, Any], timeout: float = 45.0):
    clean = {k: v for k, v in params.items() if v is not None}
    clean["dataset"] = DATASET
    url = f"{HF_BASE}/{endpoint}"
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        return await client.get(url, params=clean)


async def hf_get(endpoint: str, params: dict[str, Any], timeout: float = 45.0):
    response = await hf_raw(endpoint, params, timeout=timeout)
    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "upstream_status": response.status_code,
                "upstream_url": str(response.request.url),
                "upstream_body": response.text[:2000],
            },
        )
    return response.json()


def _safe_select(lf: pl.LazyFrame) -> pl.LazyFrame:
    schema = lf.collect_schema()
    cols = [c for c in INDEX_COLUMNS if c in schema]
    return lf.select(cols)


def _build_one_config(config: str, files: list[dict[str, Any]]) -> pl.DataFrame:
    scans: list[pl.LazyFrame] = []
    for item in sorted(files, key=lambda x: x.get("filename") or ""):
        url = item["url"]
        filename = item.get("filename") or url.rsplit("/", 1)[-1]
        lf = pl.scan_parquet(url)
        lf = _safe_select(lf).with_columns(
            pl.lit(filename).alias("_source_shard"),
            pl.lit(url).alias("_source_parquet_url"),
        )
        scans.append(lf)

    if not scans:
        raise RuntimeError(f"no parquet files for config={config}")

    combined = pl.concat(scans, how="vertical_relaxed").with_row_index("row_idx")
    return combined.collect(engine="streaming")


def _index_summary(df: pl.DataFrame) -> dict[str, Any]:
    return {
        "rows": df.height,
        "columns": df.columns,
        "estimated_size_bytes": df.estimated_size(),
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
        token_hit = pl.lit(False)
        for col in SEARCH_COLUMNS:
            if col in df.columns:
                token_hit = token_hit | pl.col(col).cast(pl.String, strict=False).fill_null("").str.contains(pattern)
        score = score + token_hit.cast(pl.Int16)

    result = (
        df.lazy()
        .with_columns(score.alias("_score"))
        .filter(pl.col("_score") > 0)
        .sort(["_score", "decision_date"], descending=[True, True], nulls_last=True)
        .head(limit)
        .collect()
    )
    return result


async def build_candidate_index():
    INDEX_STATUS["state"] = "building"
    print("XYOR_INDEX_START", flush=True)
    try:
        parquet = await hf_get("parquet", {}, timeout=30.0)
        files = parquet.get("parquet_files", [])
        grouped: dict[str, list[dict[str, Any]]] = {}
        for item in files:
            if item.get("split") != "train":
                continue
            grouped.setdefault(item.get("config"), []).append(item)

        for config in ("justice", "czcdc"):
            cfg_files = grouped.get(config, [])
            INDEX_STATUS["configs"][config] = {
                "state": "building",
                "parquet_files": len(cfg_files),
            }
            print("XYOR_INDEX_CONFIG_START", config, len(cfg_files), flush=True)
            df = await asyncio.to_thread(_build_one_config, config, cfg_files)
            INDEX[config] = df
            summary = _index_summary(df)

            probe_idx = 1000 if df.height > 1000 else 0
            alignment_ok = None
            if df.height:
                hf_probe = await hf_get(
                    "rows",
                    {"config": config, "split": "train", "offset": probe_idx, "length": 1},
                    timeout=20.0,
                )
                hf_rows = hf_probe.get("rows", [])
                if hf_rows:
                    hf_case = (hf_rows[0].get("row") or {}).get("case_number")
                    idx_case = df[probe_idx, "case_number"] if "case_number" in df.columns else None
                    alignment_ok = hf_case == idx_case
                    summary["alignment_probe"] = {
                        "row_idx": probe_idx,
                        "hf_case_number": hf_case,
                        "index_case_number": idx_case,
                        "match": alignment_ok,
                    }

            INDEX_STATUS["configs"][config] = {
                "state": "ready",
                "parquet_files": len(cfg_files),
                **summary,
            }
            print("XYOR_INDEX_CONFIG_READY", config, json.dumps(INDEX_STATUS["configs"][config], ensure_ascii=False), flush=True)

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
    return {
        "service": "XYOR CZ Case-Law Bridge",
        "version": "0.5.0",
        "dataset": DATASET,
        "index_state": INDEX_STATUS["state"],
    }


@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": "XYOR CZ Case-Law Bridge",
        "dataset": DATASET,
        "mode": "read-only",
        "version": "0.5.0",
        "index_state": INDEX_STATUS["state"],
    }


@app.get("/index/status")
async def index_status():
    return INDEX_STATUS


@app.get("/splits")
async def splits():
    return await hf_get("splits", {})


@app.get("/rows")
async def rows(
    config: str,
    split: str = "train",
    offset: int = Query(0, ge=0),
    length: int = Query(10, ge=1, le=100),
):
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
    if "case_number" not in df.columns:
        raise HTTPException(status_code=500, detail="case_number not indexed")
    result = df.filter(pl.col("case_number") == case_number).head(limit)
    return {"config": config, "case_number": case_number, "count": result.height, "candidates": result.to_dicts()}


@app.get("/candidates")
async def candidates(
    q: str,
    config: str = "justice",
    limit: int = Query(20, ge=1, le=50),
):
    df = INDEX.get(config)
    if df is None:
        raise HTTPException(status_code=503, detail={"index_state": INDEX_STATUS["state"], "config": config})
    result = await asyncio.to_thread(_candidate_search, df, q, limit)
    return {
        "config": config,
        "query": q,
        "tokens": _tokenize_query(q),
        "count": result.height,
        "candidates": result.to_dicts(),
    }


@app.get("/search")
async def search(
    config: str,
    q: str,
    split: str = "train",
    offset: int = Query(0, ge=0),
    length: int = Query(20, ge=1, le=100),
):
    return await hf_get("search", {"config": config, "split": split, "query": q, "offset": offset, "length": length}, timeout=90.0)


@app.get("/filter")
async def filter_rows(
    config: str,
    where: str,
    split: str = "train",
    offset: int = Query(0, ge=0),
    length: int = Query(20, ge=1, le=100),
):
    return await hf_get("filter", {"config": config, "split": split, "where": where, "offset": offset, "length": length}, timeout=90.0)
