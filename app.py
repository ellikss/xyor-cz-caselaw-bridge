import asyncio
import json
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

DATASET = "overthelex/cz-court-decisions"
HF_BASE = "https://datasets-server.huggingface.co"

app = FastAPI(title="XYOR CZ Case-Law Bridge", version="0.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


async def hf_raw(endpoint: str, params: dict[str, Any], timeout: float = 45.0):
    clean = {k: v for k, v in params.items() if v is not None}
    clean["dataset"] = DATASET
    url = f"{HF_BASE}/{endpoint}"
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        response = await client.get(url, params=clean)
    return response


async def hf_get(endpoint: str, params: dict[str, Any]):
    response = await hf_raw(endpoint, params)
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


def compact_row(row_obj: dict[str, Any]) -> dict[str, Any]:
    row = row_obj.get("row") or {}
    text = row.get("full_text")
    if isinstance(text, str):
        text = text[:1500]
    return {
        "row_idx": row_obj.get("row_idx"),
        "case_number": row.get("case_number"),
        "court_name": row.get("court_name"),
        "court_type": row.get("court_type"),
        "decision_date": row.get("decision_date"),
        "ecli": row.get("ecli"),
        "source_url": row.get("source_url"),
        "cited_provisions": row.get("cited_provisions"),
        "full_text_prefix": text,
        "keys": sorted(row.keys()),
    }


async def background_probe():
    print("XYOR_PROBE_START", flush=True)
    try:
        split_response = await hf_raw("splits", {}, timeout=20.0)
        print("XYOR_PROBE_SPLITS_STATUS", split_response.status_code, flush=True)
        if split_response.status_code >= 400:
            print("XYOR_PROBE_SPLITS_ERROR", split_response.text[:2000], flush=True)
            return

        split_data = split_response.json()
        splits = split_data.get("splits", [])
        print("XYOR_PROBE_SPLITS", json.dumps(splits, ensure_ascii=False), flush=True)

        for item in splits:
            config = item.get("config")
            split = item.get("split")
            if not config or not split:
                continue
            rows_response = await hf_raw(
                "rows",
                {"config": config, "split": split, "offset": 0, "length": 1},
                timeout=20.0,
            )
            print("XYOR_PROBE_ROWS_STATUS", config, split, rows_response.status_code, flush=True)
            if rows_response.status_code < 400:
                rows = rows_response.json().get("rows", [])
                if rows:
                    print(
                        "XYOR_PROBE_ROW",
                        config,
                        split,
                        json.dumps(compact_row(rows[0]), ensure_ascii=False),
                        flush=True,
                    )

        search_response = await hf_raw(
            "search",
            {
                "config": "justice",
                "split": "train",
                "query": "10 C 73/2020-127",
                "offset": 0,
                "length": 5,
            },
            timeout=20.0,
        )
        print("XYOR_PROBE_SEARCH_STATUS", search_response.status_code, flush=True)
        if search_response.status_code < 400:
            rows = search_response.json().get("rows", [])
            print("XYOR_PROBE_SEARCH_COUNT", len(rows), flush=True)
            for row in rows[:3]:
                print("XYOR_PROBE_SEARCH_ROW", json.dumps(compact_row(row), ensure_ascii=False), flush=True)
        else:
            print("XYOR_PROBE_SEARCH_ERROR", search_response.text[:2000], flush=True)
    except Exception as exc:
        print("XYOR_PROBE_EXCEPTION", repr(exc), flush=True)
    finally:
        print("XYOR_PROBE_END", flush=True)


@app.on_event("startup")
async def startup_probe():
    asyncio.create_task(background_probe())


@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": "XYOR CZ Case-Law Bridge",
        "dataset": DATASET,
        "mode": "read-only",
        "version": "0.3.0",
    }


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


@app.get("/search")
async def search(
    config: str,
    q: str,
    split: str = "train",
    offset: int = Query(0, ge=0),
    length: int = Query(20, ge=1, le=100),
):
    return await hf_get(
        "search",
        {"config": config, "split": split, "query": q, "offset": offset, "length": length},
    )


@app.get("/filter")
async def filter_rows(
    config: str,
    where: str,
    split: str = "train",
    offset: int = Query(0, ge=0),
    length: int = Query(20, ge=1, le=100),
):
    return await hf_get(
        "filter",
        {"config": config, "split": split, "where": where, "offset": offset, "length": length},
    )
