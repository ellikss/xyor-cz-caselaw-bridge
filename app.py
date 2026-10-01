import os
import json
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

DATASET = "overthelex/cz-court-decisions"
HF_BASE = "https://datasets-server.huggingface.co"

app = FastAPI(title="XYOR CZ Case-Law Bridge", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

async def hf_get(endpoint: str, params: dict):
    params = {k: v for k, v in params.items() if v is not None}
    params["dataset"] = DATASET
    url = f"{HF_BASE}/{endpoint}"
    async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:
        r = await client.get(url, params=params)
    if r.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "upstream_status": r.status_code,
                "upstream_url": str(r.request.url),
                "upstream_body": r.text[:2000],
            },
        )
    return r.json()

@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": "XYOR CZ Case-Law Bridge",
        "dataset": DATASET,
        "mode": "read-only",
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
    return await hf_get(
        "rows",
        {"config": config, "split": split, "offset": offset, "length": length},
    )

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
        {
            "config": config,
            "split": split,
            "q": q,
            "offset": offset,
            "length": length,
        },
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
        {
            "config": config,
            "split": split,
            "where": where,
            "offset": offset,
            "length": length,
        },
    )
