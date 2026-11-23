"""Strict live smoke test: checks the running FastAPI app against Hugging Face.
No corpus download. Failure is a failed test, never a hidden warning.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient

from app import app


def require(response, name):
    print("PROBE", name, "HTTP", response.status_code, flush=True)
    if response.status_code != 200:
        if name in ("/search", "/filter", "/resolve", "/candidates", "/provisions", "ready"):
            upstream_diagnostics()
        raise RuntimeError(f"{name}: HTTP {response.status_code}: {response.text[:500]}")
    data = response.json()
    if not isinstance(data, dict):
        raise AssertionError(f"{name}: result is not an object")
    return data


def upstream_diagnostics():
    """Print bounded external-provider errors; never log decision full texts."""
    import httpx
    base = "https://datasets-server.huggingface.co"
    common = {"dataset": "overthelex/cz-court-decisions", "config": "justice", "split": "train"}
    probes = [
        ("rows", {**common, "offset": 0, "length": 1}),
        ("first-rows", common),
        ("search", {**common, "query": "nájem", "offset": 0, "length": 1}),
        ("filter", {**common, "where": "\"case_number\"='10 C 73/2020-127'", "offset": 0, "length": 1}),
        ("parquet", {"dataset": common["dataset"]}),
    ]
    for endpoint, params in probes:
        try:
            response = httpx.get(base + "/" + endpoint, params=params, timeout=45.0)
            detail = ""
            if response.status_code != 200:
                detail = response.text[:350].replace("\\n", " ")
            else:
                data = response.json()
                detail = str({"keys": list(data.keys())[:8],
                              "rows_count": len(data.get("rows", [])),
                              "parquet_files": len(data.get("parquet_files", []))})
            print("HF_DIAG", endpoint, response.status_code, detail, flush=True)
        except Exception as exc:
            print("HF_DIAG", endpoint, type(exc).__name__, str(exc)[:180], flush=True)


with TestClient(app) as client:
    import time
    capabilities = require(client.get("/capabilities"), "capabilities")
    assert capabilities["source_gate"] == "CZ_SEARCH_LEAD_ONLY"
    assert capabilities["index_mode"] == "remote", "live smoke expects no in-memory Parquet index"
    ready_response = None
    for attempt in range(1, 6):
        ready_response = client.get("/ready")
        print("READY_ATTEMPT", attempt, ready_response.status_code, flush=True)
        if ready_response.status_code == 200:
            break
        if ready_response.status_code != 503 or attempt == 5:
            break
        detail = ready_response.json().get("detail") or {}
        upstream = detail.get("upstream") if isinstance(detail, dict) else {}
        upstream = upstream if isinstance(upstream, dict) else {}
        if upstream.get("reason") not in ("HF_INDEX_LOADING", "UPSTREAM_UNAVAILABLE", "UPSTREAM_HTTP_ERROR"):
            break
        print("HF_INDEX_WARMING_OR_UNAVAILABLE; RETRYING", flush=True)
        time.sleep(20)
    require(ready_response, "ready")

    splits = require(client.get("/splits"), "splits")
    available = {(item.get("config"), item.get("split")) for item in splits.get("splits", [])}
    assert ("justice", "train") in available, f"justice/train absent: {available}"
    assert ("czcdc", "train") in available, f"czcdc/train absent: {available}"

    for cfg in ("justice", "czcdc"):
        try:
            rows = require(client.get("/rows", params={"config": cfg, "offset": 0, "length": 1}), f"rows:{cfg}")
        except RuntimeError:
            upstream_diagnostics()
            raise
        assert rows["count"] == 1
        assert rows["candidates"][0]["row_idx"] == 0
        assert rows["source_gate"] == "CZ_SEARCH_LEAD_ONLY"
        print("ROW", cfg, json.dumps({
            "case_number": rows["candidates"][0].get("case_number"),
            "court_name": rows["candidates"][0].get("court_name"),
            "truncated_cells": rows["candidates"][0].get("truncated_cells"),
        }, ensure_ascii=False), flush=True)

    exact_number = require(
        client.get("/rows", params={"config": "justice", "offset": 0, "length": 1}),
        "justice_anchor",
    )["candidates"][0].get("case_number")
    assert exact_number, "justice first row case_number absent"

    for route, params in [
        ("/search", {"q": "nájem", "config": "justice", "limit": 1}),
        ("/candidates", {"q": "nájem", "config": "justice", "limit": 1}),
        ("/filter", {"case_number": exact_number, "config": "justice", "limit": 1}),
        ("/resolve", {"case_number": exact_number, "config": "justice", "limit": 1}),
        ("/provisions", {"q": "§ 2291", "limit": 1}),
    ]:
        data = require(client.get(route, params=params), route)
        assert data["source_gate"] == "CZ_SEARCH_LEAD_ONLY", route
        assert isinstance(data["count"], int), route
        assert not data["exhaustive_legal_corpus_search"], route
        print("RESULT", route, json.dumps({
            "count": data["count"],
            "upstream_partial": data.get("upstream_partial"),
            "num_rows_total_reported": data.get("num_rows_total_reported"),
        }, ensure_ascii=False), flush=True)

    case = require(client.get("/case", params={"config": "justice", "row_idx": 0}), "case")
    assert case["viewer_row"]["row_idx"] == 0
    assert case["source_gate"] == "CZ_SEARCH_LEAD_ONLY"
    quality = require(client.get("/quality", params={"config": "justice", "row_idx": 0}), "quality")
    assert quality["source_gate"] == "CZ_SEARCH_LEAD_ONLY"
    assert "full_text_truncated" in quality

print("CZ_SHELL_LIVE_SMOKE_PASS (retrieval only; official court holding NOT verified)", flush=True)
