"""Strict live smoke test: checks the running FastAPI app against Hugging Face.
No corpus download. Failure is a failed test, never a hidden warning.
"""
import json
import sys

from fastapi.testclient import TestClient

from app import app


def require(response, name):
    print("PROBE", name, "HTTP", response.status_code, flush=True)
    if response.status_code != 200:
        raise RuntimeError(f"{name}: HTTP {response.status_code}: {response.text[:500]}")
    data = response.json()
    if not isinstance(data, dict):
        raise AssertionError(f"{name}: result is not an object")
    return data


with TestClient(app) as client:
    capabilities = require(client.get("/capabilities"), "capabilities")
    assert capabilities["source_gate"] == "CZ_SEARCH_LEAD_ONLY"
    assert capabilities["index_mode"] == "remote", "live smoke expects no in-memory Parquet index"
    require(client.get("/ready"), "ready")

    splits = require(client.get("/splits"), "splits")
    available = {(item.get("config"), item.get("split")) for item in splits.get("splits", [])}
    assert ("justice", "train") in available, f"justice/train absent: {available}"
    assert ("czcdc", "train") in available, f"czcdc/train absent: {available}"

    for cfg in ("justice", "czcdc"):
        rows = require(client.get("/rows", params={"config": cfg, "offset": 0, "length": 1}), f"rows:{cfg}")
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
