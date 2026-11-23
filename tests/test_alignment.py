"""Offline regression tests. No Hugging Face access required."""
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import polars as pl

import app as bridge


class SourceAlignmentTests(unittest.TestCase):
    def setUp(self):
        self.df = pl.DataFrame({
            "case_number": ["A", "B", "C", "D"],
            "court_name": ["court"] * 4,
            "decision_date": ["2020-01-01"] * 4,
            "ecli": ["E1", "E2", "E3", "E4"],
        })

    def test_matching_rows_allow_candidate_index(self):
        async def fetch(config, idx):
            return {"row_idx": idx, "row": {
                "case_number": self.df[idx, "case_number"],
                "court_name": self.df[idx, "court_name"],
                "decision_date": self.df[idx, "decision_date"],
                "ecli": self.df[idx, "ecli"],
            }}
        with patch.object(bridge, "_get_row", side_effect=fetch):
            result = asyncio.run(bridge._check_index_alignment("justice", self.df))
        self.assertTrue(result["alignment_verified"])
        self.assertEqual([x["row_idx"] for x in result["alignment_probes"]], [0, 2, 3])

    def test_mismatch_fails_closed(self):
        async def fetch(config, idx):
            return {"row_idx": idx, "row": {
                "case_number": "WRONG" if idx == 2 else self.df[idx, "case_number"],
                "court_name": "court",
                "decision_date": "2020-01-01",
                "ecli": self.df[idx, "ecli"],
            }}
        with patch.object(bridge, "_get_row", side_effect=fetch):
            with self.assertRaisesRegex(RuntimeError, "INDEX_SOURCE_MISMATCH"):
                asyncio.run(bridge._check_index_alignment("justice", self.df))

    def test_viewer_offset_disagreement_fails_closed(self):
        async def fake_hf_get(endpoint, params):
            return {"rows": [{"row_idx": 99, "row": {"case_number": "A"}}]}
        with patch.object(bridge, "hf_get", side_effect=fake_hf_get):
            with self.assertRaises(bridge.HTTPException) as ctx:
                asyncio.run(bridge._get_row("justice", 0))
        self.assertEqual(ctx.exception.status_code, 503)

    def test_empty_index_rejected(self):
        with self.assertRaises(RuntimeError):
            asyncio.run(bridge._check_index_alignment("justice", self.df.head(0)))

    def test_encoding_damage_blocks_quote(self):
        self.assertFalse(bridge._text_quality("broken \ufffd")["encoding_quote_gate_pass"])


class EndpointReadinessTests(unittest.TestCase):
    def setUp(self):
        self.old = dict(bridge.INDEX)
        self.old_state = bridge.INDEX_STATUS["state"]
        self.old_mode = bridge.INDEX_MODE
        bridge.INDEX_MODE = "memory"
        bridge.INDEX["justice"] = pl.DataFrame({"case_number": ["A"]})
        bridge.INDEX_STATUS["state"] = "building"

    def tearDown(self):
        bridge.INDEX.clear()
        bridge.INDEX.update(self.old)
        bridge.INDEX_STATUS["state"] = self.old_state
        bridge.INDEX_MODE = self.old_mode

    def test_resolve_blocks_pending_index(self):
        with self.assertRaises(bridge.HTTPException) as ctx:
            asyncio.run(bridge.resolve_case("A"))
        self.assertEqual(ctx.exception.status_code, 503)

    def test_candidate_blocks_pending_index(self):
        with self.assertRaises(bridge.HTTPException) as ctx:
            asyncio.run(bridge.candidates("A"))
        self.assertEqual(ctx.exception.status_code, 503)




class RemoteShellTests(unittest.TestCase):
    def test_search_returns_metadata_only_and_search_lead(self):
        async def fake(endpoint, params, timeout=45.0):
            self.assertEqual(endpoint, "search")
            self.assertEqual(params["config"], "justice")
            self.assertEqual(params["query"], "nájem")
            return {"rows": [{"row_idx": 8, "row": {
                "case_number": "22 C 11/2020", "court_name": "Okresní soud",
                "full_text": "Confidential hypothetical decision text",
                "ecli": None, "subject": "nájem",
            }, "truncated_cells": []}], "num_rows_total": 1, "partial": False}
        with patch.object(bridge, "hf_get", side_effect=fake):
            data = asyncio.run(bridge.search("nájem", config="justice", limit=1, offset=0))
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["candidates"][0]["row_idx"], 8)
        self.assertTrue(data["candidates"][0]["has_text"])
        self.assertNotIn("full_text", data["candidates"][0])
        self.assertEqual(data["source_gate"], "CZ_SEARCH_LEAD_ONLY")
        self.assertFalse(data["exhaustive_legal_corpus_search"])

    def test_exact_filter_escapes_sql_quote(self):
        async def fake(endpoint, params, timeout=45.0):
            self.assertEqual(endpoint, "filter")
            self.assertEqual(params["where"], '"case_number"=\'O\'\'Hara\'')
            return {"rows": [], "partial": True}
        with patch.object(bridge, "hf_get", side_effect=fake):
            data = asyncio.run(bridge._remote_filter("justice", 5, case_number="O'Hara"))
        self.assertIs(data["upstream_partial"], True)
        self.assertFalse(data["no_match_is_conclusive"])

    def test_remote_resolve_without_memory_index(self):
        async def fake(endpoint, params, timeout=45.0):
            self.assertEqual(endpoint, "filter")
            self.assertEqual(params["where"], '"case_number"=\'10 C 73/2020-127\'')
            return {"rows": [{"row_idx": 15, "row": {
                "case_number": "10 C 73/2020-127", "court_name": "court",
            }}], "partial": False}
        with patch.object(bridge, "INDEX_MODE", "remote"), patch.object(bridge, "hf_get", side_effect=fake):
            data = asyncio.run(bridge.resolve_case("10 C 73/2020-127", limit=5))
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["candidates"][0]["row_idx"], 15)

    def test_wrong_config_rejected(self):
        with self.assertRaises(bridge.HTTPException) as ctx:
            asyncio.run(bridge._remote_search("some term", "invalid", 2))
        self.assertEqual(ctx.exception.status_code, 422)

    def test_broken_viewer_row_rejected(self):
        with self.assertRaises(bridge.HTTPException) as ctx:
            bridge._remote_result(
                {"rows": [{"row_idx": -1, "row": {"case_number": "A"}}]},
                config="justice", mode="HF_TEXT_SEARCH", query="A",
            )
        self.assertEqual(ctx.exception.status_code, 502)

    def test_truncated_evidence_blocked(self):
        async def fake_get_row(config, row_idx):
            return {"row_idx": row_idx, "row": {
                "full_text": "the relevant phrase", "case_number": "ABC",
            }, "truncated_cells": ["full_text"]}
        with patch.object(bridge, "_get_row", side_effect=fake_get_row):
            data = asyncio.run(bridge.evidence("justice", 0, "relevant", 100, 5))
        self.assertEqual(data["matches"], [])
        self.assertFalse(data["encoding_quote_gate_pass"])

    def test_ready_rejects_viewer_without_search(self):
        async def fake(endpoint, params, timeout=45.0):
            return {"viewer": True, "search": False, "filter": False}
        with patch.object(bridge, "hf_get", side_effect=fake):
            data = asyncio.run(bridge.ready())
        self.assertFalse(data["hf_search"])
        self.assertFalse(data["hf_filter"])


class ApiValidationTests(unittest.TestCase):
    def test_invalid_config_is_http_422(self):
        from fastapi.testclient import TestClient
        with TestClient(bridge.app) as client:
            response = client.get("/search", params={"config": "not-a-config", "q": "civil"})
        self.assertEqual(response.status_code, 422)

    def test_search_requires_query_min_length(self):
        from fastapi.testclient import TestClient
        with TestClient(bridge.app) as client:
            response = client.get("/search", params={"config": "justice", "q": "x"})
        self.assertEqual(response.status_code, 422)

    def test_filter_rejects_missing_predicates(self):
        from fastapi.testclient import TestClient
        with TestClient(bridge.app) as client:
            response = client.get("/filter", params={"config": "justice"})
        self.assertEqual(response.status_code, 422)


if __name__ == "__main__":
    unittest.main()
