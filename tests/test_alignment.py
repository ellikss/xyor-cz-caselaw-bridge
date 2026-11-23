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
        bridge.INDEX["justice"] = pl.DataFrame({"case_number": ["A"]})
        bridge.INDEX_STATUS["state"] = "building"

    def tearDown(self):
        bridge.INDEX.clear()
        bridge.INDEX.update(self.old)
        bridge.INDEX_STATUS["state"] = self.old_state

    def test_resolve_blocks_pending_index(self):
        with self.assertRaises(bridge.HTTPException) as ctx:
            asyncio.run(bridge.resolve_case("A"))
        self.assertEqual(ctx.exception.status_code, 503)

    def test_candidate_blocks_pending_index(self):
        with self.assertRaises(bridge.HTTPException) as ctx:
            asyncio.run(bridge.candidates("A"))
        self.assertEqual(ctx.exception.status_code, 503)


if __name__ == "__main__":
    unittest.main()
