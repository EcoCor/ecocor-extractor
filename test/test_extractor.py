#!/usr/bin/env/python

import sys
import unittest
from unittest.mock import patch
import json
from pathlib import Path

from extractor.main import process_text, SegmentEntityListUrl 

TEST_DIR = Path(__file__).parent 

JSON_TEST_FILE = TEST_DIR / "test.json"

class TestExtractor(unittest.TestCase):

    def run_test_frequencies(self, expected_result_file, args=[]):
        with patch("sys.argv", ["script.py"] + args):
            with JSON_TEST_FILE.open() as json_in:
                segments = json.load(json_in)
            segments_name_list = SegmentEntityListUrl(**segments)
            
            with (TEST_DIR / expected_result_file).open() as result_in:
                result_expected = json.load(result_in)
            result = process_text(segments_name_list)
            result = result.model_dump()
            self.assertTrue("metadata" in result)
            self.assertTrue("entity_list" in result)
            self.assertCountEqual(result["entity_list"], result_expected["entity_list"])

    def test(self):
        self.run_test_frequencies("result.json");

    def test_noun_only(self):
        self.run_test_frequencies("result_noun_only.json", ["--noun-only"]);

    def test_default_entity_list_de(self):
        segments_name_list = SegmentEntityListUrl(
            language="de",
            segments=[
                {"segment_id": "P0", "text": "Ein Hund und eine Katze spielen."},
                {"segment_id": "P1", "text": "Noch ein Hund."},
            ],
        )
        result = process_text(segments_name_list).model_dump()
        found = {e["name"]: e for e in result["entity_list"]}
        self.assertIn("Hund", found)
        self.assertEqual(found["Hund"]["overall_frequency"], 2)
        self.assertEqual(found["Hund"]["segment_frequencies"], {"P0": 1, "P1": 1})
        self.assertIn("Katze", found)
        self.assertEqual(found["Katze"]["overall_frequency"], 1)

    def test_default_entity_list_en(self):
        segments_name_list = SegmentEntityListUrl(
            language="en",
            segments=[
                {"segment_id": "P0", "text": "A dog and a cat play."},
                {"segment_id": "P1", "text": "Another dog."},
            ],
        )
        result = process_text(segments_name_list).model_dump()
        found = {e["name"]: e for e in result["entity_list"]}
        self.assertIn("dog", found)
        self.assertEqual(found["dog"]["overall_frequency"], 2)
        self.assertEqual(found["dog"]["segment_frequencies"], {"P0": 1, "P1": 1})
        self.assertIn("cat", found)
        self.assertEqual(found["cat"]["overall_frequency"], 1)

