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

