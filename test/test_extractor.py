#!/usr/bin/env/python

import unittest
import json
from pathlib import Path

from extractor.main import process_text, SegmentWordListUrl 

TEST_DIR = Path(__file__).parent 

JSON_TEST_FILE = TEST_DIR / "test.json"
EXPECTED_RESULT_FILE = TEST_DIR / "result.json"

class TestExtractor(unittest.TestCase):

    def test_frequencies(self):
        with JSON_TEST_FILE.open() as json_in:
            segments = json.load(json_in)
        segments_word_list = SegmentWordListUrl(**segments)
        
        with EXPECTED_RESULT_FILE.open() as result_in:
            result_expected = json.load(result_in)
        result = process_text(segments_word_list)
        result = result.dict()
        self.assertTrue("metadata" in result)
        self.assertTrue("word_list" in result)
        self.assertCountEqual(result["word_list"], result_expected["word_list"])
