from unittest import TestCase
import logging
import json
import os
from jsonschema import SchemaError

from hipages.utils import _parse_url, is_valid_data

cur_dir = os.path.dirname(__file__)


class TestUtils(TestCase):
    @classmethod
    def setUpClass(cls):
        logging.disable(logging.CRITICAL)

    @classmethod
    def tearDownClass(cls):
        logging.disable(logging.NOTSET)

    def test_should_correctly_parse_url(self):
        input_url = "https://www.hipages.com.au/find/electricians"
        expected_parsed_url = "www.hipages.com.au", "find", "electricians"
        actual = _parse_url(input_url)
        self.assertTupleEqual(expected_parsed_url, actual)

    def test_should_correctly_parse_url_with_qs(self):
        input_url = "https://www.hipages.com.au/find/electricians?search_str=sfdg"
        expected_parsed_url = "www.hipages.com.au", "find", "electricians"
        actual = _parse_url(input_url)
        self.assertTupleEqual(expected_parsed_url, actual)

    def test_should_correctly_parse_url_with_level_1_path(self):
        input_url = "https://www.hipages.com.au/find"
        expected_parsed_url = "www.hipages.com.au", "find", None
        actual = _parse_url(input_url)
        self.assertTupleEqual(expected_parsed_url, actual)

    def test_should_correctly_parse_url_without_path(self):
        input_url = "https://www.hipages.com.au/"
        expected_parsed_url = "www.hipages.com.au", None, None
        actual = _parse_url(input_url)
        self.assertTupleEqual(expected_parsed_url, actual)

    def test_should_correctly_parse_url_without_scheme(self):
        input_url = "www.hipages.com.au/find"
        expected_parsed_url = "www.hipages.com.au", "find", None
        actual = _parse_url(input_url)
        self.assertTupleEqual(expected_parsed_url, actual)

    def test_should_correctly_parse_empty_url(self):
        input_url = ""
        expected_parsed_url = None, None, None
        actual = _parse_url(input_url)
        self.assertTupleEqual(expected_parsed_url, actual)

    def test_should_correctly_parse_none_url(self):
        input_url = None
        expected_parsed_url = None, None, None
        actual = _parse_url(input_url)
        self.assertTupleEqual(expected_parsed_url, actual)

    def test_should_correctly_validate_data(self):
        input_doc = {"event_id": "945232832509254", "user": {"session_id": "564576", "id": 56456, "ip": "111.22.22.4"},
                     "action": "claim", "url": "https://www.hipages.com.au/find/electricians",
                     "timestamp": "02/02/2017 20:23:00"}
        with open(f"{cur_dir}/data/test_source_data_schema.json") as f:
            input_schema = json.load(f)
        expected = True
        actual = is_valid_data(input_doc, input_schema)
        self.assertEqual(expected, actual)

    def test_validate_data_invalid_schema_should_fail(self):
        input_doc = {"event_id": "945232832509254", "user": {"session_id": "564576", "id": 56456, "ip": "111.22.22.4"},
                     "action": "claim", "url": "https://www.hipages.com.au/find/electricians",
                     "timestamp": "02/02/2017 20:23:00"}
        with open(f"{cur_dir}/data/test_invalid_source_data_schema.json") as f:
            input_schema = json.load(f)
        with self.assertRaises(SchemaError):
            is_valid_data(input_doc, input_schema)

    def test_should_correctly_validate_data_invalid_doc(self):
        input_doc = {"event_id": "945232832509254", "user": {"id": 56456, "ip": "111.22.22.4"},
                     "action": "claim", "url": "https://www.hipages.com.au/find/electricians",
                     "timestamp": "02/02/2017 20:23:00"}
        with open(f"{cur_dir}/data/test_source_data_schema.json") as f:
            input_schema = json.load(f)
        expected = False
        actual = is_valid_data(input_doc, input_schema)
        self.assertEqual(expected, actual)
