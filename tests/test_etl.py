from unittest import TestCase
import logging
import io
import os
from pandas import DataFrame
from hipages.etl import USER_ACTIVITIES_OUTPUT_FILENAME, AGG_EVENTS_OUTPUT_FILENAME
from pandas._testing import assert_frame_equal

from hipages.etl import extract_data, transform_clean_data, \
    transform_data_for_user_activities, transform_data_for_agg_events, load_data

cur_dir = os.path.dirname(__file__)

class TestEtl(TestCase):
    @classmethod
    def setUpClass(cls):
        logging.disable(logging.CRITICAL)

    @classmethod
    def tearDownClass(cls):
        logging.disable(logging.NOTSET)

    def test_should_correctly_read_file(self):
        input_filename = f"{cur_dir}/data/test_source_event_data.json"
        input_schema = f"{cur_dir}/data/test_source_data_schema.json"
        data = [{"event_id": "556768754511595", "user": {"session_id": "580633", "id": 66374, "ip": "111.222.33.5"},
                 "action": "page_view", "url": "https://www.hipages.com.au/articles",
                 "timestamp": "02/02/2017 20:26:00"}]
        expected_df = DataFrame(data)
        actual_df = extract_data(input_filename, input_schema)
        assert_frame_equal(expected_df, actual_df)

    def test_should_correctly_read_file_and_ignore_invalid_json_line(self):
        input_filename = f"{cur_dir}/data/test_invalid_json_source_event_data.json"
        input_schema = f"{cur_dir}/data/test_source_data_schema.json"
        data = [{"event_id": "556768754511595", "user": {"session_id": "580633", "id": 66374, "ip": "111.222.33.5"},
                 "action": "page_view", "url": "https://www.hipages.com.au/articles",
                 "timestamp": "02/02/2017 20:26:00"}]
        expected_df = DataFrame(data)
        actual_df = extract_data(input_filename, input_schema)
        assert_frame_equal(expected_df, actual_df)

    def test_read_file_with_invalid_path_should_fail(self):
        input_filename = "invalid_path.json"
        input_schema = f"{cur_dir}/data/test_source_data_schema.json"
        with self.assertRaises(FileNotFoundError):
            extract_data(input_filename, input_schema)

    def test_should_correctly_clean_data(self):
        input_data = [
            {"event_id": "556768754511595", "user": {"session_id": "580633", "id": 66374, "ip": "111.222.33.5"},
             "action": "page_view", "url": "https://www.hipages.com.au/articles",
             "timestamp": "02/02/2017 20:26:00"}]
        input_df = DataFrame(input_data)
        expected_data = [
            {"event_id": "556768754511595", "user_session_id": "580633", "user_id": 66374, "user_ip": "111.222.33.5",
             "time_stamp": "02/02/2017 20:26:00", "url": "https://www.hipages.com.au/articles",
             "url_level1": "www.hipages.com.au", "url_level2": "articles", "url_level3": None, "activity": "page_view"}]
        df_dtypes = {"event_id": "string", "url": "string", "activity": "string", "time_stamp": "datetime64[ns]"}
        expected_df = DataFrame(expected_data).astype(df_dtypes)
        actual_df = transform_clean_data(input_df)
        assert_frame_equal(expected_df, actual_df)

    def test_should_correctly_transform_data_user_activities(self):
        input_data = [
            {"event_id": "556768754511595", "user_session_id": "580633", "user_id": 66374, "user_ip": "111.222.33.5",
             "time_stamp": "02/02/2017 20:26:00", "url": "https://www.hipages.com.au/articles",
             "url_level1": "www.hipages.com.au", "url_level2": "articles", "url_level3": None, "activity": "page_view"}]
        df_dtypes = {"event_id": "string", "url": "string", "activity": "string", "time_stamp": "datetime64[ns]"}
        input_df = DataFrame(input_data).astype(df_dtypes)
        expected_data = [
            {"user_id": 66374, "time_stamp": "02/02/2017 20:26:00", "url_level1": "www.hipages.com.au",
             "url_level2": "articles", "url_level3": None, "activity": "page_view"}]
        df_dtypes = {"activity": "string", "time_stamp": "datetime64[ns]"}
        expected_df = DataFrame(expected_data).astype(df_dtypes)
        expected_output_filename = USER_ACTIVITIES_OUTPUT_FILENAME
        actual_df, actual_output_filename = transform_data_for_user_activities(input_df)
        assert_frame_equal(expected_df, actual_df)
        self.assertEqual(expected_output_filename, actual_output_filename)

    def test_should_correctly_transform_data_for_agg_events(self):
        input_data = [
            {"event_id": "556768754511595", "user_session_id": "580633", "user_id": 66374, "user_ip": "111.222.33.5",
             "time_stamp": "02/02/2017 20:26:00", "url": "https://www.hipages.com.au/articles",
             "url_level1": "www.hipages.com.au", "url_level2": "articles", "url_level3": None, "activity": "page_view"}]
        df_dtypes = {"event_id": "string", "url": "string", "activity": "string", "time_stamp": "datetime64[ns]"}
        input_df = DataFrame(input_data).astype(df_dtypes)
        expected_data = [{"time_bucket": "2017020220", "url_level1": "www.hipages.com.au", "url_level2": "articles",
                          "activity": "page_view", "activity_count": 1, "user_count": 1}]
        expected_df = DataFrame(expected_data)
        expected_output_filename = AGG_EVENTS_OUTPUT_FILENAME
        actual_df, actual_output_filename = transform_data_for_agg_events(input_df)
        assert_frame_equal(expected_df, actual_df)
        self.assertEqual(expected_output_filename, actual_output_filename)

    def test_should_correctly_transform_data_for_agg_events_with_multiple_activities_and_users(self):
        input_data = [
            {"event_id": "556768754511595", "user_session_id": "580633", "user_id": 66374, "user_ip": "111.222.33.5",
             "time_stamp": "02/02/2017 20:26:00", "url": "https://www.hipages.com.au/articles",
             "url_level1": "www.hipages.com.au", "url_level2": "articles", "url_level3": None, "activity": "page_view"},
            {"event_id": "556768754511596", "user_session_id": "580634", "user_id": 66374, "user_ip": "111.222.33.5",
             "time_stamp": "02/02/2017 20:26:00", "url": "https://www.hipages.com.au/articles",
             "url_level1": "www.hipages.com.au", "url_level2": "articles", "url_level3": None, "activity": "page_view"},
            {"event_id": "556768754511597", "user_session_id": "580635", "user_id": 66374, "user_ip": "111.222.33.5",
             "time_stamp": "02/02/2017 20:26:00", "url": "https://www.hipages.com.au/articles",
             "url_level1": "www.hipages.com.au", "url_level2": "articles", "url_level3": None,
             "activity": "button_click"},
            {"event_id": "556768754511598", "user_session_id": "580636", "user_id": 66375, "user_ip": "111.222.34.5",
             "time_stamp": "02/02/2017 20:26:00", "url": "https://www.hipages.com.au/articles",
             "url_level1": "www.hipages.com.au", "url_level2": "articles", "url_level3": None, "activity": "page_view"},
            {"event_id": "556768754511599", "user_session_id": "580637", "user_id": 66375, "user_ip": "111.222.34.5",
             "time_stamp": "02/02/2017 20:26:00", "url": "https://www.hipages.com.au/articles",
             "url_level1": "www.hipages.com.au", "url_level2": "articles", "url_level3": None,
             "activity": "button_click"}]
        df_dtypes = {"event_id": "string", "url": "string", "activity": "string", "time_stamp": "datetime64[ns]"}
        input_df = DataFrame(input_data).astype(df_dtypes)
        expected_data = [
            {"time_bucket": "2017020220", "url_level1": "www.hipages.com.au", "url_level2": "articles",
             "activity": "button_click", "activity_count": 2, "user_count": 2},
            {"time_bucket": "2017020220", "url_level1": "www.hipages.com.au", "url_level2": "articles",
             "activity": "page_view", "activity_count": 3, "user_count": 2}
        ]
        expected_df = DataFrame(expected_data)
        expected_output_filename = AGG_EVENTS_OUTPUT_FILENAME
        actual_df, actual_output_filename = transform_data_for_agg_events(input_df)
        assert_frame_equal(expected_df, actual_df)
        self.assertEqual(expected_output_filename, actual_output_filename)

    def test_should_correctly_load_data(self):
        input_data = [{"time_bucket": "2017020220", "url_level1": "www.hipages.com.au", "url_level2": "articles",
                       "activity": "page_view", "activity_count": 1, "user_count": 1}]
        input_df = DataFrame(input_data)
        expected_file = '''time_bucket,url_level1,url_level2,activity,activity_count,user_count
2017020220,www.hipages.com.au,articles,page_view,1,1\n'''
        actual_file = io.StringIO()
        load_data((input_df, actual_file))
        actual_file.seek(0)
        self.assertEqual(expected_file, actual_file.read())

