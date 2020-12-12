from typing import Tuple
import pandas as pd
from pandas import DataFrame
import json
import os
from hipages.utils import log, is_valid_data, parse_url_series

cur_dir = os.path.dirname(__file__)

pd.set_option("display.max_columns", None)

SOURCE_EVENT_DATA_FILENAME = f'{cur_dir}/data/source_event_data.json'
SOURCE_EVENT_SCHEMA_FILENAME = f'{cur_dir}/data/source_data_schema.json'
USER_ACTIVITIES_OUTPUT_FILENAME = f'{cur_dir}/output/user_activities.csv'
AGG_EVENTS_OUTPUT_FILENAME = f'{cur_dir}/output/agg_events.csv'


def extract_data(filename: str, schema_filename: str) -> DataFrame:
    """
    Extracts valid json documents from json lines file into a DataFrame
    :param filename: Filename of the json lines file
    :param schema_filename: Json schema file needed to validate json documents
    :return: Pandas dataFrame with valid data
    """
    data = []
    try:
        with open(schema_filename) as f:
            schema = json.load(f)
        with open(filename) as f:
            for line in f:
                json_doc = json.loads(line)
                if is_valid_data(json_doc, schema):
                    data.append(json_doc)
    except ValueError as e:
        log.error(f"Error parsing json: {e}")
    except FileNotFoundError as e:
        log.error(f"File not found error: {e}")
        raise e
    except Exception as e:
        log.error(e)
        raise e
    return DataFrame(data)


def transform_clean_data(df: DataFrame) -> DataFrame:
    """
    Transforms raw data to clean data by normalizing object field and parsing url
    :param df: raw pandas dataframe
    :return: clean pandas dataframe
    """
    df_dtypes = {"event_id": "string",
                 "user": "object",
                 "action": "string",
                 "url": "string",
                 "timestamp": "datetime64[ns]"}
    df = df.astype(df_dtypes)
    user_df = pd.json_normalize(df['user']).add_prefix('user_')
    df = df.join([user_df])
    df.drop('user', inplace=True, axis=1)
    df.rename(columns={'timestamp': 'time_stamp', 'action': 'activity'}, inplace=True)
    df = df.merge(df['url'].apply(parse_url_series), left_index=True, right_index=True)
    df = df[['event_id', 'user_session_id', 'user_id', 'user_ip', 'time_stamp',
             'url', 'url_level1', 'url_level2', 'url_level3', 'activity']]
    return df


def transform_data_for_user_activities(df: DataFrame) -> Tuple[DataFrame, str]:
    """
    Transforms clean pandas dataframe into a tuple of the required
    table 1 dataframe and output filename to be stored
    :param df: clean pandas dataframe
    :return: Tuple of pandas dataframe and output filename
    """
    user_activities_df = df[['user_id', 'time_stamp', 'url_level1', 'url_level2', 'url_level3', 'activity']]
    return user_activities_df, USER_ACTIVITIES_OUTPUT_FILENAME


def transform_data_for_agg_events(df: DataFrame) -> Tuple[DataFrame, str]:
    """
    Transforms clean pandas dataframe into a tuple of the required
    table 2 dataframe and output filename to be stored
    :param df: clean pandas dataframe
    :return: Tuple of pandas dataframe and output filename
    """
    agg_events_df = df
    agg_events_df['time_bucket'] = agg_events_df['time_stamp'].dt.strftime("%Y%d%m%H")
    agg_events_df = agg_events_df.groupby(['time_bucket', 'url_level1', 'url_level2', 'activity']).agg(
        {'event_id': 'count', "user_id": 'nunique'}).rename(
        columns={'event_id': 'activity_count', 'user_id': 'user_count'}).reset_index()
    return agg_events_df, AGG_EVENTS_OUTPUT_FILENAME


def load_data(data: Tuple[DataFrame, str]) -> None:
    """
    Loads data into an output csv file
    :param data: Tuple of pandas dataframe and output filename
    :return: None
    """
    (df, output_filename) = data
    df.to_csv(output_filename, index=False)


def etl(filename: str, schema_filename: str) -> None:
    """
    Extract, transform and Load process to generate an output in the form of
    two tables (CSV) using the source data filename and schema data filename
    :param filename: Name of the source file
    :param schema_filename: Name of the schema file
    :return: None
    """
    raw_df = extract_data(filename, schema_filename)
    clean_df = transform_clean_data(raw_df)
    user_activities_df = transform_data_for_user_activities(clean_df)
    agg_events_df = transform_data_for_agg_events(clean_df)
    load_data(user_activities_df)
    load_data(agg_events_df)


if __name__ == '__main__':
    etl(SOURCE_EVENT_DATA_FILENAME, SOURCE_EVENT_SCHEMA_FILENAME)
