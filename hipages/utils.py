import logging
from urllib import parse
from typing import Tuple, Optional
from jsonschema import validate, ValidationError, SchemaError
from pandas import Series

log = logging.getLogger(__name__)

def is_valid_data(doc: dict, schema: dict) -> bool:
    """
    Validates that a document dictionary follows a JSON schema dictionary
    :param doc: document to validate
    :param schema: JSON Schema used to validate the document
    :return: Returns True if the document follows the JSON Schema
             Returns False if not
             Raises an error if the schema document is an invalid JSON schema
    """
    try:
        validate(doc, schema)
        return True
    except ValidationError as e:
        log.warning(f"Error validating schema {e}")
        return False
    except SchemaError as e:
        log.error(f"Invalid Schema {e}")
        raise e


def _parse_url(url: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parses a url to extract the netloc and the two first sub paths in the url
    If the scheme is missing, we consider that netloc is the first element before the first '/'
    :param url: url to parse
    :return: Tuple containing the netloc, first and second sub paths of the url
    """
    if not url:
        return None, None, None
    url_split = parse.urlsplit(str(url))
    (_, netloc, path, _, _) = url_split
    stripped_path = path.strip('/')
    sub_paths = stripped_path.split('/') if stripped_path else []
    n = len(sub_paths)
    if not netloc and path:
        # Handle case where scheme is not in the url.
        # We could decide to consider this as an invalid url and handle it differently
        return sub_paths[0] if n > 0 else None, sub_paths[1] if n > 1 else None, sub_paths[2] if n > 2 else None
    return netloc if netloc else None, sub_paths[0] if n > 0 else None, sub_paths[1] if n > 1 else None


def parse_url_series(url: Optional[str]) -> Series:
    """
    Utility function needed to apply _parse_url to a pandas dataframe's column
    :param url: url to parse
    :return: Pandas series with the three parsed levels of the url
    """
    url_level1, url_level2, url_level3 = _parse_url(url)
    return Series({
        'url_level1': str(url_level1) if url_level1 else None,
        'url_level2': str(url_level2) if url_level2 else None,
        'url_level3': str(url_level3) if url_level3 else None})
