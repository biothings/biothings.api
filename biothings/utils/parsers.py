import pathlib
from typing import Callable, Generator, Iterable, Optional

import orjson


def ndjson_parser(patterns: Optional[Iterable[str]] = None,) \
        -> Callable[[str], Generator[dict, None, None]]:
    """
    Create NDJSON Parser given filename patterns

    For use with manifest.json based plugins.
    Caveat: Only handles valid NDJSON (no extra newlines, UTF8, etc.)

    Args:
        patterns: glob-compatible patterns for filenames, like *.ndjson, data*.ndjson

    Returns:
        parser_func: Generator that takes in a data_folder and returns documents from
            NDJSON files that matches the filename patterns
    """
    if patterns is None:
        raise TypeError("Must provide keyword argument patterns to"
                        "match files for NDJSON Parser")

    def ndjson_parser_func(data_folder):
        work_dir = pathlib.Path(data_folder)
        for pattern in patterns:
            for filename in work_dir.glob(pattern):
                with open(filename, 'rb') as f:
                    for line in f:
                        doc = orjson.loads(line)
                        yield doc

    return ndjson_parser_func


def json_array_parser(patterns: Optional[Iterable[str]] = None) \
        -> Callable[[str], Generator[dict, None, None]]:
    """
       Create JSON Array Parser given filename patterns

       For use with manifest.json based plugins. The data comes in a JSON that is
       an JSON array, containing multiple documents.

       Args:
           patterns: glob-compatible patterns for filenames, like *.json, data*.json

       Returns:
           parser_func
    """
    if patterns is None:
        raise TypeError("Must provide keyword argument patterns to"
                        "match files for JSON Array Parser")

    def json_array_parser(data_folder):
        work_dir = pathlib.Path(data_folder)
        for pattern in patterns:
            for filename in work_dir.glob(pattern):
                with open(filename, 'r') as f:
                    data = orjson.loads(f.read())
                    try:
                        iterator = iter(data)
                    except TypeError:
                        raise RuntimeError(f"{filename} does not contain a valid"
                                           "JSON Array")
                    for doc in iterator:
                        yield doc

    return json_array_parser
