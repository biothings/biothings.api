import pathlib
from typing import Callable, Generator, Iterable, Optional
from urllib.parse import parse_qsl, urlparse

import orjson


def ndjson_parser(
    patterns: Optional[Iterable[str]] = None,
) -> Callable[[str], Generator[dict, None, None]]:
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
        raise TypeError(
            "Must provide keyword argument patterns to" "match files for NDJSON Parser"
        )

    def ndjson_parser_func(data_folder):
        work_dir = pathlib.Path(data_folder)
        for pattern in patterns:
            for filename in work_dir.glob(pattern):
                with open(filename, "rb") as f:
                    for line in f:
                        doc = orjson.loads(line)
                        yield doc

    return ndjson_parser_func


def json_array_parser(
    patterns: Optional[Iterable[str]] = None,
) -> Callable[[str], Generator[dict, None, None]]:
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
        raise TypeError(
            "Must provide keyword argument patterns to" "match files for JSON Array Parser"
        )

    def json_array_parser(data_folder):
        work_dir = pathlib.Path(data_folder)
        for pattern in patterns:
            for filename in work_dir.glob(pattern):
                with open(filename, "r") as f:
                    data = orjson.loads(f.read())
                    try:
                        iterator = iter(data)
                    except TypeError:
                        raise RuntimeError(f"{filename} does not contain a valid" "JSON Array")
                    for doc in iterator:
                        yield doc

    return json_array_parser


def docker_connection_string_parser(url):
    """
    :param url: file url include docker connection string
        format: docker+DOCKER_CLIENT_URL?image=DOCKER_IMAGE&tag=TAG&custom_cmd="python run.py"&path=/path/to/file
        example:
        docker+ssh://remote_ip:1234?image=docker_image&tag=docker_tag&custom_cmd="python run.py"&path=/path/to/file
        docker+unix://var/run/docker.sock?image=docker_image&tag=docker_tag&custom_cmd="python run.py"&path=/path/to/file
        docker+http://remote_ip:1234?image=docker_image&tag=docker_tag&custom_cmd="python run.py"&path=/path/to/file
        docker+https://remote_ip:1234?image=docker_image&tag=docker_tag&custom_cmd="python run.py"&path=/path/to/file
    :return:

    """
    parsed = urlparse(url)
    scheme = parsed.scheme
    scheme = scheme.replace("docker+", "")
    if scheme == "unix":
        docker_client_url = url.rsplit("?", 1)[0].replace("docker+", "")
    else:
        docker_client_url = f"{scheme}://{parsed.netloc}"
    query = dict(parse_qsl(parsed.query))
    image = query["image"]
    image_tag = query.get("tag")
    custom_cmd = query.get("custom_cmd")
    if not image_tag:
        image_tag = "latest"
    file_path = query["path"]
    return {
        "scheme": scheme,
        "docker_image": f"{image}:{image_tag}",
        "file_path": file_path,
        "docker_client_url": docker_client_url,
        "custom_cmd": custom_cmd,
    }
