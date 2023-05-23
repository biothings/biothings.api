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
        raise TypeError("Must provide keyword argument patterns to" "match files for NDJSON Parser")

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
        raise TypeError("Must provide keyword argument patterns to" "match files for JSON Array Parser")

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


def docker_source_info_parser(url):
    """
    :param url: file url include docker connection string
        format: docker://CONNECTION_NAME?image=DOCKER_IMAGE&tag=TAG&dump_command="python run.py"&path=/path/to/file
        the CONNECTION_NAME must be defined in the biothings Hub config.
        example:
        docker://CONNECTION_NAME?image=docker_image&tag=docker_tag&dump_command="python run.py"&path=/path/to/file
        docker://CONNECTION_NAME?image=docker_image&tag=docker_tag&dump_command="python run.py"&path=/path/to/file
        docker://CONNECTION_NAME?image=docker_image&tag=docker_tag&dump_command="python run.py"&path=/path/to/file
        docker"//CONNECTION_NAME?image=docker_image&tag=docker_tag&dump_command="python run.py"&path=/path/to/file
    :return:

    """
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query))
    image = query.get("image")
    image_tag = query.get("tag")
    dump_command = query.get("dump_command")
    keep_container = query.get("keep_container")
    container_name = query.get("container_name")
    get_version_cmd = query.get("get_version_cmd")
    if keep_container:
        keep_container = keep_container.lower() in {"true", "yes", "1", "y"}
    if dump_command:
        dump_command = dump_command.strip('"')
    if get_version_cmd:
        get_version_cmd = get_version_cmd.strip('"')
    if not image_tag:
        image_tag = "latest"
    docker_image = image and f"{image}:{image_tag}" or None
    source_config = {
        "docker_image": docker_image,
        "path": query.get("path"),
        "dump_command": dump_command,
        "connection_name": parsed.netloc,
        "container_name": container_name,
        "keep_container": keep_container,
        "get_version_cmd": get_version_cmd,
    }
    if keep_container is None:
        # remove keep_container if not set, so that later we can check its value from image/container metadata
        source_config.pop("keep_container")
    return source_config
