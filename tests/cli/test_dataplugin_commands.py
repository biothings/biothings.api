"""
Tests the `biothings-cli dataplugin` commands

The commands run in a subprocess from a temporary directory, the same way a user runs them.
The CLI loads its configuration from the working directory into sys.modules["biothings.config"],
which would replace the test configuration if the commands ran in the test process
"""

import functools
import http.server
import json
import os
import pathlib
import subprocess
import sys
import threading

import pytest
import typer
import yaml

from biothings.cli.commands.operations import _resolve_sub_source_name, do_create
from biothings.cli.exceptions import MissingPluginName
from biothings.hub.dataplugin.loaders.loader import ManifestBasedPluginLoader
from biothings.hub.dataplugin.loaders.schema.exceptions import ManifestTypeException

PARSER = """
import csv
import os


def load_data(data_folder):
    with open(os.path.join(data_folder, "genes.tsv"), encoding="utf-8") as handle:
        for row in csv.DictReader(handle, delimiter="\\t"):
            yield {"_id": row["id"], "symbol": row["symbol"], "score": float(row["score"])}
"""


def run_cli(*arguments: str, cwd: pathlib.Path) -> subprocess.CompletedProcess:
    """
    Runs the biothings-cli with the given arguments. stderr is merged into stdout
    """
    # wide enough for rich to not wrap the messages we check
    environment = dict(os.environ, COLUMNS="1000")
    for variable in ("FORCE_COLOR", "BTCLI_DEBUG", "BTCLI_RICH_TRACEBACK"):
        environment.pop(variable, None)
    return subprocess.run(
        [sys.executable, "-c", "from biothings.cli import main; main()", *arguments],
        cwd=cwd,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=120,
        check=False,
    )


def create_plugin(working_directory: pathlib.Path) -> pathlib.Path:
    """
    Creates the "demo" data plugin from the template
    """
    result = run_cli("dataplugin", "create", "--name", "demo", cwd=working_directory)
    assert result.returncode == 0, result.stdout
    return working_directory / "demo"


def set_data_url(plugin_directory: pathlib.Path, data_url: str) -> None:
    manifest_file = plugin_directory / "manifest.yaml"
    manifest = yaml.safe_load(manifest_file.read_text(encoding="utf-8"))
    manifest["dumper"]["data_url"] = data_url
    manifest_file.write_text(yaml.safe_dump(manifest), encoding="utf-8")


@pytest.fixture(scope="module")
def data_url(tmp_path_factory) -> str:
    """
    Serves a small TSV file over HTTP for the data plugin dumper
    """
    data_directory = tmp_path_factory.mktemp("cli_data")
    data_directory.joinpath("genes.tsv").write_text(
        "id\tsymbol\tscore\nG1\tTP53\t0.9\nG2\tBRCA1\t0.7\n", encoding="utf-8"
    )
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(data_directory))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/genes.tsv"
    server.shutdown()
    server.server_close()
    thread.join()


def test_create_reports_plugin_location(tmp_path: pathlib.Path):
    result = run_cli("dataplugin", "create", "--name", "demo", cwd=tmp_path)
    assert result.returncode == 0, result.stdout
    assert tmp_path.joinpath("demo", "manifest.yaml").exists()
    assert f"Successfully created data plugin template at: {tmp_path.resolve() / 'demo'}" in result.stdout


@pytest.mark.parametrize("multi_uploaders", [False, True])
@pytest.mark.parametrize("parallelizer", [False, True])
def test_created_manifest_only_requires_data_url(
    multi_uploaders: bool, parallelizer: bool, tmp_path: pathlib.Path, monkeypatch
):
    """
    The manifest template has to load once the required data_url is filled in.
    Optional fields left empty in the template are null values, rejected by the manifest schema
    """
    monkeypatch.chdir(tmp_path)
    do_create("demo", multi_uploaders=multi_uploaders, parallelizer=parallelizer)
    manifest = yaml.safe_load(tmp_path.joinpath("demo", "manifest.yaml").read_text(encoding="utf-8"))
    manifest_loader = ManifestBasedPluginLoader(plugin_name="demo")

    with pytest.raises(ManifestTypeException, match=r"\['dumper', 'data_url'\]"):
        manifest_loader.validate_manifest(manifest)

    manifest["dumper"]["data_url"] = "https://example.com/data.tsv"
    manifest_loader.validate_manifest(manifest)
    assert "schedule" not in manifest["dumper"]


def test_validate_yaml_manifest(tmp_path: pathlib.Path):
    plugin_directory = create_plugin(tmp_path)

    result = run_cli("dataplugin", "validate", cwd=plugin_directory)
    assert result.returncode == 1, result.stdout
    assert "Please update the ['dumper', 'data_url'] section of the manifest" in result.stdout

    set_data_url(plugin_directory, "https://example.com/data.tsv")
    result = run_cli("dataplugin", "validate", cwd=plugin_directory)
    assert result.returncode == 0, result.stdout
    assert "Valid Manifest: True" in result.stdout


def test_validate_json_manifest(tmp_path: pathlib.Path):
    plugin_directory = tmp_path / "demo"
    plugin_directory.mkdir()
    manifest = {
        "version": "0.3",
        "dumper": {"data_url": "https://example.com/data.tsv"},
        "uploader": {"parser": "parser:load_data"},
    }
    plugin_directory.joinpath("manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    result = run_cli("dataplugin", "validate", cwd=plugin_directory)
    assert result.returncode == 0, result.stdout
    assert "Valid Manifest: True" in result.stdout


def test_validate_missing_manifest(tmp_path: pathlib.Path):
    tmp_path.joinpath("demo").mkdir()

    result = run_cli("dataplugin", "validate", "--name", "demo", cwd=tmp_path)
    assert result.returncode == 1, result.stdout
    assert "No manifest.json or manifest.yaml found" in result.stdout


def test_single_uploader_workflow(tmp_path: pathlib.Path, data_url: str):
    plugin_directory = create_plugin(tmp_path)
    set_data_url(plugin_directory, data_url)
    plugin_directory.joinpath("parser.py").write_text(PARSER, encoding="utf-8")

    for command in ("dump", "upload"):
        result = run_cli("dataplugin", command, cwd=plugin_directory)
        assert result.returncode == 0, result.stdout

    # --sub-source-name isn't needed for a plugin with a single uploader
    result = run_cli("dataplugin", "inspect", cwd=plugin_directory)
    assert result.returncode == 0, result.stdout
    assert "symbol" in result.stdout

    result = run_cli("dataplugin", "inspect", "--sub-source-name", "unknown", cwd=plugin_directory)
    assert result.returncode == 2, result.stdout
    assert 'Unknown sub source "unknown" for demo data-plugin' in result.stdout


@pytest.mark.parametrize(
    "valid_sources, sub_source_name, expected_sub_source_name",
    [
        (["demo"], None, "demo"),
        (["demo"], "", "demo"),
        (["demo"], "demo", "demo"),
        (["data1", "data2"], "data2", "data2"),
    ],
)
def test_resolve_sub_source_name(valid_sources: list, sub_source_name: str, expected_sub_source_name: str):
    assert _resolve_sub_source_name("demo", valid_sources, sub_source_name, "inspect") == expected_sub_source_name


@pytest.mark.parametrize(
    "valid_sources, sub_source_name, error_message",
    [
        (["data1", "data2"], None, "Multiple uploaders exist for demo data-plugin"),
        (["data1", "data2"], "", "Multiple uploaders exist for demo data-plugin"),
        (["demo"], "unknown", 'Unknown sub source "unknown" for demo data-plugin'),
    ],
)
def test_resolve_sub_source_name_error(valid_sources: list, sub_source_name: str, error_message: str, caplog):
    with pytest.raises(typer.Exit) as exit_info:
        _resolve_sub_source_name("demo", valid_sources, sub_source_name, "inspect")
    assert exit_info.value.exit_code == 2
    assert error_message in caplog.text
    assert "`biothings-cli dataplugin inspect --name demo --sub-source-name" in caplog.text


def test_missing_plugin_name_message():
    message = str(MissingPluginName("/data/plugins"))
    assert "--name (-n)" in message
    assert "--plugin-name" not in message
