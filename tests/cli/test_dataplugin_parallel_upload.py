"""
Tests "biothings-cli dataplugin upload --parallel": the uploaders with a parallelizer in their plugin's
manifest load their data in parallel jobs, into the CLI's database

The commands run in a subprocess from a temporary directory, the same way a user runs them.
The CLI loads its configuration from the working directory into sys.modules["biothings.config"],
which would replace the test configuration if the commands ran in the test process
"""

import functools
import http.server
import os
import pathlib
import sqlite3
import subprocess
import sys
import threading

import pytest
import yaml

PARSER = '''
import csv
import os


def load_data(path):
    """A data file (in parallel jobs, see parallelizer.py), or the data folder"""
    paths = [path] if os.path.isfile(path) else [os.path.join(path, name) for name in sorted(os.listdir(path))]
    for file_path in paths:
        with open(file_path, encoding="utf-8") as handle:
            for row in csv.DictReader(handle, delimiter="\\t"):
                yield {"_id": row["id"], "symbol": row["symbol"]}
'''

PARALLELIZER = '''
def one_job_per_file(self):
    """One upload job per data file (copied into the uploader's class: it imports what it needs)"""
    import os

    return [(os.path.join(self.data_folder, name),) for name in sorted(os.listdir(self.data_folder))]
'''


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


@pytest.fixture(scope="module")
def data_urls(tmp_path_factory) -> list:
    """
    Serves 3 small TSV files over HTTP for the data plugin dumper, 10 genes each
    """
    data_directory = tmp_path_factory.mktemp("parallel_upload_data")
    for part in (1, 2, 3):
        rows = "".join(f"G{part}_{number}\tSYMBOL{number}\n" for number in range(10))
        data_directory.joinpath(f"genes{part}.tsv").write_text("id\tsymbol\n" + rows, encoding="utf-8")
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(data_directory))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield [f"http://127.0.0.1:{server.server_port}/genes{part}.tsv" for part in (1, 2, 3)]
    server.shutdown()
    server.server_close()


def dumped_plugin(working_directory: pathlib.Path, data_urls: list, parallelizer: bool = True) -> pathlib.Path:
    """
    Creates the "genes" data plugin from the template, with or without a parallelizer, and dumps its data
    """
    arguments = ["dataplugin", "create", "--name", "genes"] + (["--parallelizer"] if parallelizer else [])
    result = run_cli(*arguments, cwd=working_directory)
    assert result.returncode == 0, result.stdout
    plugin_directory = working_directory / "genes"
    manifest_file = plugin_directory / "manifest.yaml"
    manifest = yaml.safe_load(manifest_file.read_text(encoding="utf-8"))
    # (values the template may leave empty, which the manifest's validation refuses)
    manifest["display_name"] = "Genes"
    manifest["biothing_type"] = "gene"
    manifest["__metadata__"]["description"] = "Genes"
    manifest["uploader"]["parser_kwargs"] = {}
    manifest["dumper"].pop("schedule", None)
    manifest["dumper"]["data_url"] = data_urls
    manifest["dumper"]["uncompress"] = False
    if parallelizer:
        manifest["uploader"]["parallelizer"] = "parallelizer:one_job_per_file"
        plugin_directory.joinpath("parallelizer.py").write_text(PARALLELIZER, encoding="utf-8")
    manifest_file.write_text(yaml.safe_dump(manifest), encoding="utf-8")
    plugin_directory.joinpath("parser.py").write_text(PARSER, encoding="utf-8")
    result = run_cli("dataplugin", "dump", cwd=plugin_directory)
    assert result.returncode == 0, result.stdout
    return plugin_directory


def uploaded_documents(plugin_directory: pathlib.Path) -> int:
    """
    Number of documents uploaded in the CLI's database
    """
    with sqlite3.connect(plugin_directory / ".biothings_hub" / "data_src_database") as connection:
        return connection.execute("SELECT count(*) FROM genes").fetchone()[0]


def test_parallel_upload(tmp_path, data_urls):
    plugin_directory = dumped_plugin(tmp_path, data_urls)
    result = run_cli("dataplugin", "upload", "--parallel", cwd=plugin_directory)
    assert result.returncode == 0, result.stdout
    assert uploaded_documents(plugin_directory) == 30  # one job per data file


def test_parallel_upload_without_parallelizer(tmp_path, data_urls):
    plugin_directory = dumped_plugin(tmp_path, data_urls, parallelizer=False)
    result = run_cli("dataplugin", "upload", "--parallel", cwd=plugin_directory)
    assert result.returncode == 0, result.stdout
    assert "No parallelizer for 'genes' in the plugin's manifest" in result.stdout
    assert uploaded_documents(plugin_directory) == 30
