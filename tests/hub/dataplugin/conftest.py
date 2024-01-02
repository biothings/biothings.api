"""
Fixtures for mocking different types of plugin types
"""
from pathlib import Path
import functools
import http
import json
import logging
import os
import random
import shutil
import threading

import pytest


logger = logging.getLogger(__name__)


@pytest.fixture()
def temporary_mock_data(tmp_path_factory):
    """
    Generates a subset of random binary files for populating the mock data hosting
    """
    set_file_size_bytes = [1024, 2048, 4096, 8192]

    temp_directory_name = "submarine"
    temp_directory = tmp_path_factory.mktemp(temp_directory_name)
    num_data_files = random.randint(3, 7)

    for file_index, file_size_bytes in enumerate(random.choices(set_file_size_bytes, k=num_data_files)):
        random_binary_filename = f"dleaf{file_index}"
        random_binary_file = Path(temp_directory) / random_binary_filename
        with open(random_binary_file, "wb") as handle:
            handle.write(os.urandom(file_size_bytes))

    yield temp_directory
    shutil.rmtree(str(temp_directory))


@pytest.fixture()
def mock_data_hosting(temporary_mock_data):
    """
    Mocks a basic HTTP server pointed to a temporary directory
    with randomly generated data

    Custom server class reference:
    https://stackoverflow.com/questions/268629/how-to-stop-basehttpserver-serve-forever-in-a-basehttprequesthandler-subclass
    """

    class InterruptableHTTPServer(http.server.HTTPServer):
        def run(self):
            try:
                self.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                # Clean-up server (close socket, etc.)
                self.server_close()

    server_address = ""
    server_port = 5000
    server_coupling = (server_address, server_port)

    request_handler_callback = functools.partial(http.server.SimpleHTTPRequestHandler, directory=temporary_mock_data)
    http_server_instance = InterruptableHTTPServer(server_coupling, request_handler_callback)
    thread = threading.Thread(None, http_server_instance.run)
    thread.start()
    logger.info(f"Started server thread {thread} {http_server_instance} @ {http_server_instance.server_address}")
    yield (http_server_instance, thread)
    http_server_instance.shutdown()
    thread.join()
    logger.info(f"Shut down HTTP server instance {http_server_instance} @ {http_server_instance.server_address}")


@pytest.fixture()
def plugin(temporary_data_storage, mock_data_hosting, request):
    """
    Modified the plugin manifest to reflect the data generated
    during test setup

    - Updates the manifest file to modify the URL to point to the
    served data through our locally hosted HTTPServer
    """
    server_instance = mock_data_hosting[0]
    server_thread = mock_data_hosting[1]
    plugin_name = request.param

    plugin_directory = Path(temporary_data_storage) / "plugins"
    mock_plugin_directory = Path(plugin_directory) / plugin_name

    request_handler_keyword_mapping = server_instance.RequestHandlerClass.keywords
    data_hosting_directory = request_handler_keyword_mapping["directory"]
    server_address, server_port = server_instance.server_address

    manifest_file = mock_plugin_directory / "manifest.json"
    with open(manifest_file, "r", encoding="utf-8") as file_handle:
        manifest_content = json.load(file_handle)

    updated_data_url = [
        f"http://{server_address}:{server_port}/{data_file.name}"
        for data_file in Path(data_hosting_directory).glob("**/*")
    ]
    manifest_content["dumper"]["data_url"] = updated_data_url

    with open(manifest_file, "w", encoding="utf-8") as file_handle:
        json.dump(manifest_content, file_handle, indent=4)

    return mock_plugin_directory
