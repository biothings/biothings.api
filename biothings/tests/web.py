"""
    Biothings Test Helpers

    There are two types of test classes that provide utilities to
    three types of test cases, developed in the standalone apps.

    The two types of test classes are:
        BiothingsWebTest, which targets a running web server.
        BiothingsWebAppTest, which targets a web server config file.

    To further illustrate, for any biothings web applications, it
    typically conforms to the following architectures:

    Layer 3: A web server that implements the behaviors defined below.
    Layer 2: A config file that defines how to serve data from ES.
    Layer 1: An Elasticsearch server with data.

    And for the two types of test classes, to explain their differences
    in the context of the layered design described above:
        BiothingsWebTest targets an existing Layer 3 endpoint.
        BiothingsWebAppTest targets layer 2 and runs its own layer 3.
        Note no utility is provided to directly talk to layer 1.

    The above discussed the python structures provided as programming
    utilities, on the other hand, there are three types of use cases,
    or testing objectives:
        L3 Data test, which is aimed to test the data integrity of an API.
            It subclasses BiothingsWebTest and ensures all layers working.
            The data has to reside in elasticsearch already.
        L3 Feature test, which is aimed to test the API implementation.
            It makes sure the settings in config file is reflected.
            These tests work on production data and require constant
            updates to keep the test cases in sync with the actual data.
            These test cases subclass BiothingsWebTest as well and asl
            require existing production data in elasticsearch.
        L2 Feature test, doing basically the same things as above but uses
            a small set of data that it ingests into elasticsearch.
            This is a lightweight test for development and automated testings
            for each new commit. It comes with data it will ingest in ES
            and does not require any existing data setup to run.

    To illustrate the differences in a chart:
    +--------------+---------------------+-----------------+-------------+---------------------------+
    | Objectives   | Class               | Test Target     | ES Has Data | Automated Testing Trigger |
    +--------------+---------------------+-----------------+-------------+---------------------------+
    | L3 Data Test | BiothingsWebTest    | A Running API   | Yes         | Data Release              |
    +--------------+---------------------+-----------------+-------------+---------------------------+
    | L3 Feature T.| BiothingsWebTest    | A Running API   | Yes         | Data Release & New Commit |
    +--------------+---------------------+-----------------+-------------+---------------------------+
    | L2 Feature T.| BiothingsWebAppTest | A config module | No*         | New Commit                |
    +--------------+---------------------+-----------------+-------------+---------------------------+
    * For L2 Feature Test, data is defined in the test cases and will be automatically ingested into
      Elasticsearch at the start of the testing and get deleted after testing finishes. The other
      two types of testing require existing production data on the corresponding ES servers.

    In development, it is certainly possible for a particular test case
    to fall under multiple test types, then the developer can use proper
    inheritance structures to avoid repeating the specific test case.

    In terms of naming conventions, sometimes the L3 tests are grouped
    together and called remote tests, as they mostly target remote servers.
    And the L2 tests are called local tests, as they starts a local server.

    L3 Envs:

    TEST_SCHEME
    TEST_PREFIX
    TEST_HOST
    TEST_CONF

    L2 Envs:

    TEST_KEEPDATA
    < Config Module Override >

"""
import glob
import inspect
import json
import os
import sys
from functools import partial
from typing import Optional, Union

import pytest
import requests
import urllib3
from tornado.ioloop import IOLoop
from tornado.testing import AsyncHTTPTestCase

import biothings
from biothings.utils.common import traverse
from biothings.web.launcher import BiothingsAPI
from biothings.web.settings import configs


class BiothingsWebTestBase:
    # relative path parsing configuration
    scheme = "http"
    prefix = "v1"
    host = ""

    def tearDown(self):
        # By default, a new "IOLoop" is constructed for each test and is available as "self.io_loop".
        # To maintain the desired test behavior, it is necessary to clear the current IOLoop at the end
        # of each test function. See class AsyncTestCase in the reference.
        # Reference: https://www.tornadoweb.org/en/branch6.4/_modules/tornado/testing.html
        self.io_loop.clear_current()

    def request(self, path, method="GET", expect=200, **kwargs):
        """
        Use requests library to make an HTTP request.
        Ensure path is translated to an absolute path.
        Conveniently check if status code is as expected.
        """
        url = self.get_url(path)
        res = requests.request(method, url, **kwargs)
        assert res.status_code == expect, res.text

        return res

    def get_url(self, path):
        """
        Try best effort to get a full url to make a request.
        Return an absolute url when class var 'host' is defined.
        If not, return a path relative to the host root.
        """
        # already an absolute path
        if path.lower().startswith(("http://", "https://")):
            return path

        # path standardization
        if not path.startswith("/"):
            if self.prefix:  # append prefix
                path = "/".join((self.prefix, path))
            path = "/" + path

        # host standardization
        if self.host:
            path = f"{self.scheme}://{self.host}{path}"

        return path

    def query(self, method="GET", endpoint="query", hits=True, data=None, json=None, **kwargs):
        """
        Make a query and assert positive hits by default.
        Assert zero hit when hits is set to False.
        """

        if method == "GET":
            res = self.request(endpoint, params=kwargs, data=data, json=json).json()

            assert bool(res.get("hits")) == hits
            return res

        if method == "POST":
            res = self.request(endpoint, method=method, params=kwargs, data=data, json=json).json()

            for item in res:  # list
                if "_id" not in item:
                    _hits = False
                    break
            else:
                _hits = bool(res)
            assert _hits is hits
            return res

        raise ValueError("Invalid Request Method.")

    @staticmethod
    def msgpack_ok(packed_bytes):
        """Load msgpack into a dict"""
        try:
            import msgpack
        except ImportError:
            pytest.skip("Msgpack is not installed.")
        try:
            dic = msgpack.unpackb(packed_bytes)
        except BaseException:  # pylint: disable=bare-except
            # assert False, "Not a valid Msgpack binary."
            raise ValueError("Not a valid Msgpack binary.")
        return dic

    @staticmethod
    def value_in_result(value, result: Union[dict, list], key: str, case_insensitive: bool = False) -> bool:
        """
        Check if value is in result at specific key

        Elasticsearch does not care if a field has one or more values (arrays),
        so you may get a search with multiple values in one field.
        You were expecting a result of type T but now you have a List[T] which
        is bad.
        In testing, usually any one element in the list eq. to the value you're
        looking for, you don't really care which.
        This helper function checks if the value is at a key, regardless
        of the details of nesting, so you can just do this:
            assert self.value_in_result(value, result, 'where.it.should.be')

        Caveats:
        case_insensitive only calls .lower() and does not care about locale/
        unicode/anything

        Args:
            value: value to look for
            result: dict or list of input, most likely from the APIs
            key: dot delimited key notation
            case_insensitive: for str comparisons, invoke .lower() first
        Returns:
            boolean indicating whether the value is found at the key
        Raises:
            TypeError: when case_insensitive set to true on unsupported types
        """
        res_at_key = []
        if case_insensitive:
            try:
                value = value.lower()
            except Exception:
                raise TypeError("failed to invoke method .lower()")
        for k, v in traverse(result, leaf_node=True):
            if k == key:
                if case_insensitive:
                    try:
                        v = v.lower()
                    except Exception:
                        raise TypeError("failed to invoke method .lower()")
                res_at_key.append(v)
        return value in res_at_key


class BiothingsWebTest(BiothingsWebTestBase):
    """ """

    @classmethod
    def setup_class(cls):
        """this is the setup method when pytest run tests from this class"""
        cls.scheme = os.getenv("TEST_SCHEME", cls.scheme)
        cls.prefix = os.getenv("TEST_PREFIX", cls.prefix).strip("/")
        cls.host = os.getenv("TEST_HOST", cls.host).strip("/")
        base_url = f"{cls.scheme}://{cls.host}/{cls.prefix}" if cls.host else f"/{cls.prefix}"
        msg = f"\n\tTest URL: {base_url}"
        msg += f"\n\tBioThings SDK Version: {biothings.__version__}"
        msg += f"\n\tBioThings SDK path: {biothings.__file__}\n"
        # this stderr output will be suppressed by pytest
        # but will be shown when --capture=no or -s is passed,
        # allowing us to see the test url when running tests
        sys.__stderr__.write(msg)


class BiothingsWebAppTest(BiothingsWebTestBase, AsyncHTTPTestCase):
    """
    Starts the tornado application to run tests locally.
    Need a config.py under the test class folder.
    """

    TEST_DATA_DIR_NAME: Optional[str] = None  # set sub-dir name

    @pytest.fixture(scope="class", autouse=True)
    def _setup_elasticsearch(self):
        # Author: Zhongchao Qian
        # https://github.com/biothings/biothings.api/pull/135

        if not self.TEST_DATA_DIR_NAME:
            yield  # do no setup and yield control to pytest
            return

        s = requests.Session()
        s.mount(
            "http://", adapter=requests.adapters.HTTPAdapter(max_retries=urllib3.Retry(total=5, backoff_factor=3.0))
        )  # values seem reasonable
        es_host = "http://" + self.config.ES_HOST

        server_info = s.get(es_host).json()
        version_info = tuple(int(v) for v in server_info["version"]["number"].split("."))
        if version_info[0] < 6 or version_info[0] == 6 and version_info[1] < 8:
            pytest.exit("Tests need to be running on ES6.8+")

        indices = []  # for cleanup later
        data_dir_path = os.path.dirname(inspect.getfile(type(self)))
        data_dir_path = os.path.join(data_dir_path, "test_data")
        data_dir_path = os.path.join(data_dir_path, self.TEST_DATA_DIR_NAME)
        glob_json_pattern = os.path.join(data_dir_path, "*.json")
        # wrap around in try-finally so the index is guaranteed to be
        err_flag = False
        try:
            # TODO No match seems to cause illegible error
            for index_mapping_path in glob.glob(glob_json_pattern):
                index_name = os.path.basename(index_mapping_path)
                index_name = os.path.splitext(index_name)[0]
                indices.append(index_name)
                r = s.head(f"{es_host}/{index_name}")
                if r.status_code != 404:
                    if os.environ.get("TEST_KEEPDATA"):
                        continue
                    raise RuntimeError(f"{index_name} already exists!")
                with open(index_mapping_path, "r") as f:
                    mapping = json.load(f)
                data_path = os.path.join(data_dir_path, index_name + ".ndjson")
                with open(data_path, "r") as f:
                    bulk_data = f.read()
                if version_info[0] == 6:
                    r = s.put(f"{es_host}/{index_name}", json=mapping, params={"include_type_name": "false"})
                elif version_info[0] > 6:
                    r = s.put(f"{es_host}/{index_name}", json=mapping)
                else:
                    raise RuntimeError("This shouldn't have happened")
                r.raise_for_status()
                if version_info[0] < 8:
                    r = s.post(
                        f"{es_host}/{index_name}/_doc/_bulk",
                        data=bulk_data,
                        headers={"Content-type": "application/x-ndjson"},
                    )
                elif version_info[0] >= 8:
                    r = s.post(
                        f"{es_host}/{index_name}/_bulk",
                        data=bulk_data,
                        headers={"Content-type": "application/x-ndjson"},
                    )
                else:
                    raise RuntimeError("This shouldn't have happened")
                r.raise_for_status()
                s.post(f"{es_host}/{index_name}/_refresh")
            yield
        except Exception as e:
            err_msg = str(e)
            err_flag = True
        finally:
            if not os.environ.get("TEST_KEEPDATA"):
                for index_name in indices:
                    s.delete(f"{es_host}/{index_name}")
            if err_flag:
                pytest.exit("Error setting up ES for tests:", err_msg)

    @property
    def config(self):
        if not hasattr(self, "_config"):
            conf = os.getenv("TEST_CONF", "config.py")
            base = os.path.dirname(inspect.getfile(type(self)))
            file = os.path.join(base, conf)
            self._config = configs.load(file)
        return self._config

    # override
    def get_new_ioloop(self):
        return IOLoop.current()

    # override
    def get_app(self):
        prefix = self.config.APP_PREFIX
        version = self.config.APP_VERSION
        self.prefix = f"{prefix}/{version}".strip("/")
        return BiothingsAPI.get_app(self.config)

    # override
    def request(self, path, method="GET", expect=200, **kwargs):
        url = self.get_url(path)

        func = partial(requests.request, method, url, **kwargs)
        res = self.io_loop.run_sync(lambda: self.io_loop.run_in_executor(None, func))

        assert res.status_code == expect, res.text
        return res

    # override
    def get_url(self, path):
        path = BiothingsWebTestBase.get_url(self, path)
        return AsyncHTTPTestCase.get_url(self, path)


# Compatibility
BiothingsTestCase = BiothingsWebAppTest
BiothingsDataTest = BiothingsWebTest
