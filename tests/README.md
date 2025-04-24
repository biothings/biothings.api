## Biothings Test Infrastructure 


### Running all tests locally

To run the entire test suite:

#### [0] Setup local instances for Elasticsearch and MongoDB

Because the hub backend expects a running Elasticsearch instance,
ensure that you have an Elasticsearch server running on your local machine.

This is specified in the fixture via `~/tests/conftest.py`. It by default expects
to find the server running on `localhost:9200`

```python
"HUB_DB_BACKEND": {
    "module": "biothings.utils.es",
    "host": "http://localhost:9200",
}
```

It also expects a MongoDB instance for certain connections running locally at `localhost:27017`

To create an instance of Elasticsearch locally you can run the following shell command

###### Elasticsearch setup command

```shell
docker run  \
-d  \
--name elasticsearch \
-p 127.0.0.1:9200:9200 \
-e discovery.type=single-node \
-e xpack.security.enabled=false \
-e xpack.security.http.ssl.enabled=false \
-e xpack.security.transport.ssl.enabled=false \
elasticsearch:8.17.1
```

To create an instance of MongoDB locally you can run the following shell command

###### MongoDB setup command

```shell
docker run  \
-d  \
-p 127.0.0.1:27017:27017 \
--name mongo \
mongo:7.0.11
```

To setup both via docker compose, you can use the following configuration


###### docker compose setup command `docker compose up -f <filename>`

```DOCKER
version: '3.8'

services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.17.1
    container_name: elasticsearch
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - ES_JAVA_OPTS=-Xms1g -Xmx1g
    ports:
      - 9200:9200
    volumes:
      - esdata:/usr/share/elasticsearch/data

  mongodb:
    image: mongo:7.0.11
    container_name: mongodb
    ports:
      - 27017:27017
    volumes:
      - mongodata:/data/db

volumes:
  esdata:
  mongodata:
```

#### [1] Test collection

We leverage `pytest` as our main test runner. To ensure that you have the expected number of
collected tests, you can run the following command to collect the entire test suite prior to
running:

```shell
python3 -m pytest ./tests --collect-only
```

This should provide a large dump of all tests collected in the `tests` directory. You can use this
to ensure the expected number of tests are collected / discovered by pytest

```shell
======================================================================== 430 tests collected in 1.51s ========================================================================
```

Similarly to ensure fixture loading, you can use the following to ensure that a test has the
appropriate fixture setup before running

```shell
python3 -m pytest tests/hub/config/test_configuration_wrapper.py --setup-plan
```

```shell
============================================================================ test session starts =============================================================================
platform linux -- Python 3.12.10, pytest-8.3.5, pluggy-1.5.0 -- /home/workspace/biothings/.direnv/python-3.12.10/bin/python3
cachedir: .pytest_cache
rootdir: /home/workspace/biothings/biothings.api
configfile: pyproject.toml
plugins: asyncio-0.26.0, mock-3.14.0, anyio-4.9.0
asyncio: mode=Mode.STRICT, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 23 items

tests/hub/config/test_configuration_wrapper.py::test_00_structure
SETUP    S event_loop_policy
SETUP    S root_configuration
    SETUP    M configuration_data_storage
    SETUP    M base_config (fixtures used: configuration_data_storage)
        tests/hub/config/test_configuration_wrapper.py::test_00_structure (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_01_top_param
        tests/hub/config/test_configuration_wrapper.py::test_01_top_param (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_02_section_init
        tests/hub/config/test_configuration_wrapper.py::test_02_section_init (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_03_section_continue
        tests/hub/config/test_configuration_wrapper.py::test_03_section_continue (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_04_section_new
        tests/hub/config/test_configuration_wrapper.py::test_04_section_new (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_05_section_another
        tests/hub/config/test_configuration_wrapper.py::test_05_section_another (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_06_section_redefine
        tests/hub/config/test_configuration_wrapper.py::test_06_section_redefine (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_07_section_reset
        tests/hub/config/test_configuration_wrapper.py::test_07_section_reset (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_08_invisible
        tests/hub/config/test_configuration_wrapper.py::test_08_invisible (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_09_value_hidden
        tests/hub/config/test_configuration_wrapper.py::test_09_value_hidden (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_10_read_only
        tests/hub/config/test_configuration_wrapper.py::test_10_read_only (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_11_read_only_value_hidden
        tests/hub/config/test_configuration_wrapper.py::test_11_read_only_value_hidden (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_12_invisible_has_precedence
        tests/hub/config/test_configuration_wrapper.py::test_12_invisible_has_precedence (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_13_dynamic_param_readonly
        tests/hub/config/test_configuration_wrapper.py::test_13_dynamic_param_readonly (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_16_edit
        tests/hub/config/test_configuration_wrapper.py::test_16_edit (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_14_readonly_not_editable
        tests/hub/config/test_configuration_wrapper.py::test_14_readonly_not_editable (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_15_invisible_not_editable
        tests/hub/config/test_configuration_wrapper.py::test_15_invisible_not_editable (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_17_special_param_not_editable
        tests/hub/config/test_configuration_wrapper.py::test_17_special_param_not_editable (fixtures used: base_config, configuration_data_storage, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_01_override_value
    SETUP    M deep_config (fixtures used: configuration_data_storage)
        tests/hub/config/test_configuration_wrapper.py::test_01_override_value (fixtures used: configuration_data_storage, deep_config, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_02_override_desc
        tests/hub/config/test_configuration_wrapper.py::test_02_override_desc (fixtures used: configuration_data_storage, deep_config, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_03_override_desc_of_readonly
        tests/hub/config/test_configuration_wrapper.py::test_03_override_desc_of_readonly (fixtures used: configuration_data_storage, deep_config, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_04_only_in_base
        tests/hub/config/test_configuration_wrapper.py::test_04_only_in_base (fixtures used: configuration_data_storage, deep_config, event_loop_policy, root_configuration)
tests/hub/config/test_configuration_wrapper.py::test_05_add_readonly
        tests/hub/config/test_configuration_wrapper.py::test_05_add_readonly (fixtures used: configuration_data_storage, deep_config, event_loop_policy, root_configuration)
    TEARDOWN M deep_config
    TEARDOWN M base_config
    TEARDOWN M configuration_data_storage
TEARDOWN S root_configuration
TEARDOWN S event_loop_policy

============================================================================= slowest durations ==============================================================================
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_00_structure
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_05_add_readonly
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_04_section_new
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_01_override_value
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_00_structure
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_01_top_param
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_09_value_hidden
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_02_section_init
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_03_section_continue
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_11_read_only_value_hidden
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_05_add_readonly
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_17_special_param_not_editable
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_12_invisible_has_precedence
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_03_override_desc_of_readonly
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_15_invisible_not_editable
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_05_section_another
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_06_section_redefine
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_13_dynamic_param_readonly
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_14_readonly_not_editable
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_16_edit
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_02_override_desc
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_08_invisible
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_04_only_in_base
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_01_top_param
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_07_section_reset
0.00s setup    tests/hub/config/test_configuration_wrapper.py::test_10_read_only
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_13_dynamic_param_readonly
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_03_section_continue
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_11_read_only_value_hidden
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_02_section_init
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_05_section_another
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_04_section_new
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_10_read_only
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_02_override_desc
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_04_only_in_base
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_07_section_reset
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_15_invisible_not_editable
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_01_override_value
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_12_invisible_has_precedence
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_17_special_param_not_editable
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_14_readonly_not_editable
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_03_override_desc_of_readonly
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_16_edit
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_09_value_hidden
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_06_section_redefine
0.00s teardown tests/hub/config/test_configuration_wrapper.py::test_08_invisible
============================================================================= 1 warning in 0.07s ============================================================================
```

#### [2] Running the test suite


```shell
python3 -m pytest ./tests  # Run the entire test suite for the biothings.api
python3 -m pytest ./tests/hub  # Run the test suite only for the biothings.api hub
python3 -m pytest ./tests/hub/config  # Run the a specific module from the test suite by directory
python3 -m pytest ./tests/hub/config/test_configuration_wrapper.py::test_00_structure # Run a specific test within a test file
```

##### keyword search

###### Command 
```shell
python3 -m pytest -k "config" --collect-only
```

###### Output 

```shell
tests/hub/config on  fix-tests [$!?] via 🐍 v3.12.10 (python-3.12.10) took 3s
❯ python3 -m pytest -k "config" --collect-only
============================================================================ test session starts =============================================================================
platform linux -- Python 3.12.10, pytest-8.3.5, pluggy-1.5.0 -- /home/workspace/biothings/.direnv/python-3.12.10/bin/python3
cachedir: .pytest_cache
rootdir: /home/workspace/biothings/biothings.api
configfile: pyproject.toml
plugins: asyncio-0.26.0, mock-3.14.0, anyio-4.9.0
asyncio: mode=Mode.STRICT, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 23 items

<Dir biothings.api>
  <Package tests>
    <Dir hub>
      <Dir config>
        <Module test_configuration_wrapper.py>
          <Function test_00_structure>
          <Function test_01_top_param>
          <Function test_02_section_init>
          <Function test_03_section_continue>
          <Function test_04_section_new>
          <Function test_05_section_another>
          <Function test_06_section_redefine>
          <Function test_07_section_reset>
          <Function test_08_invisible>
          <Function test_09_value_hidden>
          <Function test_10_read_only>
          <Function test_11_read_only_value_hidden>
          <Function test_12_invisible_has_precedence>
          <Function test_13_dynamic_param_readonly>
          <Function test_16_edit>
          <Function test_14_readonly_not_editable>
          <Function test_15_invisible_not_editable>
          <Function test_17_special_param_not_editable>
          <Function test_01_override_value>
          <Function test_02_override_desc>
          <Function test_03_override_desc_of_readonly>
          <Function test_04_only_in_base>
          <Function test_05_add_readonly>
======================================================================== 23 tests collected in 0.04s ========================================================================
```

##### marker search

To view all markers available under the test suite run `python3 -m pytest --markers`

To select a specify marker use the `-m` flag

```shell
biothings.api on  fix-tests [$!?] via 🐍 v3.12.10 (python-3.12.10)
❯ python3 -m pytest -m ReleaseNoteSrcBuildReader
============================================================================ test session starts =============================================================================
platform linux -- Python 3.12.10, pytest-8.3.5, pluggy-1.5.0 -- /home/workspace/biothings/.direnv/python-3.12.10/bin/python3
cachedir: .pytest_cache
rootdir: /home/workspace/biothings/biothings.api
configfile: pyproject.toml
testpaths: tests
plugins: asyncio-0.26.0, mock-3.14.0, anyio-4.9.0
asyncio: mode=Mode.STRICT, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collecting ...
---------------------------------------------------------------------------- live log collection -----------------------------------------------------------------------------
collected 430 items / 428 deselected / 2 selected

tests/hub/datarelease/test_releasenote.py::test_read_cold_src_build
SETUP    S event_loop_policy
SETUP    S root_configuration
        SETUP    F cold_src_build_doc
        tests/hub/datarelease/test_releasenote.py::test_read_cold_src_build (fixtures used: cold_src_build_doc, event_loop_policy, root_configuration)PASSED
        TEARDOWN F cold_src_build_doc
tests/hub/datarelease/test_releasenote.py::test_read_hot_src_build
        SETUP    F hot_src_build_doc
        SETUP    F cold_src_build_doc
        tests/hub/datarelease/test_releasenote.py::test_read_hot_src_build (fixtures used: cold_src_build_doc, event_loop_policy, hot_src_build_doc, root_configuration)PASSED
        TEARDOWN F cold_src_build_doc
        TEARDOWN F hot_src_build_doc
TEARDOWN S root_configuration
TEARDOWN S event_loop_policy

=================================================================================== PASSES ===================================================================================
============================================================================= slowest durations ==============================================================================
0.00s setup    tests/hub/datarelease/test_releasenote.py::test_read_cold_src_build
0.00s call     tests/hub/datarelease/test_releasenote.py::test_read_cold_src_build
0.00s setup    tests/hub/datarelease/test_releasenote.py::test_read_hot_src_build
0.00s teardown tests/hub/datarelease/test_releasenote.py::test_read_hot_src_build
0.00s teardown tests/hub/datarelease/test_releasenote.py::test_read_cold_src_build
0.00s call     tests/hub/datarelease/test_releasenote.py::test_read_hot_src_build
========================================================================== short test summary info ===========================================================================
PASSED tests/hub/datarelease/test_releasenote.py::test_read_cold_src_build
PASSED tests/hub/datarelease/test_releasenote.py::test_read_hot_src_build
================================================================ 2 passed, 428 deselected, 1 warning in 1.03s ===============================================================



