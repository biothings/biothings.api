from biothings.hub import HubServer


def test_hubserver_construction():
    """
    Test construction of the HubServer object and ensures that
    the defaults are appropriately set
    """
    test_sources = ["fakesource0", "fakesource1"]
    server_name = "TestHubServer"

    test_hubserver = HubServer(
        source_list=test_sources,
        features=None,
        name=server_name,
        managers_custom_args=None,
        api_config=None,
        reloader_config=None,
        dataupload_config=None,
        websocket_config=None,
        autohub_config=None,
    )
    assert test_hubserver.source_list == test_sources
    assert test_hubserver.features == test_hubserver.DEFAULT_FEATURES
    assert test_hubserver.name == server_name

    assert test_hubserver.api_config == test_hubserver.DEFAULT_API_CONFIG
    assert test_hubserver.reloader_config == test_hubserver.DEFAULT_RELOADER_CONFIG
    assert test_hubserver.dataupload_config == test_hubserver.DEFAULT_DATAUPLOAD_CONFIG
    assert test_hubserver.websocket_config == test_hubserver.DEFAULT_WEBSOCKET_CONFIG
    assert test_hubserver.autohub_config == test_hubserver.DEFAULT_AUTOHUB_CONFIG
