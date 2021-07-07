import pytest
import os
import importlib

# What's changed
#
# 1. description in a superseding module will not override
# the previous one anymore, instead, it is added to the
# end of the previous paragraph.
#
# 2. no longer allowing \n character in descriptions
# because many times the \n is inserted because of
# python module's line limit only.


@pytest.fixture(scope="module")
def config():
    os.environ["HUB_CONFIG"] = "conf_deep"
    from biothings import hub
    importlib.reload(hub)
    config = hub.config
    config.reset()
    return config.show()["scope"]["config"]


def test_01_override_value(config):
    assert config["D_D"]["value"] == "d"  # new value
    assert config["D_D"]["desc"] == "descD_D"  # but description hasn't changed, taken from base
    assert config["D_D"]["section"] == "section beta"  # same for section, from base


def test_02_override_desc(config):
    assert config["E"]["value"] == "heu"  # new value
    assert config["E"]["desc"] == "descE. additional description"  # new description
    assert config["E"]["section"] == "section gamma"  # same for section


def test_03_override_desc_of_readonly(config):
    assert config["READ_ONLY"]["value"] == "written in titanium"  # new value
    assert "additional desc of read-only" in config["READ_ONLY"]["desc"]  # new description
    assert config["READ_ONLY"]["readonly"]  # still read-only, from base


def test_04_only_in_base(config):
    assert config["G"]["value"] == "G"


def test_05_add_readonly(config):
    assert config["F"]["value"] == "Forged"
    assert config["F"]["readonly"]
    assert config["F"]["desc"] == "descF. back to beta section."
    assert config["F"]["section"] == "section beta"
