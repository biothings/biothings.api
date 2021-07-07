import json
import os
import pytest
import importlib

@pytest.fixture(scope="module")
def config():
    os.environ["HUB_CONFIG"] = "conf_base"
    from biothings import hub
    importlib.reload(hub)
    config = hub.config
    config.reset()
    config.DYNPARAM = "runtime"
    return config


def test_00_structure(config):
    conf = config.show()
    assert sorted(list(conf.keys())) == ["_dirty", "allow_edits", "scope"]
    assert list(conf["scope"].keys()) == ["config", "class"]
    assert not conf["scope"]["class"]


def test_01_top_param(config):
    config = config.show()["scope"]["config"]
    assert config["ONE"]["value"] == 1
    assert config["ONE"]["value"] == config["ONE"]["default"]  # not superseded
    assert config["ONE"]["section"] is None
    assert config["ONE"]["desc"] == "descONE"

def test_02_section_init(config):
    config = config.show()["scope"]["config"]
    assert config["B"]["value"] == "B"
    assert config["B"]["value"] == config["B"]["default"]  # not superseded
    assert config["B"]["section"] == "section alpha"
    assert config["B"]["desc"] is None  # no comment

def test_03_section_continue(config):
    config = config.show()["scope"]["config"]
    assert config["C"]["value"] == "C"
    assert config["C"]["value"] == config["C"]["default"]  # not superseded
    assert config["C"]["section"] == "section alpha"
    assert config["C"]["desc"] == "ends with space should be stripped descC"  # inline comment

def test_04_section_new(config):
    config = config.show()["scope"]["config"]
    # includes underscore in param
    assert config["D_D"]["value"] == "D"
    assert config["D_D"]["value"] == config["D_D"]["default"]
    assert config["D_D"]["section"] == "section beta"
    assert config["D_D"]["desc"] == "descD_D"

def test_05_section_another(config):
    config = config.show()["scope"]["config"]
    assert config["E"]["value"] == "E"
    assert config["E"]["value"] == config["E"]["default"]
    assert config["E"]["section"] == "section gamma"
    assert config["E"]["desc"] == "descE."

def test_06_section_redefine(config):
    config = config.show()["scope"]["config"]
    assert config["F"]["value"] == "F"
    assert config["F"]["value"] == config["F"]["default"]
    assert config["F"]["section"] == "section beta"
    assert config["F"]["desc"] == "descF. back to beta section."

def test_07_section_reset(config):
    config = config.show()["scope"]["config"]
    assert config["G"]["value"] == "G"
    assert config["G"]["value"] == config["G"]["default"]
    assert config["G"]["section"] is None
    assert config["G"]["desc"] == "reset section"

def test_08_invisible(config):
    config = config.show()["scope"]["config"]
    assert config.get("INVISIBLE") is None

def test_09_value_hidden(config):
    config = config.show()["scope"]["config"]
    assert config["PASSWORD"]["value"] == "********"
    assert config["PASSWORD"]["desc"] == "hide the value, not the param"

def test_10_read_only(config):
    config = config.show()["scope"]["config"]
    assert config["READ_ONLY"]["value"] == "written in stone"
    assert config["READ_ONLY"]["readonly"]
    assert config["READ_ONLY"]["desc"] == "it's readonly"

def test_11_read_only_value_hidden(config):
    config = config.show()["scope"]["config"]
    assert config["READ_ONLY_PASSWORD"]["value"] == "********"
    assert config["READ_ONLY_PASSWORD"]["readonly"]
    assert config["READ_ONLY_PASSWORD"]["desc"] == "it's read-only and value is hidden, not the param"

def test_12_invisible_has_precedence(config):
    config = config.show()["scope"]["config"]
    assert not config.get("INVISIBLE_READ_ONLY")

def test_13_dynamic_param_readonly(config):
    config = config.show()["scope"]["config"]
    assert config["DYNPARAM"]["value"] == "runtime"
    assert config["DYNPARAM"]["readonly"]

def test_16_edit(config):
    newval = "a new life for B"
    config.store_value_to_db("B", newval)
    assert config.modified
    # update config dict
    from biothings.utils.hub_db import get_hub_config
    d = get_hub_config().find_one({"_id": "B"})
    assert d["json"] == json.dumps(newval)
    assert config.show()["scope"]["config"]["B"]["value"] == newval

def test_14_readonly_not_editable(config):
    with pytest.raises(RuntimeError):
        config.store_value_to_db("READ_ONLY", "trying anyway")

def test_15_invisible_not_editable(config):
    with pytest.raises(RuntimeError):
        config.store_value_to_db("INVISIBLE", "trying anyway")

def test_17_special_param_not_editable(config):
    with pytest.raises(RuntimeError):
        config.store_value_to_db("CONFIG_READONLY", "trying anyway")
