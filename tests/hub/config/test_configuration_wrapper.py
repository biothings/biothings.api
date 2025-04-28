import importlib
import json

import pytest


def test_00_structure(base_config):
    conf = base_config.show()
    assert sorted(list(conf.keys())) == ["_dirty", "allow_edits", "scope"]
    assert list(conf["scope"].keys()) == ["config", "class"]
    assert not conf["scope"]["class"]


def test_01_top_param(base_config):
    base_config = base_config.show()["scope"]["config"]
    assert base_config["ONE"]["value"] == 1
    assert base_config["ONE"]["value"] == base_config["ONE"]["default"]  # not superseded
    assert base_config["ONE"]["section"] is None
    assert base_config["ONE"]["desc"] is None


def test_02_section_init(base_config):
    base_config = base_config.show()["scope"]["config"]
    assert base_config["B"]["value"] == "B"
    assert base_config["B"]["value"] == base_config["B"]["default"]  # not superseded
    assert base_config["B"]["section"] is None
    assert base_config["B"]["desc"] is None  # no comment


def test_03_section_continue(base_config):
    base_config = base_config.show()["scope"]["config"]
    assert base_config["C"]["value"] == "C"
    assert base_config["C"]["value"] == base_config["C"]["default"]  # not superseded
    assert base_config["C"]["section"] is None
    assert base_config["C"]["desc"] == "ends with space should be stripped descC"  # inline comment


def test_04_section_new(base_config):
    base_config = base_config.show()["scope"]["config"]
    # includes underscore in param
    assert base_config["D_D"]["value"] == "D"
    assert base_config["D_D"]["value"] == base_config["D_D"]["default"]
    assert base_config["D_D"]["section"] is None
    assert base_config["D_D"]["desc"] is None


def test_05_section_another(base_config):
    base_config = base_config.show()["scope"]["config"]
    assert base_config["E"]["value"] == "E"
    assert base_config["E"]["value"] == base_config["E"]["default"]
    assert base_config["E"]["section"] is None
    assert base_config["E"]["desc"] is None


def test_06_section_redefine(base_config):
    base_config = base_config.show()["scope"]["config"]
    assert base_config["F"]["value"] == "F"
    assert base_config["F"]["value"] == base_config["F"]["default"]
    assert base_config["F"]["section"] is None
    assert base_config["F"]["desc"] is None


def test_07_section_reset(base_config):
    base_config = base_config.show()["scope"]["config"]
    assert base_config["G"]["value"] == "G"
    assert base_config["G"]["value"] == base_config["G"]["default"]
    assert base_config["G"]["section"] is None
    assert base_config["G"]["desc"] is None


def test_08_invisible(base_config):
    base_config = base_config.show()["scope"]["config"]
    assert base_config.get("INVISIBLE")


@pytest.mark.xfail(reason="There is no hidden variable in default_config")
def test_09_value_hidden(base_config):
    base_config = base_config.show()["scope"]["config"]
    assert base_config["PASSWORD"]["value"] == "********"
    assert base_config["PASSWORD"]["desc"] == "hide the value, not the param"


def test_10_read_only(base_config):
    base_config = base_config.show()["scope"]["config"]
    assert base_config["HUB_SSH_PORT"]["value"] == "123"
    assert base_config["HUB_SSH_PORT"]["readonly"]
    assert base_config["HUB_SSH_PORT"]["desc"] == "SSH port for hub console"


@pytest.mark.xfail(reason="There is no hidden variable in default_config")
def test_11_read_only_value_hidden(base_config):
    base_config = base_config.show()["scope"]["config"]
    assert base_config["READ_ONLY_PASSWORD"]["value"] == "********"
    assert base_config["READ_ONLY_PASSWORD"]["readonly"]
    assert base_config["READ_ONLY_PASSWORD"]["desc"] == "it's read-only and value is hidden, not the param"


@pytest.mark.xfail(reason="There is no INVISIBLE variable in default_config")
def test_12_invisible_has_precedence(base_config):
    base_config = base_config.show()["scope"]["config"]
    assert not base_config.get("INVISIBLE_READ_ONLY")


def test_13_dynamic_param_readonly(base_config):
    base_config.DYNPARAM = "runtime"
    base_config = base_config.show()["scope"]["config"]
    assert base_config["DYNPARAM"]["value"] == "runtime"
    assert base_config["DYNPARAM"]["readonly"]


@pytest.mark.xfail(reason="Requires hubdb setup for mutating configuration")
def test_16_edit(base_config):
    newval = "a new life for B"
    base_config.store_value_to_db("B", newval)
    assert base_config.modified

    # update config dict
    from biothings.utils.hub_db import get_hub_config

    d = get_hub_config().find_one({"_id": "B"})
    assert d["json"] == json.dumps(newval)
    assert base_config.show()["scope"]["config"]["B"]["value"] == newval


def test_14_readonly_not_editable(base_config):
    with pytest.raises(RuntimeError):
        base_config.store_value_to_db("HUB_SSH_PORT", "trying anyway")


@pytest.mark.xfail(reason="There is no INVISIBLE variable in default_config")
def test_15_invisible_not_editable(base_config):
    with pytest.raises(RuntimeError):
        base_config.store_value_to_db("INVISIBLE", "trying anyway")


def test_17_special_param_not_editable(base_config):
    with pytest.raises(RuntimeError):
        base_config.store_value_to_db("CONFIG_READONLY", "trying anyway")


# What's changed
#
# 1. description in a superseding module will not override
# the previous one anymore, instead, it is added to the
# end of the previous paragraph.
#
# 2. no longer allowing \n character in descriptions
# because many times the \n is inserted because of
# python module's line limit only.


def test_01_override_value(deep_config):
    deep_config = deep_config.show()["scope"]["config"]
    assert deep_config["RUN_DIR"]["value"] == "run_dir"  # new value
    assert (
        deep_config["RUN_DIR"]["desc"] == "where to store info about processes launched by the hub"
    )  # but description hasn't changed, taken from default_config
    assert deep_config["RUN_DIR"]["section"] == "3. Folders"  # same for section, from base


def test_02_override_desc(deep_config):
    deep_config = deep_config.show()["scope"]["config"]
    assert deep_config["E"]["value"] == "heu"  # new value
    assert deep_config["E"]["desc"] is None  # E not in default_config, skipping override metadata
    assert deep_config["E"]["section"] is None  # E not in default_config, skipping override metadata


@pytest.mark.xfail(reason="Description doesn't appear to parse correctly from config")
def test_03_override_desc_of_readonly(deep_config):
    deep_config = deep_config.show()["scope"]["config"]
    assert deep_config["READ_ONLY"]["value"] == "written in titanium"  # new value
    assert "additional desc of read-only" in deep_config["READ_ONLY"]["desc"]  # new description
    assert deep_config["READ_ONLY"]["readonly"]  # still read-only, from base


def test_04_only_in_base(deep_config):
    deep_config = deep_config.show()["scope"]["config"]
    assert deep_config["G"]["value"] == "G"


@pytest.mark.xfail(reason="Unsure where the Forged value for F is supposed to come from")
def test_05_add_readonly(deep_config):
    deep_config = deep_config.show()["scope"]["config"]
    assert deep_config["F"]["value"] == "Forged"
    assert deep_config["F"]["readonly"]
    assert deep_config["F"]["desc"] == "descF. back to beta section."
    assert deep_config["F"]["section"] == "section beta"
