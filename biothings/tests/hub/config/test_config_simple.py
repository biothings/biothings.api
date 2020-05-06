import sys, os, pprint, json

from nose.tools import ok_, eq_, raises


class SimpleConfigTest(object):
    
    __test__ = True

    def setUp(self):
        import conf_base
        # reload as config manager may delete some params
        import importlib
        importlib.reload(conf_base)
        conf_base.HUB_DB_BACKEND = {
                "module" : "biothings.utils.sqlite3",
                "sqlite_db_folder" : "."
                } # mandatory for
        conf_base.DATA_HUB_DB_DATABASE = "unittest_config"
        # simulaye config param set at runtime, not from config files
        conf_base.DYNPARAM = "runtime"
        import biothings
        biothings.config_for_app(conf_base)
        from biothings import config
        self.confobj = config
        self.config = self.confobj.show()["scope"]["config"]
        from biothings.utils.hub_db import get_hub_config
        self.hub_config = get_hub_config()

    def tearDown(self):
        from biothings.utils.hub_db import get_hub_config
        get_hub_config().drop()

    def test_00_structure(self):
        conf = self.confobj.show()
        eq_(list(conf.keys()),["scope","allow_edits","_dirty"])
        eq_(list(conf["scope"].keys()), ["config","class"])
        eq_(conf["scope"]["class"],{}) # no config for superseded class

    def test_01_top_param(self):
        eq_(self.config["ONE"]["value"],1)
        eq_(self.config["ONE"]["value"],self.config["ONE"]["default"]) # not superseded
        eq_(self.config["ONE"]["section"],None)
        eq_(self.config["ONE"]["desc"],"descONE")

    def test_02_section_init(self):
        eq_(self.config["B"]["value"],"B")
        eq_(self.config["B"]["value"],self.config["B"]["default"]) # not superseded
        eq_(self.config["B"]["section"],"section alpha")
        eq_(self.config["B"]["desc"],None) # no comment

    def test_03_section_continue(self):
        eq_(self.config["C"]["value"],"C")
        eq_(self.config["C"]["value"],self.config["C"]["default"]) # not superseded
        eq_(self.config["C"]["section"],"section alpha")
        eq_(self.config["C"]["desc"],"ends with space should be stripped descC") # inline comment

    def test_04_section_new(self):
        # includes underscore in param
        eq_(self.config["D_D"]["value"],"D")
        eq_(self.config["D_D"]["value"],self.config["D_D"]["default"])
        eq_(self.config["D_D"]["section"],"section beta")
        eq_(self.config["D_D"]["desc"],"descD_D")

    def test_05_section_another(self):
        eq_(self.config["E"]["value"],"E")
        eq_(self.config["E"]["value"],self.config["E"]["default"])
        eq_(self.config["E"]["section"],"section gamma")
        eq_(self.config["E"]["desc"],"descE")

    def test_06_section_redefine(self):
        eq_(self.config["F"]["value"],"F")
        eq_(self.config["F"]["value"],self.config["F"]["default"])
        eq_(self.config["F"]["section"],"section beta")
        eq_(self.config["F"]["desc"],"descF\nback to beta section")

    def test_07_section_reset(self):
        eq_(self.config["G"]["value"],"G")
        eq_(self.config["G"]["value"],self.config["G"]["default"])
        eq_(self.config["G"]["section"],None)
        eq_(self.config["G"]["desc"],"reset section")

    def test_08_invisible(self):
        eq_(self.config.get("INVISIBLE"),None)

    def test_09_value_hidden(self):
        eq_(self.config["PASSWORD"]["value"],"********")
        eq_(self.config["PASSWORD"]["desc"],"hide the value, not the param")

    def test_10_read_only(self):
        eq_(self.config["READ_ONLY"]["value"],"written in stone")
        eq_(self.config["READ_ONLY"]["readonly"],True)
        eq_(self.config["READ_ONLY"]["desc"],"it's readonly")

    def test_11_read_only_value_hidden(self):
        eq_(self.config["READ_ONLY_PASSWORD"]["value"],"********")
        eq_(self.config["READ_ONLY_PASSWORD"]["readonly"],True)
        eq_(self.config["READ_ONLY_PASSWORD"]["desc"],"it's read-only and value is hidden, not the param")

    def test_12_invisible_has_precedence(self):
        eq_(self.config.get("INVISIBLE_READ_ONLY"),None)

    def test_13_dynamic_param_readonly(self):
        eq_(self.config["DYNPARAM"]["value"],"runtime")
        eq_(self.config["DYNPARAM"]["readonly"], True)

    @raises(AssertionError)
    def test_14_readonly_not_editable(self):
        self.confobj.store_value_to_db("READ_ONLY","trying anyway")

    @raises(AssertionError)
    def test_15_invisible_not_editable(self):
        self.confobj.store_value_to_db("INVISIBLE","trying anyway")

    def test_16_edit(self):
        newval = "a new life for B"
        self.confobj.store_value_to_db("B",json.dumps(newval))
        eq_(self.confobj.dirty,True)
        # cached cleared
        eq_(self.confobj.bykeys,{})
        eq_(self.confobj.byroots,{})
        eq_(self.confobj._original_params,{})
        # update config dict
        self.config = self.confobj.show()["scope"]["config"]
        d = self.hub_config.find_one({"_id":"B"})
        eq_(d["value"],newval)
        eq_(d["scope"],"config") # that's the default
        eq_(self.config["B"]["value"],newval)

    @raises(AssertionError)
    def test_17_special_param_not_editable(self):
        self.confobj.store_value_to_db("CONFIG_READONLY","trying anyway")


