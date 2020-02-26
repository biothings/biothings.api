import sys, os, pprint

from nose.tools import ok_, eq_

class DeepConfigTest(object):

    __test__ = True

    def setUp(self):
        import conf_deep
        conf_deep.HUB_DB_BACKEND = {
                "module" : "biothings.utils.sqlite3",
                "sqlite_db_folder" : "."
                } # mandatory for
        conf_deep.DATA_HUB_DB_DATABASE = "test_config"
        import biothings
        biothings.config_for_app(conf_deep)
        from biothings import config
        self.confobj = config
        self.config = self.confobj.show()["scope"]["config"]

    def test_01_override_value(self):
        eq_(self.config["D_D"]["value"],"d") # new value
        eq_(self.config["D_D"]["desc"],"descD_D") # but description hasn't changed, taken from base
        eq_(self.config["D_D"]["section"],"section beta") # same for section, from base

    def test_02_override_desc(self):
        eq_(self.config["E"]["value"],"heu") # new value
        eq_(self.config["E"]["desc"],"redefine description") # new description
        eq_(self.config["E"]["section"],"section gamma") # same for section

    def test_03_override_desc_of_readonly(self):
        eq_(self.config["READ_ONLY"]["value"],"written in titanium") # new value
        eq_(self.config["READ_ONLY"]["desc"],"redefine desc of read-only") # new description
        eq_(self.config["READ_ONLY"]["readonly"],True) # still read-onlu, from base

    def test_04_only_in_base(self):
        eq_(self.config["G"]["value"],"G")

    def test_05_add_readonly(self):
        eq_(self.config["F"]["value"],"Forged")
        eq_(self.config["F"]["readonly"],True)
        eq_(self.config["F"]["desc"],"descF\nback to beta section")
        eq_(self.config["F"]["section"],"section beta")

