from biothings.tests import BiothingTest
from biothings.tests.settings import NosetestSettings
from nose.tools import ok_, eq_

ns = NosetestSettings()

class {% nosetest_settings_class %}(BiothingTest):
    # Add extra nosetests here
    pass
