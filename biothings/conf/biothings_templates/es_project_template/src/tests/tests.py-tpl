import sys
import os

# Add this directory to python path (contains nosetest_config)
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from biothings.tests import BiothingTest
from biothings.tests.settings import NosetestSettings

ns = NosetestSettings()

class {% nosetest_settings_class %}(BiothingTest):
    __test__ = True

    # Add extra nosetests here
    pass
