''' Biothings Test Utility '''

import os
import sys

from .helper import BiothingsTestCase, TornadoTestServerMixin

# Add Project Folder to PYTHONPATH
SRC_PATH = os.path.dirname(sys.path[0])
if SRC_PATH not in sys.path:
    sys.path.insert(1, SRC_PATH)
