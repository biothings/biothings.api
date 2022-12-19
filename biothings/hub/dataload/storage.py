import warnings

warnings.filterwarnings("default", category=DeprecationWarning, module=__name__)
warnings.warn(
    "This module is deprecated, please use `biothings.utils.storage` module", DeprecationWarning
)
from biothings.utils.storage import *  # NOQA #F403
