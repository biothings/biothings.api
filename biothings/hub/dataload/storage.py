import warnings

warnings.filterwarnings("default", category=DeprecationWarning, module=__name__)
warnings.warn(
    "This module is deprecated, please use `biothings.utils.storage` module", DeprecationWarning, stacklevel=2
)
from biothings.utils.storage import *  # NOQA #F403
