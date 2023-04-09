"""A Programmatic Query API supporting Biothings Query Syntax.

From an architecture perspective, :py:mod:`biothings.web.query` is one of
the data services, built on top of the :py:mod:`biothings.web.connections`
layer, however, due to the complexity of the module, it is escalated one
level in organization to simplify the overall folder structure. The features
are available in `biothings.web.services.query` namespace via import.

"""

from biothings.web.query import *
