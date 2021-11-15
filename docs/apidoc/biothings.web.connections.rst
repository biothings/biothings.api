biothings.web.connections
===============================

.. automodule:: biothings.web.connections
   :members:
   :undoc-members:
   :show-inheritance:

Additionally, you can reuse connecions initialized with the same
parameters by getting it from the connection pools every time.
Here's the connection pool interface signature:

.. autoclass:: biothings.web.connections._ClientPool
   :members:
   :undoc-members:
   :show-inheritance:

The module has already initialized connection pools for each supported
databases. Directly access these pools without creating by yourselves.

.. autodata:: es
.. autodata:: sql
.. autodata:: mongo
