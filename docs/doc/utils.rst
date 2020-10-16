###############
Utility modules
###############

The utility modules under `biothings.utils` provides a collections of utilities used across the BioThings SDK code base.
They also provide a level of abstraction for individual functionalities.

.. toctree::
    :maxdepth: 3


***************
biothings.utils
***************
.. automodule:: biothings.utils
    :members:

.. Use the following code snippet to generate module list
    xli = list(os.walk('biothings/utils'))[0][2]
    xli = sorted([x[:-3] for x in xli if not x.startswith('__')])
    tpl = '''{0}
    {1}
    .. automodule:: biothings.utils.{0}
        :members:
    '''
    print(''.join([tpl.format(x, '='*len(x)) for x in xli]))

aws
===
.. automodule:: biothings.utils.aws
    :members:

backend
=======
.. automodule:: biothings.utils.backend
    :members:

common
======
.. automodule:: biothings.utils.common
    :members:

dataload
========
.. automodule:: biothings.utils.dataload
    :members:

diff
====
.. automodule:: biothings.utils.diff
    :members:

doc_traversal
=============
.. automodule:: biothings.utils.doc_traversal
    :members:

dotfield
========
.. automodule:: biothings.utils.dotfield
    :members:

dotstring
=========
.. automodule:: biothings.utils.dotstring
    :members:

es
==
.. automodule:: biothings.utils.es
    :members:

exclude_ids
===========
.. automodule:: biothings.utils.exclude_ids
    :members:

hub
===
.. automodule:: biothings.utils.hub
    :members:

hub_db
======
.. automodule:: biothings.utils.hub_db
    :members:

inspect
=======
.. automodule:: biothings.utils.inspect
    :members:

jsondiff
========
.. automodule:: biothings.utils.jsondiff
    :members:

jsonpatch
=========
.. automodule:: biothings.utils.jsonpatch
    :members:

jsonschema
==========
.. automodule:: biothings.utils.jsonschema
    :members:

loggers
=======
.. automodule:: biothings.utils.loggers
    :members:

manager
=======
.. automodule:: biothings.utils.manager
    :members:

mongo
=====
.. automodule:: biothings.utils.mongo
    :members:

.. parallel is deprecated!
.. parallel
.. ========
.. .. automodule:: biothings.utils.parallel
..     :members:

parallel_mp
===========
.. automodule:: biothings.utils.parallel_mp
    :members:

redis
=====
.. automodule:: biothings.utils.redis
    :members:

.. shelve is deprecated!
.. shelve
.. ======
.. .. automodule:: biothings.utils.shelve
..     :members:

slack
=====
.. automodule:: biothings.utils.slack
    :members:

sqlite3
=======
.. automodule:: biothings.utils.sqlite3
    :members:

version
=======
.. automodule:: biothings.utils.version
    :members:
