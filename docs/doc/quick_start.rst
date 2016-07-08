Quick start
-----------

biothings.api is a python framework for creating and customizing high performance APIs that serve aggregated biological data.  biothings.api is based on the `tornado <http://www.tornadoweb.org/en/stable/>`_ web framework, and currently supports both full-text and id-lookup queries with an elasticsearch backend.

Installing biothings.api
^^^^^^^^^^^^^^^^^^^^^^^^

You can install the biothings.api framework with pip, like:

::
    
    pip install git+https://github.com/SuLab/biothings.api.git#egg=biothings

Alternatively, you can clone the `biothings.api repository <http://github.com/SuLab/biothings.api>`_ and run:

::
    
    python setup.py install


Starting a new biothings project
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once you have installed biothings.api, you can start a new project by running:

::

    biothings-admin.py *project_path* *biothing_name* [key=value ...]

This startup script will create a skeleton project directory based on the input parameter *biothing_name*.  For example, if you want to create an API that serves aggregated genetic variant data, you might make *biothing_name* variant.

.. Hint:: You can override any of the template variables by passing key=value pairs to the biothings-admin.py script.  A full list of variables used in the template can be found `here <https://github.com/SuLab/biothings.api/blob/master/biothings/bin/biothings-admin.py#L30>`_.

Biothings project directory structure
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ 

A bare biothings project consists of

* **requirements_web.txt** - list of python dependencies required to run API
* **src** - folder of all project-specific source code

    * **config.py** - all project-specific settings
    * **dataload** - all code to load data into elasticsearch
    * **www** - front-end code, handlers and elasticsearch querying
    
        * **index.py** - 
        * **api** - 

            * **es.py** -
            * **handlers.py** -

