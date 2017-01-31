Biothings SDK tutorial
----------------------

The following tutorial shows how to create a "hub", a piece of software used to
download, maintain up-to-date, process, merge data. This part of Biothings is used
to create an ElasticSearch, which is then queried by the Biothings webservice API.

Prerequesites
^^^^^^^^^^^^^

Biothings SDK uses MongoDB as backend. You must a have working MongoDB instance you can connect to.
We'll also perform some basic commands. You must also have Biothings SDK installed (
``git clone https://github.com/SuLab/biothings.api.git`` is usually enough, followed by
``pip install -r requirements.txt``). You may want to use ``virtualenv`` to isolate your installation.
Finally, Biothings SDK is written in python, so you must know some basics.

Configuration file
^^^^^^^^^^^^^^^^^^

Before starting to implement our hub, we first need to define a configuration file. There’s a **config.py.example** that
can be used as a template. Some important settings are about:

* MongDB connections parameters, ``DATA_SRC_*`` and ``DATA_TARGET_*`` parameters. They define connections to two different databases,
  one will contain individual collections for each datasource (SRC) and the other will contain merged collections (TARGET).

* ``DATA_ARCHIVE_ROOT`` contains the path of the root folder that will contain all the downloaded and processed data.
  Other parameters should be self-explanatory and probably don’t need to be changed. Copy **config.py.example** as **config.py**
  and adjust it to your need.

hub.py
^^^^^^

This script represents the main hub executable. Each hub should define it, this is where the different hub commands are going to be
defined and where tasks are actually running. It’s also from this script that a SSH server will run so we can actually log
into the hub and access those registered commands.

Along this tutorial, we will enrich that script. For now, we’re just going to define a JobManager, the SSH server and
make sure everything is running fine.

.. code-block:: python

   import asyncio, asyncssh, sys
   import concurrent.futures
   from functools import partial

   import config, biothings
   biothings.config_for_app(config)

   from biothings.utils.manager import JobManager

   loop = asyncio.get_event_loop()
   process_queue = concurrent.futures.ProcessPoolExecutor(max_workers=2)
   thread_queue = concurrent.futures.ThreadPoolExecutor()
   loop.set_default_executor(process_queue)
   jmanager = JobManager(loop,
                         process_queue, thread_queue,
                         max_memory_usage=None,
                         )

``jmanager`` is our JobManager, it’s going to be used everywhere in the hub, each time a parallelized job is created.
Species hub is a small one, there’s no need for many process workers, two should be fine.

Next, let’s define some basic commands for our new hub:


.. code-block:: python

   from biothings.utils.hub import schedule, top, pending, done
   COMMANDS = {
           "sch" : partial(schedule,loop),
           "top" : partial(top,process_queue,thread_queue),
           "pending" : pending,
           "done" : done,
           }

These commands are then registered in the SSH server, which is linked to a python interpreter.
Commands will be part of the interpreter’s namespace and be available from a SSH connection.

.. code-block:: python

    passwords = {
            'guest': '', # guest account with no password
            }

    from biothings.utils.hub import start_server
    server = start_server(loop, "Species hub",passwords=passwords,port=7022,commands=COMMANDS)

    try:
        loop.run_until_complete(server)
    except (OSError, asyncssh.Error) as exc:
        sys.exit('Error starting server: ' + str(exc))

    loop.run_forever()

Let’s try to run that script ! The first run, it will complain about some missing SSH key:

.. code:: bash

   AssertionError: Missing key 'bin/ssh_host_key' (use: 'ssh-keygen -f bin/ssh_host_key' to generate it

Let’s generate it, following instruction. Now we can run it again and try to connect:

.. code:: bash

   $ ssh guest@localhost -p 7022
   The authenticity of host '[localhost]:7022 ([127.0.0.1]:7022)' can't be established.
   RSA key fingerprint is SHA256:USgdr9nlFVryr475+kQWlLyPxwzIUREcnOCyctU1y1Q.
   Are you sure you want to continue connecting (yes/no)? yes
   Warning: Permanently added '[localhost]:7022' (RSA) to the list of known hosts.

   Welcome to Species hub, guest!
   hub>

Let’s try a command:

.. code-block:: bash

   hub> top()
   0 running job(s)
   0 pending job(s), type 'top(pending)' for more

Nothing fancy here, we don’t have much in our hub yet, but everything is running fine.


Dumpers
^^^^^^^

Biothings species API gathers data from different datasources. We will need to define
different dumpers to make this data available locally for further processing.

Taxonomy dumper
===============
This dumper will download taxonomy data from NCBI FTP server. There’s one file to download,
available at this location: ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz.

When defining a dumper, we’ll need to choose a base class to derive our dumper class from.
There are different base dumper classes available in Biothings SDK, depending on the protocol
we want to use to download data. In this case, we’ll derive our class from ``biothings.dataload.dumper.FTPDumper``.
In addition to defining some specific class attributes, we will need to implement a method called ``create_todump_list()``.
This method fills ``self.to_dump`` list, which is later going to be used to download data.
One element in that list is a dictionary with the following structure:

.. code-block:: python

   {"remote": "<path to file on remote server", "local": "<local path to file>"}

Remote information are relative to the working directory specified as class attribute. Local information is an absolute path, containing filename used to save data.

Let’s start coding. We’ll save that python module in `dataload/sources/taxonomy/dumper.py <https://github.com/SuLab/biothings.species/blob/master/src/dataload/sources/taxonomy/dumper.py>`_.

.. code-block:: python

   import biothings, config
   biothings.config_for_app(config)

Those lines are used to configure Biothings SDK according to our own configuration information.

.. code-block:: python

   from config import DATA_ARCHIVE_ROOT
   from biothings.dataload.dumper import FTPDumper

We then import a configuration constant, and the FTPDumper base class.

.. code-block:: python

   class TaxonomyDumper(FTPDumper):

       SRC_NAME = "taxonomy"
       SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
       FTP_HOST = 'ftp.ncbi.nih.gov'
       CWD_DIR = '/pub/taxonomy'
       SUFFIX_ATTR = "timestamp"
       SCHEDULE = "0 9 * * *"

* ``SRC_NAME`` will used as the registered name for this datasource (more on this later).
* ``SRC_ROOT_FOLDER`` is the folder path for this resource, without any version information
  (dumper will create different sub-folders for each version).
* ``FTP_HOST`` and ``CWD_DIR`` gives information to connect to the remove FTP server and move to appropriate
  remote directory (``FTP_USER`` and ``FTP_PASSWD`` constants can also be used for authentication).
* ``SUFFIX_ATTR`` defines the attributes that’s going to be used to create folder for each downloaded version.
  It’s basically either “release” or “timestamp”, depending on whether the resource we’re trying to dump
  has an actual version. Here, for taxdump file, there’s no version, so we’re going to use “timestamp”.
  This attribute is automatically set to current date, so folders will look like that: **.../taxonomy/20170120**, **.../taxonomy/20170121**, etc…
* Finally ``SCHEDULE``, if defined, will allow that dumper to regularly run within the hub.
  This is a cron-like notation (see aiocron documentation for more).

We now need to tell the dumper what to download, that is, create that self.to_dump list:

.. code-block:: python

   def create_todump_list(self, force=False):
       file_to_dump = "taxdump.tar.gz"
       new_localfile = os.path.join(self.new_data_folder,file_to_dump)
       try:
           current_localfile = os.path.join(self.current_data_folder, file_to_dump)
       except TypeError:
           # current data folder doesn't even exist
           current_localfile = new_localfile
       if force or not os.path.exists(current_localfile) or self.remote_is_better(file_to_dump, current_localfile):
           # register new release (will be stored in backend)
           self.to_dump.append({"remote": file_to_dump, "local":new_localfile})

That method tries to get the latest downloaded file and then compare that file with the remote file using
``self.remote_is_better(file_to_dump, current_localfile)``, which compares the dates and return True if the remote is more recent.
A dict is then created with required elements and appened to ``self.to_dump`` list.

When the dump is running, each element from that self.to_dump list will be submitted to a job and be downloaded in parallel.
Let’s try our new dumper. We need to update ``hub.py`` script to add a DumperManager and then register this dumper:

In `hub.py <https://github.com/SuLab/biothings.species/blob/master/src/bin/hub.py>`_:

.. code-block:: python

   import dataload
   import biothings.dataload.dumper as dumper

   dmanager = dumper.DumperManager(job_manager=jmanager)
   dmanager.register_sources(dataload.__sources__)
   dmanager.schedule_all()

Let’s also register new commands in the hub:

.. code-block:: python

   COMMANDS = {
        # dump commands
       "dm" : dmanager,
       "dump" : dmanager.dump_src,
   ...

``dm`` will a shortcut for the dumper manager object, and ``dump`` will actually call manager’s ``dump_src()`` method.

Manager is auto-registering dumpers from list defines in dataload package. Let’s define that list:

In `dataload/__init__.py <https://github.com/SuLab/biothings.species/blob/master/src/dataload/__init__.py>`_:

.. code-block:: python

   __sources__ = [
           "dataload.sources.taxonomy",
   ]

That’s it, it’s just a string pointing to our taxonomy package. We’ll expose our dumper class in that package
so the manager can inspect it and find our dumper (note: we could use give the full path to our dumper module,
``dataload.sources.taxonomy.dumper``, but we’ll add uploaders later, it’s better to have one single line per resource).

In `dataload/sources/taxonomy/__init__.py <https://github.com/SuLab/biothings.species/blob/master/src/dataload/sources/taxonomy/__init__.py>`_

.. code-block:: python

   from .dumper import TaxonomyDumper

Let’s run the hub again. We can on the logs that our dumper has been found:

.. code:: bash

   Found a class based on BaseDumper: '<class 'dataload.sources.taxonomy.dumper.TaxonomyDumper'>'

Also, manager has found scheduling information and created a task for this:

.. code:: bash

  Scheduling task functools.partial(<bound method DumperManager.create_and_dump of <DumperManager [1 registered]: ['taxonomy']>>, <class 'dataload.sources.taxonomy.dumper.TaxonomyDumper'>, job_manager=<biothings.utils.manager.JobManager object at 0x7f88fc5346d8>, force=False): 0 9 * * *

We can double-check this by connecting to the hub, and type some commands:

.. code:: bash

   Welcome to Species hub, guest!
   hub> dm
   <DumperManager [1 registered]: ['taxonomy']>

When printing the manager, we can check our taxonomy resource has been registered properly.

.. code:: bash

   hub> sch()
   DumperManager.create_and_dump(<class 'dataload.sources.taxonomy.dumper.TaxonomyDumper'>,) [0 9 * * * ] {run in 00h:39m:09s}

Dumper is going to run in 39 minutes ! We can trigger a manual upload too:

.. code:: bash

   hub> dump("taxonomy")
   [1] RUN {0.0s} dump("taxonomy")

OK, dumper is running, we can follow task status from the console. At some point, task will be done:

.. code:: bash

   hub>
   [1] OK  dump("taxonomy"): finished, [None]

It successfully run (OK), nothing was returned by the task ([None]). Logs show some more details:

.. code:: bash

   DEBUG:species.hub:Creating new TaxonomyDumper instance
   INFO:taxonomy_dump:1 file(s) to download
   DEBUG:taxonomy_dump:Downloading 'taxdump.tar.gz'
   INFO:taxonomy_dump:taxonomy successfully downloaded
   INFO:taxonomy_dump:success

Alright, now if we try to run the dumper again, nothing should be downloaded since we got the latest
file available. Let’s try that, here are the logs:

.. code:: bash

   DEBUG:species.hub:Creating new TaxonomyDumper instance
   DEBUG:taxonomy_dump:'taxdump.tar.gz' is up-to-date, no need to download
   INFO:taxonomy_dump:Nothing to dum

So far so good! The actual file, depending on the configuration settings, it’s located in **./data/taxonomy/20170125/taxdump.tar.gz**.
We can notice the timestamp used to create the folder. Let’s also have a look at in the internal database to see the resource status. Connect to mongoDB:

.. code:: javascript

   > use dev_speciesdoc_src
   switched to db dev_speciesdoc_src
   > db.src_dump.find()
   {
           "_id" : "taxonomy",
           "release" : "20170125",
           "data_folder" : "./data/taxonomy/20170125",
           "pending_to_upload" : true,
           "download" : {
                   "logfile" : "./data/taxonomy/taxonomy_20170125_dump.log",
                   "time" : "4.52s",
                   "status" : "success",
                   "started_at" : ISODate("2017-01-25T08:32:28.448Z")
           }
   }
   >


We have some information about the download process, how long it took to download files, etc… We have the path to the
``data_folder`` containing the latest version, the ``release`` number (here, it’s a timestamp), and a flag named ``pending_to_upload``.
That will be used later to automatically trigger an upload after a dumper has run.

So the actual file is currently compressed, we need to uncompress it before going further. We can add a post-dump step to our dumper.
There are two options there, by overriding one of those methods:

.. code-block:: python

   def post_download(self, remotefile, localfile): triggered for each downloaded file
   def post_dump(self): triggered once all files have been downloaded

We could use either, but there’s a utility function available in BiothingsSDK that uncompress everything in a directory, let’s use it in a global post-dump step:

.. code-block:: python

   from biothings.utils.common import untargzall
   ...

       def post_dump(self):
           untargzall(self.new_data_folder)

``self.new_data_folder`` is the path to the folder freshly created by the dumper (in our case, **./data/taxonomy/20170125**)

Let’s try this in the console (restart the hub to make those changes alive). Because file is up-to-date, dumper will not run. We need to force it:

.. code:: bash

   hub> dump("taxonomy",force=True)

Or, instead of downloading the file again, we can directly trigger the post-dump step:

.. code:: bash

   hub> dump("taxonomy",steps="post")

There are 2 steps steps available in a dumper:

1. **dump** : will actually download files
2. **post** : will post-process downloaded files (post_dump)

By default, both run sequentially.

After typing either of these commands, logs will show some information about the uncompressing step:

.. code:: bash

   DEBUG:species.hub:Creating new TaxonomyDumper instance
   INFO:taxonomy_dump:success
   INFO:root:untargz '/opt/slelong/Documents/Projects/biothings.species/src/data/taxonomy/20170125/taxdump.tar.gz'

Folder contains all uncompressed files, ready to be process by an uploader.

Species dumper
==============

Following guideline from previous taxonomy dumper, we’re now implementing a new dumper used to download species list.
There’s just one file to be downloaded from ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/docs/speclist.txt.
Same as before, dumper will inherits FTPDumper base class. File is not compressed, so except this, this dumper will look the same.

Code is available on github for further details: `ee674c55bad849b43c8514fcc6b7139423c70074 <https://github.com/SuLab/biothings.species/commit/ee674c55bad849b43c8514fcc6b7139423c70074>`_
for the whole commit changes, and `dataload/sources/species/dumper.py <https://github.com/SuLab/biothings.species/blob/master/src/dataload/sources/species/dumper.py>`_ for the actual dumper.

Gene information dumper
=======================

The last dumper we have to implement will download some gene information from NCBI (ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_info.gz).
It’s very similar to the first one (we could even have merged them together).

Code is available on github:
`d3b3486f71e865235efd673d2f371b53eaa0bc5b <https://github.com/SuLab/biothings.species/commit/d3b3486f71e865235efd673d2f371b53eaa0bc5b>`_
for whole changes and `dataload/sources/geneinfo/dumper.py <https://github.com/SuLab/biothings.species/blob/master/src/dataload/sources/geneinfo/dumper.py>`_ for the dumper.

Uploaders
^^^^^^^^^

Now that we have local data available, we can process them. We’re going to create 3 different uploaders, one for each datasource.
Each uploader will load data into MongoDB, into individual/single collections. Those will then be used in the last merging step.

Before going further, we’ll first create an UploaderManager instance and register some of its commands in the hub:

.. code-block:: python

   import biothings.dataload.uploader as uploader
   # will check every 10 seconds for sources to upload
   umanager = uploader.UploaderManager(poll_schedule = '* * * * * */10', job_manager=jmanager)
   umanager.register_sources(dataload.__sources__)
   umanager.poll()

   COMMANDS = {
   ...
           # upload commands
           "um" : umanager,
           "upload" : umanager.upload_src,
   ...


Running the hub, we’ll see the kind of log statements:

.. code:: bash

   INFO:species.hub:Found 2 resources to upload (['species', 'geneinfo'])
   INFO:species.hub:Launch upload for 'species'
   ERROR:species.hub:Resource 'species' needs upload but is not registerd in manager
   INFO:species.hub:Launch upload for 'geneinfo'
   ERROR:species.hub:Resource 'geneinfo' needs upload but is not registerd in manager
   ...

Indeed, datasources have been dumped, and a ``pending_to_upload`` flag has been to True in ``src_dump``. UploadManager polls this ``src_dump``
internal collection, looking for this flag. If set, it runs automatically the corresponding uploader(s). Since we didn’t implement any uploaders yet,
manager complains… Let’s fix that.

Taxonomy uploader
=================

The taxonomy files we downloaded need to be parsed and stored into a MongoDB collection. We won’t go in too much details regarding the actual parsing,
there are two parsers, one for **nodes.dmp** and another for **names.dmp** files. They yield dictionaries as the result of this parsing step. We just
need to “connect” those parsers to uploaders.

Following the same approach as for dumpers, we’re going to implement our first uploaders by inheriting one the base classes available in Biothings SDK.
We have two files to parse, data will stored in two different mongoDB collections, so we’re going to have two uploaders. Each inherits from
``biothings.dataload.uploader.BaseSourceUploader``, ``load_data`` method has to be implemented, this is where we “connect” parsers.

Beside this method, another important point relates to the storage engine. ``load_data`` will, through the parser, yield documents (dictionaries).
This data is processed internally by the base uploader class (``BaseSourceUploader``) using a storage engine. ``BaseSourceUploader`` uses
``biothings.dataload.storage.BasicStorage`` as its engine. This storage inserts data in mongoDB collection using bulk operations for better performances.
There are other storages available, depending on how data should be inserted (eg. IgnoreDuplicatedStorage will ignore any duplicated data error).
While choosing a base uploader class, we need to consider which storage class it’s actually using behind-the-scene (an alternative way to do this is
using ``BaseSourceUploader`` and set the class attribute storage_class, such as in this uploader:
`biothings/dataload/uploader.py#L417 <https://github.com/SuLab/biothings.api/blob/master/biothings/dataload/uploader.py#L418>`_).

The first uploader will take care of nodes.dmp parsing and storage.

.. code-block:: python

   import biothings.dataload.uploader as uploader
   from .parser import parse_refseq_names, parse_refseq_nodes

   class TaxonomyNodesUploader(uploader.BaseSourceUploader):

       main_source = "taxonomy"
       name = "nodes"

       def load_data(self,data_folder):
           nodes_file = os.path.join(data_folder,"nodes.dmp")
           self.logger.info("Load data from file '%s'" % nodes_file)
           return parse_refseq_nodes(open(nodes_file))

* ``TaxonomyNodesUploader`` derives from ``BaseSourceUploader``
* ``name`` gives the name of the collection used to store the data. If ``main_source`` is *not* defined,
  it must match ``SRC_NAME`` in dumper’s attributes
* ``main_source`` is optional and allows to define main sources and sub-sources. Since we have 2 parsers here,
  we’re going to have 2 collections created. For this one, we want the collection named “nodes”. But this parser
  relates to *taxonomy* datasource, so we define a ``main source`` called **taxonomy**, which matches ``SRC_NAME`` in dumper’s attributes.
* ``load_data()``  has ``data_folder`` as parameter. It will be set accordingly, to the path of the last version dumped.
  Also, that method gets data from parsing function ``parse_refseq_nodes``. It’s where we “connect” the parser. We just need to
  return parser’s result so the storage can actually store the data.

The other parser, for names.dmp, is almost the same:

.. code-block:: python

   class TaxonomyNamesUploader(uploader.BaseSourceUploader):

       main_source = "taxonomy"
       name = "names"

       def load_data(self,data_folder):
           names_file = os.path.join(data_folder,"names.dmp")
           self.logger.info("Load data from file '%s'" % names_file)
           return parse_refseq_names(open(names_file))

We then need to “expose” those parsers in taxonomy package, in `dataload/sources/taxonomy/__init__.py <https://github.com/SuLab/biothings.species/blob/master/src/dataload/sources/taxonomy/__init__.py>`_:

.. code-block:: python

   from .uploader import TaxonomyNodesUploader, TaxonomyNamesUploader

Now let’s try to run the hub again. We should see uploader manager has automatically triggered some uploads:

.. code:: bash

   INFO:species.hub:Launch upload for 'taxonomy'
   ...
   ...
   INFO:taxonomy.names_upload:Uploading 'names' (collection: names)
   INFO:taxonomy.nodes_upload:Uploading 'nodes' (collection: nodes)
   INFO:taxonomy.nodes_upload:Load data from file './data/taxonomy/20170125/nodes.dmp'
   INFO:taxonomy.names_upload:Load data from file './data/taxonomy/20170125/names.dmp'
   INFO:root:Uploading to the DB...
   INFO:root:Uploading to the DB...

While running, we can check what jobs are running, using top() command:

.. code:: bash

   hub> top()
      PID    |              SOURCE               | CATEGORY |        STEP        |         DESCRIPTION          |   MEM    | CPU  |     STARTED_AT     | DURATION
   5795      | taxonomy.nodes                    | uploader | update_data        |                              | 49.7MiB  | 0.0% | 2017/01/25 14:58:40|15.49s
   5796      | taxonomy.names                    | uploader | update_data        |                              | 54.6MiB  | 0.0% | 2017/01/25 14:58:40|15.49s
   2 running job(s)
   0 pending job(s), type 'top(pending)' for more
   16 finished job(s), type 'top(done)' for more

We can see two uploaders running at the same time, one for each file. ``top(done)`` can also display jobs that are done and finally
``top(pending)`` can give an overview of jobs that are going to be launched when a worker is available (it happens when there are more
jobs created than the available number of workers overtime).

In ``src_dump`` collection, we can see some more information about the resource and its upload processes. Two jobs were created,
we have information about the duration, log files, etc...

.. code:: javascript

   > db.src_dump.find({_id:"taxonomy"})
   {
           "_id" : "taxonomy",
           "download" : {
                   "started_at" : ISODate("2017-01-25T13:09:26.423Z"),
                   "status" : "success",
                   "time" : "3.31s",
                   "logfile" : "./data/taxonomy/taxonomy_20170125_dump.log"
           },
           "data_folder" : "./data/taxonomy/20170125",
           "release" : "20170125",
           "upload" : {
                   "status" : "success",
                   "jobs" : {
                           "names" : {
                                   "started_at" : ISODate("2017-01-25T14:58:40.034Z"),
                                   "pid" : 5784,
                                   "logfile" : "./data/taxonomy/taxonomy.names_20170125_upload.log",
                                   "step" : "names",
                                   "temp_collection" : "names_temp_eJUdh1te",
                                   "status" : "success",
                                   "time" : "26.61s",
                                   "count" : 1552809,
                                   "time_in_s" : 27
                           },
                           "nodes" : {
                                   "started_at" : ISODate("2017-01-25T14:58:40.043Z"),
                                   "pid" : 5784,
                                   "logfile" : "./data/taxonomy/taxonomy.nodes_20170125_upload.log",
                                   "step" : "nodes",
                                   "temp_collection" : "nodes_temp_T5VnzRQC",
                                   "status" : "success",
                                   "time" : "22.4s",
                                   "time_in_s" : 22,
                                   "count" : 1552809
                           }
                   }
           }
   }

In the end, two collections were created, containing parsed data:

.. code:: javascript

   > db.names.count()
   1552809
   > db.nodes.count()
   1552809

   > db.names.find().limit(2)
   {
           "_id" : 1,
           "taxid" : 1,
           "other_names" : [
                   "all"
           ],
           "scientific_name" : "root"
   }
   {
           "_id" : 2,
           "other_names" : [
                   "bacteria",
                   "not bacteria haeckel 1894"
           ],
           "genbank_common_name" : "eubacteria",
           "in-part" : [
                   "monera",
                   "procaryotae",
                   "prokaryota",
                   "prokaryotae",
                   "prokaryote",
                   "prokaryotes"
           ],
           "taxid" : 2,
           "scientific_name" : "bacteria"
   }

   > db.nodes.find().limit(2)
   { "_id" : 1, "rank" : "no rank", "parent_taxid" : 1, "taxid" : 1 }
   {
           "_id" : 2,
           "rank" : "superkingdom",
           "parent_taxid" : 131567,
           "taxid" : 2
   }


Species uploader
================

Following the same guideline, we’re going to create another uploader for species file.

.. code-block:: python

   import biothings.dataload.uploader as uploader
   from .parser import parse_uniprot_speclist

   class SpeciesUploader(uploader.BaseSourceUploader):

       name = "species"

       def load_data(self,data_folder):
           nodes_file = os.path.join(data_folder,"speclist.txt")
           self.logger.info("Load data from file '%s'" % nodes_file)
           return parse_uniprot_speclist(open(nodes_file))


In that case, we need only one uploader, so we just define “name” (no need to define main_source here).

We need to expose that uploader from the package, in `dataload/sources/species/__init__.py <https://github.com/SuLab/biothings.species/blob/master/src/dataload/sources/species/__init__.py>`_:

.. code-block:: python

   from .uploader import SpeciesUploader

Let’s run this through the hub. We can use the “upload” command there (though manager should trigger the upload itself):

.. code:: bash

   hub> upload("species")
   [1] RUN {0.0s} upload("species")

Similar to dumpers, there are different steps we can individually call for an uploader:

* **data**: will take care of storing data
* **post**: calls post_update() method, once data has been inserted. Useful to post-process data or create an index for instance
* **master**: will register the source in src_master collection, which is used during the merge step.
  Uploader method ``get_mapping()<`` can optionally returns an ElasticSearch mapping, it will be stored in src_master during
  that step. We’ll see more about this later.
* **clean**: will clean temporary collections and other leftovers...

Within the hub, we can specify these steps manually (they’re all executed by default).

.. code:: bash

   hub> upload("species",steps="clean")

Or using a list:

.. code:: bash

   hub> upload("species",steps=["data","clean"])

Gene information uploader
=========================

Let’s move forward and implement the last uploader. The goal for this uploader is to identify whether, for a taxonomy ID, there are
existing/known genes. File contains information about genes, first column is the ``taxid``. We want to know all taxonomy IDs present
in the file, and the merged document, we want to add key such as ``{'has_gene' : True/False}``.

Obviously, we’re going to have a lot of duplicates, because for one taxid we can have many genes present in the files.
We have options here 1) remove duplicates before inserting data in database, or 2) let the database handle the duplicates (rejecting them).
Though we could process data in memory -- processed data is rather small in the end --, for demo purpose, we’ll go for the second option.

.. code-block:: python

   import biothings.dataload.uploader as uploader
   import biothings.dataload.storage as storage
   from .parser import parse_geneinfo_taxid

   class GeneInfoUploader(uploader.BaseSourceUploader):

       storage_class = storage.IgnoreDuplicatedStorage

       name = "geneinfo"

       def load_data(self,data_folder):
           gene_file = os.path.join(data_folder,"gene_info")
           self.logger.info("Load data from file '%s'" % gene_file)
           return parse_geneinfo_taxid(open(gene_file))

* ``storage_class``: this is the most important setting in this case, we want to use a storage that will ignore any duplicated records.
* ``parse_geneinfo_taxid`` : is the parsing function, yield documents as ``{“_id” : "taxid"}``

The rest is closed to what we already encountered. Code is available on github in
`dataload/sources/geneinfo/uploader.py <https://github.com/SuLab/biothings.species/blob/master/src/dataload/sources/geneinfo/uploader.py>`_

When running the uploader, logs show statements like these:

.. code:: bash

   INFO:species.hub:Found 1 resources to upload (['geneinfo'])
   INFO:species.hub:Launch upload for 'geneinfo'
   INFO:species.hub:Building task: functools.partial(<bound method UploaderManager.create_and_load of <UploaderManager [3 registered]: ['geneinfo', 'species', 'taxonomy']>>, <class 'dataload.sources.gen
   einfo.uploader.GeneInfoUploader'>, job_manager=<biothings.utils.manager.JobManager object at 0x7fbf5f8c69b0>)
   INFO:geneinfo_upload:Uploading 'geneinfo' (collection: geneinfo)
   INFO:geneinfo_upload:Load data from file './data/geneinfo/20170125/gene_info'
   INFO:root:Uploading to the DB...
   INFO:root:Inserted 62 records, ignoring 9938 [0.3s]
   INFO:root:Inserted 15 records, ignoring 9985 [0.28s]
   INFO:root:Inserted 0 records, ignoring 10000 [0.23s]
   INFO:root:Inserted 31 records, ignoring 9969 [0.25s]
   INFO:root:Inserted 16 records, ignoring 9984 [0.26s]
   INFO:root:Inserted 4 records, ignoring 9996 [0.21s]
   INFO:root:Inserted 4 records, ignoring 9996 [0.25s]
   INFO:root:Inserted 1 records, ignoring 9999 [0.25s]
   INFO:root:Inserted 26 records, ignoring 9974 [0.23s]
   INFO:root:Inserted 61 records, ignoring 9939 [0.26s]
   INFO:root:Inserted 77 records, ignoring 9923 [0.24s]

While processing data in batch, some are inserted, others (duplicates) are ignored and discarded. The file is quite big, so the process can be long…

Note: should we want to implement the first option, the parsing function would build a dictionary indexed by taxid and would read the whole,
extracting taxid. The whole dict would then be returned, and then processed by storage engine.

So far, we’ve defined dumpers and uploaders, made them working together through some managers defined in the hub. We’re now ready to move the last step: merging data.

