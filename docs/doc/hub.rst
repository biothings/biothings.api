#############
Hub component
#############

The purpose of the BioThings hub component is to allow you to easily automate the parsing and uploading of your data to an Elasticsearch backend.

.. py:module:: biothings

********
uploader
********

BaseSourceUploader
==================

.. autoclass:: biothings.dataload.uploader.BaseSourceUploader
    :members:

NoBatchIgnoreDuplicatedSourceUploader
-------------------------------------

.. autoclass:: biothings.dataload.uploader.NoBatchIgnoreDuplicatedSourceUploader
    :members:

IgnoreDuplicatedSourceUploader
------------------------------

.. autoclass:: biothings.dataload.uploader.IgnoreDuplicatedSourceUploader
    :members:

MergerSourceUploader
--------------------

.. autoclass:: biothings.dataload.uploader.MergerSourceUploader
    :members:

DummySourceUploader
-------------------

.. autoclass:: biothings.dataload.uploader.DummySourceUploader
    :members:

ParallelizedSourceUploader
--------------------------

.. autoclass:: biothings.dataload.uploader.ParallelizedSourceUploader
    :members:

NoDataSourceUploader
--------------------

.. autoclass:: biothings.dataload.uploader.NoDataSourceUploader
    :members:

******
dumper
******

BaseDumper
==========

.. autoclass:: biothings.dataload.dumper.BaseDumper
    :members:

FTPDumper
---------

.. autoclass:: biothings.dataload.dumper.FTPDumper
    :members:

HTTPDumper
----------

.. autoclass:: biothings.dataload.dumper.HTTPDumper
    :members:

GoogleDriveDumper
+++++++++++++++++

.. autoclass:: biothings.dataload.dumper.GoogleDriveDumper
    :members:

WgetDumper
----------

.. autoclass:: biothings.dataload.dumper.WgetDumper
    :members:

DummyDumper
-----------

.. autoclass:: biothings.dataload.dumper.DummyDumper
    :members:

ManualDumper
-----------

.. autoclass:: biothings.dataload.dumper.ManualDumper
    :members:

*******
indexer
*******

Indexer
=======

.. autoclass:: biothings.dataindex.indexer.Indexer
    :members:

*******
builder
*******

DataBuilder
===========

.. autoclass:: biothings.databuild.builder.DataBuilder
    :members:

******
differ
******

BaseDiffer
==========

.. autoclass:: biothings.databuild.differ.BaseDiffer
    :members:

JsonDiffer
----------

.. autoclass:: biothings.databuild.differ.JsonDiffer
    :members:

SelfContainedJsonDiffer
+++++++++++++++++++++++

.. autoclass:: biothings.databuild.differ.SelfContainedJsonDiffer
    :members:

DiffReportRendererBase
======================

.. autoclass:: biothings.databuild.differ.DiffReportRendererBase
    :members:   

DiffReportTxt
-------------

.. autoclass:: biothings.databuild.differ.DiffReportTxt
    :members:

******
syncer
******

BaseSyncer
==========

.. autoclass:: biothings.databuild.syncer.BaseSyncer
    :members:

MongoJsonDiffSyncer
-------------------

.. autoclass:: biothings.databuild.syncer.MongoJsonDiffSyncer
    :members:

MongoJsonDiffSelfContainedSyncer
--------------------------------

.. autoclass:: biothings.databuild.syncer.MongoJsonDiffSelfContainedSyncer
    :members:

ESJsonDiffSyncer
----------------

.. autoclass:: biothings.databuild.syncer.ESJsonDiffSyncer
    :members:

ESJsonDiffSelfContainedSyncer
-----------------------------

.. autoclass:: biothings.databuild.syncer.ESJsonDiffSelfContainedSyncer
    :members:
