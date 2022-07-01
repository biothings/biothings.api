biothings.hub.commands
===============

This document will show you all available commands that can be used when you access the Hub shell, and their usages.

.. py:method:: status(managers)

function status in module biothings.hub

status(managers)
    Return a global hub status (number or sources, documents, etc...)
    according to available managers



.. py:method:: export_command_documents(filepath)

method export_command_documents in module biothings.hub

export_command_documents(filepath) method of biothings.hub.HubServer instance



.. py:method:: config()

method show in module biothings.utils.configuration

show() method of biothings.utils.configuration.ConfigurationWrapper instance



.. py:method:: setconf(name, value)

method store_value_to_db in module biothings.utils.configuration

store_value_to_db(name, value) method of biothings.utils.configuration.ConfigurationWrapper instance



.. py:method:: resetconf(name=None)

method reset in module biothings.utils.configuration

reset(name=None) method of biothings.utils.configuration.ConfigurationWrapper instance



.. py:method:: source_info(name, debug=False)

method get_source in module biothings.hub.dataload.source

get_source(name, debug=False) method of biothings.hub.dataload.source.SourceManager instance



.. py:method:: source_reset(name, key='upload', subkey=None)

method reset in module biothings.hub.dataload.source

reset(name, key='upload', subkey=None) method of biothings.hub.dataload.source.SourceManager instance
    Reset, ie. delete, internal data (src_dump document) for given source name, key subkey.
    This method is useful to clean outdated information in Hub's internal database.
    
    Ex: key=upload, name=mysource, subkey=mysubsource, will delete entry in corresponding
        src_dump doc (_id=mysource), under key "upload", for sub-source named "mysubsource"
    
    "key" can be either 'download', 'upload' or 'inspect'. Because there's no such notion of subkey for
    dumpers (ie. 'download', subkey is optional.



.. py:method:: dump(src, force=False, skip_manual=False, schedule=False, check_only=False, **kwargs)

method dump_src in module biothings.hub.dataload.dumper

dump_src(src, force=False, skip_manual=False, schedule=False, check_only=False, **kwargs) method of biothings.hub.dataload.dumper.DumperManager instance



.. py:method:: dump_all(force=False, **kwargs)

method dump_all in module biothings.hub.dataload.dumper

dump_all(force=False, **kwargs) method of biothings.hub.dataload.dumper.DumperManager instance
    Run all dumpers, except manual ones



.. py:method:: upload(src, *args, **kwargs)

method upload_src in module biothings.hub.dataload.uploader

upload_src(src, *args, **kwargs) method of biothings.hub.dataload.uploader.UploaderManager instance
    Trigger upload for registered resource named 'src'.
    Other args are passed to uploader's load() method



.. py:method:: upload_all(raise_on_error=False, **kwargs)

method upload_all in module biothings.hub.dataload.uploader

upload_all(raise_on_error=False, **kwargs) method of biothings.hub.dataload.uploader.UploaderManager instance
    Trigger upload processes for all registered resources.
    `**kwargs` are passed to upload_src() method



.. py:method:: whatsnew(build_name=None, old=None)

method whatsnew in module biothings.hub.databuild.builder

whatsnew(build_name=None, old=None) method of biothings.hub.databuild.builder.BuilderManager instance
    Return datasources which have changed since last time
    (last time is datasource information from metadata, either from
    given old src_build doc name, or the latest found if old=None)



.. py:method:: lsmerge(build_config=None, only_archived=False)

method list_merge in module biothings.hub.databuild.builder

list_merge(build_config=None, only_archived=False) method of biothings.hub.databuild.builder.BuilderManager instance



.. py:method:: rmmerge(merge_name)

method delete_merge in module biothings.hub.databuild.builder

delete_merge(merge_name) method of biothings.hub.databuild.builder.BuilderManager instance
    Delete merged collections and associated metadata



.. py:method:: merge(build_name, sources=None, target_name=None, **kwargs)

method merge in module biothings.hub.databuild.builder

merge(build_name, sources=None, target_name=None, **kwargs) method of biothings.hub.databuild.builder.BuilderManager instance
    Trigger a merge for build named 'build_name'. Optional list of sources can be
    passed (one single or a list). target_name is the target collection name used
    to store to merge data. If none, each call will generate a unique target_name.



.. py:method:: archive(merge_name)

method archive_merge in module biothings.hub.databuild.builder

archive_merge(merge_name) method of biothings.hub.databuild.builder.BuilderManager instance
    Delete merged collections and associated metadata



.. py:data:: index_config




.. py:data:: snapshot_config




.. py:method:: diff(diff_type, old, new, batch_size=100000, steps=['content', 'mapping', 'reduce', 'post'], mode=None, exclude=['_timestamp'])

method diff in module biothings.hub.databuild.differ

diff(diff_type, old, new, batch_size=100000, steps=['content', 'mapping', 'reduce', 'post'], mode=None, exclude=['_timestamp']) method of biothings.hub.databuild.differ.DifferManager instance
    Run a diff to compare old vs. new collections. using differ algorithm diff_type. Results are stored in
    a diff folder.
    Steps can be passed to choose what to do:
    - count: will count root keys in new collections and stores them as statistics.
    - content: will diff the content between old and new. Results (diff files) format depends on diff_type



.. py:method:: report(old_db_col_names, new_db_col_names, report_filename='report.txt', format='txt', detailed=True, max_reported_ids=None, max_randomly_picked=None, mode=None)

method diff_report in module biothings.hub.databuild.differ

diff_report(old_db_col_names, new_db_col_names, report_filename='report.txt', format='txt', detailed=True, max_reported_ids=None, max_randomly_picked=None, mode=None) method of biothings.hub.databuild.differ.DifferManager instance



.. py:method:: index(indexer_env, build_name, index_name=None, ids=None, **kwargs)

method index in module biothings.hub.dataindex.indexer

index(indexer_env, build_name, index_name=None, ids=None, **kwargs) method of biothings.hub.dataindex.indexer.IndexManager instance
    Trigger an index creation to index the collection build_name and create an
    index named index_name (or build_name if None). Optional list of IDs can be
    passed to index specific documents.



.. py:method:: index_cleanup(env=None, keep=3, dryrun=True, **filters)

method cleanup in module biothings.hub.dataindex.indexer

cleanup(env=None, keep=3, dryrun=True, **filters) method of biothings.hub.dataindex.indexer.IndexManager instance
    Delete old indices except for the most recent ones.
    
    Examples:
        >>> index_cleanup()
        >>> index_cleanup("production")
        >>> index_cleanup("local", build_config="demo")
        >>> index_cleanup("local", keep=0)
        >>> index_cleanup(_id="<elasticsearch_index>")



.. py:method:: snapshot(snapshot_env, index, snapshot=None)

method snapshot in module biothings.hub.dataindex.snapshooter

snapshot(snapshot_env, index, snapshot=None) method of biothings.hub.dataindex.snapshooter.SnapshotManager instance
    Create a snapshot named "snapshot" (or, by default, same name as the index)
    from "index" according to environment definition (repository, etc...) "env".



.. py:method:: snapshot_cleanup(env=None, keep=3, group_by='build_config', dryrun=True, **filters)

method cleanup in module biothings.hub.dataindex.snapshooter

cleanup(env=None, keep=3, group_by='build_config', dryrun=True, **filters) method of biothings.hub.dataindex.snapshooter.SnapshotManager instance
    Delete past snapshots and keep only the most recent ones.
    
    Examples:
        >>> snapshot_cleanup()
        >>> snapshot_cleanup("s3_outbreak")
        >>> snapshot_cleanup("s3_outbreak", keep=0)



.. py:method:: create_release_note(old, new, filename=None, note=None, format='txt')

method create_release_note in module biothings.hub.datarelease.publisher

create_release_note(old, new, filename=None, note=None, format='txt') method of biothings.hub.datarelease.publisher.ReleaseManager instance
    Generate release note files, in TXT and JSON format, containing significant changes
    summary between target collections old and new. Output files
    are stored in a diff folder using generate_folder(old,new).
    
    'filename' can optionally be specified, though it's not recommended as the publishing pipeline,
    using these files, expects a filenaming convention.
    
    'note' is an optional free text that can be added to the release note, at the end.
    
    txt 'format' is the only one supported for now.



.. py:method:: get_release_note(old, new, format='txt', prefix='release_*')

method get_release_note in module biothings.hub.datarelease.publisher

get_release_note(old, new, format='txt', prefix='release_*') method of biothings.hub.datarelease.publisher.ReleaseManager instance



.. py:method:: publish(publisher_env, snapshot_or_build_name, *args, **kwargs)

method publish in module biothings.hub.datarelease.publisher

publish(publisher_env, snapshot_or_build_name, *args, **kwargs) method of biothings.hub.datarelease.publisher.ReleaseManager instance



.. py:method:: publish_diff(publisher_env, build_name, previous_build=None, steps=['pre', 'reset', 'upload', 'meta', 'post'])

method publish_diff in module biothings.hub.datarelease.publisher

publish_diff(publisher_env, build_name, previous_build=None, steps=['pre', 'reset', 'upload', 'meta', 'post']) method of biothings.hub.datarelease.publisher.ReleaseManager instance



.. py:method:: publish_snapshot(publisher_env, snapshot, build_name=None, previous_build=None, steps=['pre', 'meta', 'post'])

method publish_snapshot in module biothings.hub.datarelease.publisher

publish_snapshot(publisher_env, snapshot, build_name=None, previous_build=None, steps=['pre', 'meta', 'post']) method of biothings.hub.datarelease.publisher.ReleaseManager instance



.. py:method:: sync(backend_type, old_db_col_names, new_db_col_names, diff_folder=None, batch_size=10000, mode=None, target_backend=None, steps=['mapping', 'content', 'meta', 'post'], debug=False)

method sync in module biothings.hub.databuild.syncer

sync(backend_type, old_db_col_names, new_db_col_names, diff_folder=None, batch_size=10000, mode=None, target_backend=None, steps=['mapping', 'content', 'meta', 'post'], debug=False) method of biothings.hub.databuild.syncer.SyncerManager instance



.. py:method:: inspect(data_provider, mode='type', batch_size=10000, limit=None, sample=None, **kwargs)

method inspect in module biothings.hub.datainspect.inspector

inspect(data_provider, mode='type', batch_size=10000, limit=None, sample=None, **kwargs) method of biothings.hub.datainspect.inspector.InspectorManager instance
    Inspect given data provider:
    - backend definition, see bt.hub.dababuild.create_backend for
      supported format), eg "merged_collection" or ("src","clinvar")
    - or callable yielding documents
    Mode:
    - "type": will inspect and report type map found in data (internal/non-standard format)
    - "mapping": will inspect and return a map compatible for later
      ElasticSearch mapping generation (see bt.utils.es.generate_es_mapping)
    - "stats": will inspect and report types + different counts found in
      data, giving a detailed overview of the volumetry of each fields and sub-fields
    - "jsonschema", same as "type" but result is formatted as json-schema standard
    - limit: when set to an integer, will inspect only x documents.
    - sample: combined with limit, for each document, if random.random() <= sample (float),
      the document is inspected. This option allows to inspect only a sample of data.



.. py:method:: register_url(url)

method register_url in module biothings.hub.dataplugin.assistant

register_url(url) method of biothings.hub.dataplugin.assistant.AssistantManager instance



.. py:method:: unregister_url(url=None, name=None)

method unregister_url in module biothings.hub.dataplugin.assistant

unregister_url(url=None, name=None) method of biothings.hub.dataplugin.assistant.AssistantManager instance



.. py:method:: export_plugin(plugin_name, folder=None, what=['dumper', 'uploader', 'mapping'], purge=False)

method export in module biothings.hub.dataplugin.assistant

export(plugin_name, folder=None, what=['dumper', 'uploader', 'mapping'], purge=False) method of biothings.hub.dataplugin.assistant.AssistantManager instance
    Export generated code for a given plugin name, in given folder
    (or use DEFAULT_EXPORT_FOLDER if None). Exported information can be:
    - dumper: dumper class generated from the manifest
    - uploader: uploader class generated from the manifest
    - mapping: mapping generated from inspection or from the manifest
    If "purge" is true, any existing folder/code will be deleted first, otherwise,
    will raise an error if some folder/files already exist.



.. py:method:: dump_plugin(src, force=False, skip_manual=False, schedule=False, check_only=False, **kwargs)

method dump_src in module biothings.hub.dataload.dumper

dump_src(src, force=False, skip_manual=False, schedule=False, check_only=False, **kwargs) method of biothings.hub.dataplugin.manager.DataPluginManager instance



.. py:method:: list()

method list_biothings in module biothings.hub.standalone

list_biothings() method of biothings.hub.standalone.AutoHubFeature instance
    Example:
    [{'name': 'mygene.info',
    'url': 'https://biothings-releases.s3-us-west-2.amazonaws.com/mygene.info/versions.json'}]



.. py:method:: versions(src, method_name, *args, **kwargs)

method call in module biothings.hub.dataload.dumper

call(src, method_name, *args, **kwargs) method of biothings.hub.dataload.dumper.DumperManager instance
    Create a dumper for datasource "src" and call method "method_name" on it,
    with given arguments. Used to create arbitrary calls on a dumper.
    "method_name" within dumper definition must a coroutine.



.. py:method:: check(src, force=False, skip_manual=False, schedule=False, check_only=False, **kwargs)

method dump_src in module biothings.hub.dataload.dumper

dump_src(src, force=False, skip_manual=False, schedule=False, check_only=False, **kwargs) method of biothings.hub.dataload.dumper.DumperManager instance



.. py:method:: info(src, method_name, *args, **kwargs)

method call in module biothings.hub.dataload.dumper

call(src, method_name, *args, **kwargs) method of biothings.hub.dataload.dumper.DumperManager instance
    Create a dumper for datasource "src" and call method "method_name" on it,
    with given arguments. Used to create arbitrary calls on a dumper.
    "method_name" within dumper definition must a coroutine.



.. py:method:: download(src, force=False, skip_manual=False, schedule=False, check_only=False, **kwargs)

method dump_src in module biothings.hub.dataload.dumper

dump_src(src, force=False, skip_manual=False, schedule=False, check_only=False, **kwargs) method of biothings.hub.dataload.dumper.DumperManager instance



.. py:method:: apply(src, *args, **kwargs)

method upload_src in module biothings.hub.dataload.uploader

upload_src(src, *args, **kwargs) method of biothings.hub.dataload.uploader.UploaderManager instance
    Trigger upload for registered resource named 'src'.
    Other args are passed to uploader's load() method



.. py:method:: install(src_name, version='latest', dry=False, force=False)

method install in module biothings.hub.standalone

install(src_name, version='latest', dry=False, force=False) method of biothings.hub.standalone.AutoHubFeature instance
    Update hub's data up to the given version (default is latest available),
    using full and incremental updates to get up to that given version (if possible).



.. py:method:: backend(src, method_name, *args, **kwargs)

method call in module biothings.hub.dataload.dumper

call(src, method_name, *args, **kwargs) method of biothings.hub.dataload.dumper.DumperManager instance
    Create a dumper for datasource "src" and call method "method_name" on it,
    with given arguments. Used to create arbitrary calls on a dumper.
    "method_name" within dumper definition must a coroutine.



.. py:method:: reset_backend(src, method_name, *args, **kwargs)

method call in module biothings.hub.dataload.dumper

call(src, method_name, *args, **kwargs) method of biothings.hub.dataload.dumper.DumperManager instance
    Create a dumper for datasource "src" and call method "method_name" on it,
    with given arguments. Used to create arbitrary calls on a dumper.
    "method_name" within dumper definition must a coroutine.



.. py:method:: upgrade(code_base)

function upgrade in module biothings.hub

upgrade(code_base)
    Upgrade (git pull) repository for given code base name ("biothings_sdk" or "application")



.. py:method:: restart(force=False, stop=False)

method restart in module biothings.utils.hub

restart(force=False, stop=False) method of biothings.utils.hub.HubShell instance



.. py:method:: stop(force=False)

method stop in module biothings.utils.hub

stop(force=False) method of biothings.utils.hub.HubShell instance



.. py:method:: backup(folder='.', archive=None)

function backup in module biothings.utils.hub_db

backup(folder='.', archive=None)
    Dump the whole hub_db database in given folder. "archive" can be pass
    to specify the target filename, otherwise, it's randomly generated
    
    .. note:: this doesn't backup source/merge data, just the internal data
             used by the hub



.. py:method:: restore(archive, drop=False)

function restore in module biothings.utils.hub_db

restore(archive, drop=False)
    Restore database from given archive. If drop is True, then delete existing collections



.. py:method:: help(func=None)

method help in module biothings.utils.hub

help(func=None) method of biothings.utils.hub.HubShell instance
    Display help on given function/object or list all available commands



.. py:method:: commands(id=None, running=None, failed=None)

method command_info in module biothings.utils.hub

command_info(id=None, running=None, failed=None) method of traitlets.traitlets.MetaHasTraits instance



.. py:method:: command(id, *args, **kwargs)

function <lambda> in module biothings.utils.hub

<lambda> lambda id, *args, **kwargs



.. py:data:: g

This is a instance of type: <class 'dict'>


.. py:method:: sch(loop)

function get_schedule in module biothings.hub

get_schedule(loop)
    try to render job in a human-readable way...



.. py:data:: pending

This is a instance of type: <class 'str'>


.. py:data:: loop

This is a instance of type: <class 'asyncio.unix_events._UnixSelectorEventLoop'>


.. py:data:: pqueue

This is a instance of type: <class 'concurrent.futures.process.ProcessPoolExecutor'>


.. py:data:: tqueue

This is a instance of type: <class 'concurrent.futures.thread.ThreadPoolExecutor'>


.. py:data:: jm

This is a instance of type: <class 'biothings.utils.manager.JobManager'>


.. py:method:: top(action='summary')

method top in module biothings.utils.manager

top(action='summary') method of biothings.utils.manager.JobManager instance



.. py:method:: job_info()

method job_info in module biothings.utils.manager

job_info() method of biothings.utils.manager.JobManager instance



.. py:method:: schedule(crontab, func, *args, **kwargs)

method schedule in module biothings.utils.manager

schedule(crontab, func, *args, **kwargs) method of biothings.utils.manager.JobManager instance
    Helper to create a cron job from a callable "func". *argd, and **kwargs
    are passed to func. "crontab" follows aicron notation.



.. py:data:: sm

This is a instance of type: <class 'biothings.hub.dataload.source.SourceManager'>


.. py:method:: sources(id=None, debug=False, detailed=False)

method get_sources in module biothings.hub.dataload.source

get_sources(id=None, debug=False, detailed=False) method of biothings.hub.dataload.source.SourceManager instance



.. py:method:: source_save_mapping(name, mapping=None, dest='master', mode='mapping')

method save_mapping in module biothings.hub.dataload.source

save_mapping(name, mapping=None, dest='master', mode='mapping') method of biothings.hub.dataload.source.SourceManager instance



.. py:data:: dm

This is a instance of type: <class 'biothings.hub.dataload.dumper.DumperManager'>


.. py:method:: dump_info()

method dump_info in module biothings.hub.dataload.dumper

dump_info() method of biothings.hub.dataload.dumper.DumperManager instance



.. py:data:: dpm

This is a instance of type: <class 'biothings.hub.dataplugin.manager.DataPluginManager'>


.. py:data:: am

This is a instance of type: <class 'biothings.hub.dataplugin.assistant.AssistantManager'>


.. py:data:: um

This is a instance of type: <class 'biothings.hub.dataload.uploader.UploaderManager'>


.. py:method:: upload_info()

method upload_info in module biothings.hub.dataload.uploader

upload_info() method of biothings.hub.dataload.uploader.UploaderManager instance



.. py:data:: bm

This is a instance of type: <class 'biothings.hub.databuild.builder.BuilderManager'>


.. py:method:: builds(id=None, conf_name=None, fields=None, only_archived=False)

method build_info in module biothings.hub.databuild.builder

build_info(id=None, conf_name=None, fields=None, only_archived=False) method of biothings.hub.databuild.builder.BuilderManager instance
    Return build information given an build _id, or all builds
    if _id is None. "fields" can be passed to select which fields
    to return or not (mongo notation for projections), if None
    return everything except:
     - "mapping" (too long)
    If id is None, more are filtered:
     - "sources" and some of "build_config"
    only_archived=True will return archived merges only



.. py:method:: build(id)

function <lambda> in module biothings.hub

<lambda> lambda id



.. py:method:: build_config_info()

method build_config_info in module biothings.hub.databuild.builder

build_config_info() method of biothings.hub.databuild.builder.BuilderManager instance



.. py:method:: build_save_mapping(name, mapping=None, dest='build', mode='mapping')

method save_mapping in module biothings.hub.databuild.builder

save_mapping(name, mapping=None, dest='build', mode='mapping') method of biothings.hub.databuild.builder.BuilderManager instance



.. py:method:: create_build_conf(name, doc_type, sources, roots=[], builder_class=None, params={}, archived=False)

method create_build_configuration in module biothings.hub.databuild.builder

create_build_configuration(name, doc_type, sources, roots=[], builder_class=None, params={}, archived=False) method of biothings.hub.databuild.builder.BuilderManager instance



.. py:method:: update_build_conf(name, doc_type, sources, roots=[], builder_class=None, params={}, archived=False)

method update_build_configuration in module biothings.hub.databuild.builder

update_build_configuration(name, doc_type, sources, roots=[], builder_class=None, params={}, archived=False) method of biothings.hub.databuild.builder.BuilderManager instance



.. py:method:: delete_build_conf(name)

method delete_build_configuration in module biothings.hub.databuild.builder

delete_build_configuration(name) method of biothings.hub.databuild.builder.BuilderManager instance



.. py:data:: dim

This is a instance of type: <class 'biothings.hub.databuild.differ.DifferManager'>


.. py:method:: diff_info()

method diff_info in module biothings.hub.databuild.differ

diff_info() method of biothings.hub.databuild.differ.DifferManager instance



.. py:method:: jsondiff(src, dst, **kwargs)

function make in module biothings.utils.jsondiff

make(src, dst, **kwargs)



.. py:data:: sym

This is a instance of type: <class 'biothings.hub.databuild.syncer.SyncerManager'>


.. py:data:: im

This is a instance of type: <class 'biothings.hub.dataindex.indexer.IndexManager'>


.. py:method:: index_info(remote=False)

method index_info in module biothings.hub.dataindex.indexer

index_info(remote=False) method of biothings.hub.dataindex.indexer.IndexManager instance
    Show index manager config with enhanced index information.



.. py:method:: validate_mapping(mapping, env)

method validate_mapping in module biothings.hub.dataindex.indexer

validate_mapping(mapping, env) method of biothings.hub.dataindex.indexer.IndexManager instance



.. py:method:: update_metadata(indexer_env, index_name, build_name=None, _meta=None)

method update_metadata in module biothings.hub.dataindex.indexer

update_metadata(indexer_env, index_name, build_name=None, _meta=None) method of biothings.hub.dataindex.indexer.IndexManager instance
    Update _meta field of the index mappings, basing on
        1. the _meta value provided, including {}.
        2. the _meta value of the build_name in src_build.
        3. the _meta value of the build with the same name as the index.
    
    Examples:
        update_metadata("local", "mynews_201228_vsdevjd")
        update_metadata("local", "mynews_201228_vsdevjd", _meta={})
        update_metadata("local", "mynews_201228_vsdevjd", _meta={"author":"b"})
        update_metadata("local", "mynews_201228_current", "mynews_201228_vsdevjd")



.. py:data:: ssm

This is a instance of type: <class 'biothings.hub.dataindex.snapshooter.SnapshotManager'>


.. py:method:: snapshot_info(env=None, remote=False)

method snapshot_info in module biothings.hub.dataindex.snapshooter

snapshot_info(env=None, remote=False) method of biothings.hub.dataindex.snapshooter.SnapshotManager instance



.. py:data:: rm

This is a instance of type: <class 'biothings.hub.datarelease.publisher.ReleaseManager'>


.. py:method:: release_info(env=None, remote=False)

method release_info in module biothings.hub.datarelease.publisher

release_info(env=None, remote=False) method of biothings.hub.datarelease.publisher.ReleaseManager instance



.. py:method:: reset_synced(old, new)

method reset_synced in module biothings.hub.datarelease.publisher

reset_synced(old, new) method of biothings.hub.datarelease.publisher.ReleaseManager instance
    Reset sync flags for diff files produced between "old" and "new" build.
    Once a diff has been applied, diff files are flagged as synced so subsequent diff
    won't be applied twice (for optimization reasons, not to avoid data corruption since
    diff files can be safely applied multiple times).
    In any needs to apply the diff another time, diff files needs to reset.



.. py:data:: ism

This is a instance of type: <class 'biothings.hub.datainspect.inspector.InspectorManager'>


.. py:data:: api

This is a instance of type: <class 'biothings.hub.api.manager.APIManager'>


.. py:method:: get_apis()

method get_apis in module biothings.hub.api.manager

get_apis() method of biothings.hub.api.manager.APIManager instance



.. py:method:: delete_api(api_id)

method delete_api in module biothings.hub.api.manager

delete_api(api_id) method of biothings.hub.api.manager.APIManager instance



.. py:method:: create_api(api_id, es_host, index, doc_type, port, description=None, **kwargs)

method create_api in module biothings.hub.api.manager

create_api(api_id, es_host, index, doc_type, port, description=None, **kwargs) method of biothings.hub.api.manager.APIManager instance



.. py:method:: start_api(api_id)

method start_api in module biothings.hub.api.manager

start_api(api_id) method of biothings.hub.api.manager.APIManager instance



.. py:method:: stop_api(api_id)

method stop_api in module biothings.hub.api.manager

stop_api(api_id) method of biothings.hub.api.manager.APIManager instance



.. py:method:: expose(endpoint_name, command_name, method, **kwargs)

method add_api_endpoint in module biothings.hub

add_api_endpoint(endpoint_name, command_name, method, **kwargs) method of biothings.hub.HubServer instance
    Add an API endpoint to expose command named "command_name"
    using HTTP method "method". **kwargs are used to specify
    more arguments for EndpointDefinition

