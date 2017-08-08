#!/usr/bin/env python

import asyncio, asyncssh, sys
import concurrent.futures
from functools import partial

import config, biothings
biothings.config_for_app(config)

import logging
# shut some mouths...
logging.getLogger("elasticsearch").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)

logging.info("Hub DB backend: %s" % biothings.config.HUB_DB_BACKEND)
logging.info("Hub database: %s" % biothings.config.DATA_HUB_DB_DATABASE)

from biothings.utils.manager import JobManager
loop = asyncio.get_event_loop()
process_queue = concurrent.futures.ProcessPoolExecutor(max_workers=config.HUB_MAX_WORKERS)
thread_queue = concurrent.futures.ThreadPoolExecutor()
loop.set_default_executor(process_queue)
jmanager = JobManager(loop,
                      process_queue, thread_queue,
                      max_memory_usage=None,
                      )

import biothings.hub.dataload.uploader as uploader
import biothings.hub.dataload.dumper as dumper
import biothings.hub.databuild.builder as builder
import biothings.hub.databuild.differ as differ
import biothings.hub.databuild.syncer as syncer
import biothings.hub.dataindex.indexer as indexer

differ_manager = differ.DifferManager(job_manager=jmanager)
differ_manager.configure()
syncer_manager = syncer.SyncerManager(job_manager=jmanager)
syncer_manager.configure()
import biothings.hub.dataindex.indexer as indexer
pindexer = partial(indexer.Indexer,es_host=config.ES_HOST)
index_manager = indexer.IndexerManager(pindexer=pindexer,job_manager=jmanager)
index_manager.configure()

dmanager = dumper.DumperManager(job_manager=jmanager)
dmanager.schedule_all()
# manually register biothings source dumper
# this dumper will download whatever is necessary to update an ES index
from biothings.hub.autoupdate import BiothingsDumper
from biothings.utils.es import ESIndexer
from biothings.utils.backend import DocESBackend
BiothingsDumper.BIOTHINGS_APP = "t.biothings.io"
pidxr = partial(ESIndexer,index=config.ES_INDEX_NAME,doc_type=config.ES_DOC_TYPE,es_host=config.ES_HOST)
partial_backend = partial(DocESBackend,pidxr)
BiothingsDumper.TARGET_BACKEND = partial_backend
dmanager.register_classes([BiothingsDumper])

# will check every 10 seconds for sources to upload
umanager = uploader.UploaderManager(poll_schedule = '* * * * * */10', job_manager=jmanager)
# manually register biothings source uploader
# this uploader will use dumped data to update an ES index
from biothings.hub.autoupdate import BiothingsUploader
BiothingsUploader.TARGET_BACKEND = partial_backend
# syncer will work on index used in web part
partial_syncer = partial(syncer_manager.sync,"es",target_backend=config.ES_BACKEND)
BiothingsUploader.SYNCER_FUNC = partial_syncer
BiothingsUploader.AUTO_PURGE_INDEX = True # because we believe
umanager.register_classes([BiothingsUploader])
umanager.poll()

from biothings.utils.hub import schedule, top, pending, done, HubCommand

COMMANDS = {
        # dump commands
        "check" : partial(dmanager.dump_src,"biothings",check_only=True),
        "download" : partial(dmanager.dump_src,"biothings"),
        # upload commands
        "apply" : partial(umanager.upload_src,"biothings"),
        "update": HubCommand("download() && apply()"),
        ## admin/advanced
        "g": globals(),
        "sch" : partial(schedule,loop),
        "top" : partial(top,process_queue,thread_queue),
        "pending" : pending,
        "done" : done,
        }

passwords = hasattr(config,"HUB_ACCOUNTS") and config.HUB_ACCOUNTS or {
        'guest': '', # guest account with no password
        }

from biothings.utils.hub import start_server

server = start_server(loop, "Auto-hub",passwords=passwords,
        port=config.HUB_SSH_PORT,commands=COMMANDS)

try:
    loop.run_until_complete(server)
except (OSError, asyncssh.Error) as exc:
    sys.exit('Error starting server: ' + str(exc))

loop.run_forever()

