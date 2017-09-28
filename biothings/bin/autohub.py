#!/usr/bin/env python

import asyncio, asyncssh, sys
import concurrent.futures
from functools import partial
from collections import OrderedDict

import config, biothings
biothings.config_for_app(config)

if not hasattr(config,"ES_BACKEND") or len(config.ES_BACKEND) != 3:
    raise Exception("Config file must declare ES_BACKEND as tuple(es_host,es_index,es_doctype)")

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
import biothings.hub.databuild.syncer as syncer
import biothings.hub.dataindex.indexer as indexer

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
from biothings.hub.autoupdate.dumper import LATEST
from biothings.utils.es import ESIndexer
from biothings.utils.backend import DocESBackend
BiothingsDumper.BIOTHINGS_APP = config.BIOTHINGS_APP
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
umanager.poll('upload',lambda doc: upload_manager.upload_src(doc["_id"]))

from biothings.utils.hub import schedule, pending, done, HubCommand


def cycle_update(version=LATEST, max_cycles=10):
    """
    Update hub's data up to the given version (default is latest available),
    using full and incremental updates to get up to that given version (if possible).
    To prevent any infinite loop that could occur (eg. network issues), a max of
    max_cycles will be considered to bring the hub up-to-date.
    """
    @asyncio.coroutine
    def do(version):
        cycle = True
        count = 0
        while cycle:
            jobs = dmanager.dump_src("biothings",version=version,check_only=True)
            check = asyncio.gather(*jobs)
            res = yield from check
            assert len(res) == 1
            if res[0] == "Nothing to dump":
                cycle = False
            else:
                remote_version = res[0]
                jobs = dmanager.dump_src("biothings",version=remote_version)
                download = asyncio.gather(*jobs)
                res = yield from download
                assert len(res) == 1
                if res[0] == None:
                    # download ready, now update
                    jobs = umanager.upload_src("biothings")
                    upload = asyncio.gather(*jobs)
                    res = yield from upload
                else:
                    assert res[0] == "Nothing to dump"
                    cycle = False
            count += 1
            if count >= max_cycles:
                logging.warning("Reach max updating cycle (%s), now aborting process" % count)
                cycle = False

    return asyncio.ensure_future(do(version))


COMMANDS = OrderedDict()
# dump commands
COMMANDS["versions"] = partial(dmanager.call,"biothings","versions")
COMMANDS["check"] = partial(dmanager.dump_src,"biothings",check_only=True)
COMMANDS["info"] = partial(dmanager.call,"biothings","info")
COMMANDS["download"] = partial(dmanager.dump_src,"biothings")
# upload commands
COMMANDS["apply"] = partial(umanager.upload_src,"biothings")
COMMANDS["step_update"] = HubCommand("download() && apply()")
COMMANDS["update"] = cycle_update

# admin/advanced
EXTRA_NS = {
    "dm" : dmanager,
    "um" : umanager,
    "jm" : jmanager,
    "q" : jmanager.process_queue,
    "t": jmanager.thread_queue,
    "g" : globals(),
    "l" : loop,
    "sch" : partial(schedule,loop),
    "top" : jmanager.top,
    "pending" : pending,
    "done" : done
    }

passwords = hasattr(config,"HUB_ACCOUNTS") and config.HUB_ACCOUNTS or {
        'guest': '', # guest account with no password
        }

from biothings.utils.hub import start_server

server = start_server(loop, "Auto-hub",passwords=passwords,
        port=config.HUB_SSH_PORT,commands=COMMANDS,extra_ns=EXTRA_NS)

try:
    loop.run_until_complete(server)
except (OSError, asyncssh.Error) as exc:
    sys.exit('Error starting server: ' + str(exc))

loop.run_forever()

