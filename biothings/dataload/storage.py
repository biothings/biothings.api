import types, copy, datetime, time

import asyncio
from pymongo.errors import DuplicateKeyError, BulkWriteError


class BaseStorage(object):

    def __init__(self,collection,logger):
        self.temp_collection = collection
        self.logger = logger

    def process(self,iterable,*args,**kwargs):
        """
        Process iterable to store data
        """
        raise NotImplementedError("implement-me in subclass")

class BasicStorage(BaseStorage):

    def doc_iterator(self, doc_d, batch=True, step=10000):
        if isinstance(doc_d, types.GeneratorType) and batch:
            for doc_li in iter_n(doc_d, n=step):
                yield doc_li
        else:
            if batch:
                doc_li = []
                i = 0
            for _id, doc in doc_d.items():
                doc['_id'] = _id
                _doc = {}
                #_doc.clear()
                _doc.update(doc)
                #if validate:
                #    _doc.validate()
                if batch:
                    doc_li.append(_doc)
                    i += 1
                    if i % step == 0:
                        yield doc_li
                        doc_li = []
                else:
                    yield _doc

            if batch:
                yield doc_li

class MergerStorage(BasicStorage):
    """
    This storage will try to merge documents when finding duplicated errors.
    It's useful when data is parsed using iterator. A record can be stored in database,
    then later, another record with the same ID is sent to the db, raising a duplicated error.
    These two documents would have been merged before using a 'put all in memory' parser. 
    Since data is here read line by line, the merge is done while storing
    """

    def process(self, doc_d, step):
        self.logger.info("Uploading to the DB...")
        t0 = time.time()
        tinner = time.time()
        aslistofdict = None
        for doc_li in self.doc_iterator(doc_d, batch=True, step=step):
            toinsert = len(doc_li)
            nbinsert = 0
            self.logger.info("Inserting %s records ... " % toinsert)
            try:
                bob = self.temp_collection.initialize_unordered_bulk_op()
                for d in doc_li:
                    aslistofdict = d.pop("__aslistofdict__",None)
                    bob.insert(d)
                res = bob.execute()
                nbinsert += res["nInserted"]
                self.logger.info("OK [%s]" % timesofar(tinner))
            except BulkWriteError as e:
                inserted = e.details["nInserted"]
                nbinsert += inserted
                self.logger.info("Fixing %d records " % len(e.details["writeErrors"]))
                ids = [d["op"]["_id"] for d in e.details["writeErrors"]]
                # build hash of existing docs
                docs = self.temp_collection.find({"_id" : {"$in" : ids}})
                hdocs = {}
                for doc in docs:
                    hdocs[doc["_id"]] = doc
                bob2 = self.temp_collection.initialize_unordered_bulk_op()
                for err in e.details["writeErrors"]:
                    errdoc = err["op"]
                    existing = hdocs[errdoc["_id"]]
                    assert "_id" in existing
                    _id = errdoc.pop("_id")
                    merged = merge_struct(errdoc, existing,aslistofdict=aslistofdict)
                    bob2.find({"_id" : _id}).update_one({"$set" : merged})
                    # update previously fetched doc. if several errors are about the same doc id,
                    # we would't merged things properly without an updated document
                    assert "_id" in merged
                    hdocs[_id] = merged
                    nbinsert += 1

                res = bob2.execute()
                self.logger.info("OK [%s]" % timesofar(tinner))
            assert nbinsert == toinsert, "nb %s to %s" % (nbinsert,toinsert)
            # end of loop so it counts the time spent in doc_iterator
            tinner = time.time()

        self.logger.info('Done[%s]' % timesofar(t0))
        self.switch_collection()
        self.post_update_data()


class IgnoreDuplicatedStorage(BasicStorage):

    def process(self,iterable,step):
        self.logger.info("Uploading to the DB...")
        t0 = time.time()
        tinner = time.time()
        for doc_li in self.doc_iterator(iterable, batch=True, step=step):
            try:
                bob = self.temp_collection.initialize_unordered_bulk_op()
                for d in doc_li:
                    bob.insert(d)
                res = bob.execute()
                self.logger.info("Inserted %s records [%s]" % (res['nInserted'],timesofar(tinner)))
            except BulkWriteError as e:
                self.logger.info("Inserted %s records, ignoring %d [%s]" % (e.details['nInserted'],len(e.details["writeErrors"]),timesofar(tinner)))
            tinner = time.time()
        self.logger.info('Done[%s]' % timesofar(t0))

class NoBatchIgnoreDuplicatedStorage(BasicStorage):
    """
    You should use IgnoreDuplicatedStorag, which works using batch
    and is thus way faster...
    """

    def process(self, doc_d, step):
        self.logger.info("Uploading to the DB...")
        t0 = time.time()
        tinner = time.time()
        # force step = 1
        cnt = 0
        dups = 0
        for doc_li in self.doc_iterator(doc_d, batch=True, step=1):
            try:
                res = self.temp_collection.insert(doc_li, manipulate=False, check_keys=False)
                cnt += 1
                if (cnt + dups) % step == 0:
                    # we insert one by one but display progress on a "step" base
                    self.logger.info("Inserted %s records, ignoring %s [%s]" % (cnt,dups,timesofar(tinner)))
                    cnt = 0
                    dups = 0
                    tinner = time.time()
            except DuplicateKeyError:
                dups += 1
                pass
        self.logger.info('Done[%s]' % timesofar(t0))
        self.switch_collection()
        self.post_update_data()


class AsyncIgnoreDuplicatedStorage(IgnoreDuplicatedStorage):

    def __init__(self,*args,**kwargs):
        super(AsyncIgnoreDuplicatedStorage,self).__init__(*args,**kwargs)
        self.loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def process(self,queue,step):
        self.logger.info("Uploading to the DB...")
        t0 = time.time()
        while True:
            tinner = time.time()
            try:
                bob = self.temp_collection.initialize_unordered_bulk_op()
                for i in range(step):
                    #print("on get")
                    doc = yield from queue.get()
                    #print("qsize: %s" % queue.qsize())
                    #yield from self.loop.run_in_executor(None,bob.insert,doc)
                    #print("doc: %s" % doc)
                    bob.insert(doc)
                    #yield from asyncio.sleep(0)
                res = yield from self.loop.run_in_executor(None,bob.execute)
                #res = bob.execute()
                howlong = yield from self.loop.run_in_executor(None,timesofar,tinner)
                yield from self.loop.run_in_executor(None,self.logger.info,"Inserted %s records [%s]" % (res['nInserted'],howlong))
                #yield from loop.run_in_executor(None,logging.info,"Inserted %s records [%s]" % (res['nInserted'],howlong))
                #self.logger.info("Inserted %s records [%s]" % (res['nInserted'],howlong))
                yield from asyncio.sleep(0)
            except BulkWriteError as e:
                howlong = yield from self.loop.run_in_executor(None,timesofar,tinner)
                self.logger.info("Inserted %s records, ignoring %d [%s]" % (e.details['nInserted'],len(e.details["writeErrors"]),howlong))

