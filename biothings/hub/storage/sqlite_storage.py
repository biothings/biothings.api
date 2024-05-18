import abc
import copy
import logging
import time
import types

from sqlite3 import IntegrityError

from biothings.utils.common import iter_n, timesofar
from biothings.utils.dataload import merge_root_keys, merge_struct
from biothings.utils.storage import BasicStorage


class MergerStorage(BasicStorage):
    """
    This storage will try to merge documents when finding duplicated errors.
    It's useful when data is parsed using iterator. A record can be stored in database,
    then later, another record with the same ID is sent to the db, raising a duplicated error.
    These two documents would have been merged before using a 'put all in memory' parser.
    Since data is here read line by line, the merge is done while storing
    """

    merge_func = merge_struct

    def process(self, doc_d, batch_size: int, max_batch_num: int=None):
        self.logger.info("Uploading to the DB...")
        t0 = time.time()
        tinner = time.time()
        aslistofdict = None
        total = 0
        batch_num = 0
        for doc_li in self.doc_iterator(doc_d, batch=True, batch_size=batch_size):
            if max_batch_num and batch_num >= max_batch_num:
                break
            batch_num += 1
            toinsert = len(doc_li)
            nbinsert = 0
            self.logger.info("Inserting %s records ... " % toinsert)
            try:
                bulk = []
                for d in doc_li:
                    aslistofdict = d.pop("__aslistofdict__", None)
                    bulk.append(InsertOne(d))

                res = self.temp_collection.bulk_write(bulk, ordered=False)
                nbinsert += res.inserted_count
                self.logger.info("OK [%s]" % timesofar(tinner))
            except BulkWriteError as e:
                inserted = e.details["nInserted"]
                nbinsert += inserted
                self.logger.info("Fixing %d records " % len(e.details["writeErrors"]))
                ids = [d["op"]["_id"] for d in e.details["writeErrors"]]
                # build hash of existing docs
                docs = self.temp_collection.find({"_id": {"$in": ids}})
                hdocs = {}
                for doc in docs:
                    hdocs[doc["_id"]] = doc
                bulk = []
                for err in e.details["writeErrors"]:
                    errdoc = err["op"]
                    existing = hdocs[errdoc["_id"]]
                    if errdoc is existing:
                        # if the same document has been yielded twice,
                        # they could be the same, so we ignore it but
                        # count it as processed (see assert below)
                        nbinsert += 1
                        continue
                    assert "_id" in existing
                    _id = errdoc.pop("_id")
                    merged = self.__class__.merge_func(errdoc, existing, aslistofdict=aslistofdict)
                    # update previously fetched doc. if several errors are about the same doc id,
                    # we would't merged things properly without an updated document
                    assert "_id" in merged
                    bulk.append(UpdateOne({"_id": _id}, {"$set": merged}))
                    hdocs[_id] = merged
                    nbinsert += 1

                self.temp_collection.bulk_write(bulk, ordered=False)
                self.logger.info("OK [%s]" % timesofar(tinner))
            assert nbinsert == toinsert, "nb %s to %s" % (nbinsert, toinsert)
            # end of loop so it counts the time spent in doc_iterator
            tinner = time.time()
            total += nbinsert

        self.logger.info(f"Done[{timesofar(t0)}] with {total} docs")

        return total
