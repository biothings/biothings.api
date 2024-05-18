import abc
import copy
import logging
import time
import types

from sqlite3 import IntegrityError

from pymongo import InsertOne, ReplaceOne, UpdateOne
from pymongo.errors import BulkWriteError, DuplicateKeyError

from biothings.utils.common import iter_n, timesofar
from biothings.utils.dataload import merge_root_keys, merge_struct
from biothings.utils.mongo import check_document_size, get_src_db
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


class RootKeyMergerStorage(MergerStorage):
    """
    Just like MergerStorage, this storage deals with duplicated error
    by appending key's content to existing document. Keys in existing
    document will be converted to a list as needed.

    Note:
      - root keys must have the same type in each documents
      - inner structures aren't merged together, the merge happend
        at root key level
    """

    @classmethod
    def merge_func(klass, doc1, doc2, **kwargs):
        # caller popped it from doc1, let's take from doc2
        _id = doc2["_id"]
        # exclude_id will remove _if from doc2, that's why we kept it from before
        # also, copy doc2 ref as the merged doc will be stored in
        # a bulk op object, since doc2 is modified in place, this could impact
        # the bulk op and procude empty $set error from mongo
        doc = merge_root_keys(doc1, copy.copy(doc2), exclude=["_id"])
        doc["_id"] = _id
        return doc


class IgnoreDuplicatedStorage(BasicStorage):
    def process(self, iterable, batch_size, max_batch_num=None):
        self.logger.info("Uploading to the DB...")
        t0 = time.time()
        tinner = time.time()
        total = 0
        batch_num = 0
        for doc_li in self.doc_iterator(iterable, batch=True, batch_size=batch_size):
            if max_batch_num and batch_num >= max_batch_num:
                break
            batch_num += 1
            try:
                bulk = []
                for d in doc_li:
                    bulk.append(InsertOne(d))
                res = self.temp_collection.bulk_write(bulk, ordered=False)
                total += res.inserted_count
                self.logger.info("Inserted %s records [%s]" % (res.inserted_count, timesofar(tinner)))
            except BulkWriteError as e:
                self.logger.info(
                    "Inserted %s records, ignoring %d [%s]"
                    % (e.details["nInserted"], len(e.details["writeErrors"]), timesofar(tinner))
                )
            except IntegrityError as e:
                self.logger.info(f"Skipping duplicate record. Details: {e}")
            except Exception:
                raise
            tinner = time.time()
        self.logger.info("Done[%s]" % timesofar(t0))

        return total


class NoBatchIgnoreDuplicatedStorage(BasicStorage):
    """
    You should use IgnoreDuplicatedStorag, which works using batch
    and is thus way faster...
    """

    def process(self, doc_d, batch_size, max_batch_num=None):
        self.logger.info("Uploading to the DB...")
        t0 = time.time()
        tinner = time.time()
        # force step = 1
        cnt = 0
        total = 0
        dups = 0
        batch_num = 0
        for doc_li in self.doc_iterator(doc_d, batch=True, batch_size=1):
            if max_batch_num and batch_num >= max_batch_num:
                break
            batch_num += 1
            try:
                self.temp_collection.insert(doc_li, manipulate=False, check_keys=False)
                cnt += 1
                total += 1
                if (cnt + dups) % batch_size == 0:
                    # we insert one by one but display progress on a "batch_size" base
                    self.logger.info("Inserted %s records, ignoring %s [%s]" % (cnt, dups, timesofar(tinner)))
                    cnt = 0
                    dups = 0
                    tinner = time.time()
            except DuplicateKeyError:
                dups += 1
                pass
        self.logger.info("Done[%s]" % timesofar(t0))

        return total


class UpsertStorage(BasicStorage):
    """Insert or update documents, based on _id"""

    def process(self, iterable, batch_size, max_batch_num=None):
        self.logger.info("Uploading to the DB...")
        t0 = time.time()
        tinner = time.time()
        total = 0
        batch_num = 0
        for doc_li in self.doc_iterator(iterable, batch=True, batch_size=batch_size):
            if max_batch_num and batch_num >= max_batch_num:
                break
            batch_num += 1
            try:
                bulk = []
                for d in doc_li:
                    bulk.append(ReplaceOne(filter={"_id": d["_id"]}, replacement=d, upsert=True))
                res = self.temp_collection.bulk_write(bulk, ordered=False)
                nb = res.upserted_count + res.modified_count
                total += nb
                self.logger.info("Upserted %s records [%s]" % (nb, timesofar(tinner)))
            except Exception:
                raise
            tinner = time.time()
        self.logger.info("Done[%s]" % timesofar(t0))

        return total
