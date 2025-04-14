import collections
import copy
import logging
import json
import operator
import time
import types
from typing import Iterable, List

import sqlite3
from sqlite3 import IntegrityError

from pymongo import InsertOne, ReplaceOne, UpdateOne
from pymongo.errors import BulkWriteError, DuplicateKeyError

from biothings.utils.common import iter_n, timesofar
from biothings.utils.dataload import merge_root_keys, merge_struct
from biothings.utils.mongo import check_document_size, get_src_db
from biothings.utils.sqlite3 import Sqlite3BulkWriteError


class StorageException(Exception):
    pass


class BaseStorage:
    def __init__(self, db, dest_col_name, logger=logging):
        db = db or get_src_db()
        self.temp_collection = db[dest_col_name]
        self.logger = logger

    def process(self, iterable, *args, **kwargs):
        """
        Process iterable to store data. Must return the number
        of inserted records (even 0 if none)
        """
        raise NotImplementedError("implement-me in subclass")

    def check_doc_func(self, doc):
        """
        Return doc if it's alright, False if doc should be ignore for some reason
        Subclass and override as needed.
        """
        return doc


class CheckSizeStorage(BaseStorage):
    def check_doc_func(self, doc):
        ok = check_document_size(doc)
        # this is typically used to skip LFQSCWFLJHTTHZ-UHFFFAOYSA-N (Ethanol)
        # because there are too many elements in "ndc" list
        if not ok:
            self.logger.warning("Skip document '%s' because too large" % doc.get("_id"))
            return False
        return ok


class BasicStorage(BaseStorage):
    def doc_iterator(self, doc_d, batch=True, batch_size=10000):
        if isinstance(doc_d, (types.GeneratorType, list)) and batch:
            for doc_li in iter_n(doc_d, n=batch_size):
                doc_li = [d for d in doc_li if self.check_doc_func(d)]
                yield doc_li
        else:
            if batch:
                doc_li = []
                i = 0
            for _id, doc in doc_d.items():
                doc["_id"] = _id
                _doc = {}
                _doc.update(doc)
                if batch:
                    doc_li.append(_doc)
                    i += 1
                    if i % batch_size == 0:
                        doc_li = [d for d in doc_li if self.check_doc_func(d)]
                        yield doc_li
                        doc_li = []
                else:
                    yield self.check_doc_func(_doc)

            if batch:
                doc_li = [d for d in doc_li if self.check_doc_func(d)]
                yield doc_li

    def process(self, doc_d, batch_size, max_batch_num=None):
        self.logger.info("Uploading to the DB...")
        t0 = time.time()
        total = 0
        batch_num = 0
        for doc_li in self.doc_iterator(doc_d, batch=True, batch_size=batch_size):
            if max_batch_num and batch_num >= max_batch_num:
                break
            batch_num += 1
            self.temp_collection.insert(doc_li, manipulate=False, check_keys=False)
            total += len(doc_li)
        self.logger.info(f"Done[{timesofar(t0)}] with {total} docs")

        return total


class MergerStorage(BasicStorage):
    """
    This storage will try to merge documents when finding duplicated errors.
    It's useful when data is parsed using iterator. A record can be stored in database,
    then later, another record with the same ID is sent to the db, raising a duplicated error.
    These two documents would have been merged before using a 'put all in memory' parser.
    Since data is here read line by line, the merge is done while storing
    """

    merge_func = merge_struct
    process_count = 0

    def process(self, doc_d, batch_size, max_batch_num=None):
        self.process_count += 1
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
                self.logger.debug("Fixing %d records " % len(e.details["writeErrors"]))
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
            except Sqlite3BulkWriteError as sqlite3_bulk_error:
                """
                internal collection -> entries already in the sqlite3 database
                external collection -> entries in the batch for the bulk_write

                Conflict 2) id value in the external collection collides with a separate id in the
                internal collection

                Conflict 1) id value in the external collection collides with the id in the internal
                collection

                Handle the merge for conflict 2 before isolating conflict 1

                We generate a default dict with a list as the container so that we can aggregate the
                documents into batches based off their id as the key value. Any entries with values
                greater than 2 have duplicates and need to be merged
                """
                self.logger.exception(sqlite3_bulk_error)
                self.logger.debug(
                    (
                        "Attempting to correct sqlite3 uniqueness constraint for the id column.\n" "Batch Size %s",
                        len(bulk),
                    )
                )

                # external conflict management
                self.logger.debug("Processing external entries")
                entry_mapping = collections.defaultdict(list)
                for index, entry in enumerate(bulk):
                    entry_mapping[entry._doc["_id"]].append(index)

                multiple_collisions = {
                    eid: collection for eid, collection in entry_mapping.items() if len(collection) > 1
                }

                external_removal_indices = []
                for conflict_id, index_pointers in multiple_collisions.items():
                    self.logger.debug("Discovered %s identical entries in external batch", len(index_pointers))

                    starter_index = index_pointers.pop(0)
                    external_removal_indices.append(starter_index)
                    merged_document = bulk[starter_index]._doc

                    while len(index_pointers) > 0:
                        pointer_index = index_pointers.pop(-1)
                        external_removal_indices.append(pointer_index)
                        conflict_value = bulk[pointer_index]._doc
                        merged_document = self.__class__.merge_func(merged_document, conflict_value)

                    self.logger.debug("Adding merged document %s", merged_document)
                    bulk.append(InsertOne(merged_document))

                for offset_index, remove_index in enumerate(sorted(external_removal_indices)):
                    self.logger.debug("Popping %s index from bulk upload", remove_index - offset_index)
                    bulk.pop(remove_index - offset_index)

                entry_mapping = {entry._doc["_id"]: index for index, entry in enumerate(bulk)}

                # internal conflict management
                self.logger.debug("Processing internal entries")
                batch_ids = [entry._doc["_id"] for entry in bulk]
                internal_conflicts = self.temp_collection.id_search(batch_ids)

                self.logger.debug("Discovered %s internal conflict entries", len(index_pointers))

                internal_removal_indicies = []
                for conflict_result in internal_conflicts:
                    conflict_id = conflict_result[0]
                    conflict_value = json.loads(conflict_result[1])
                    internal_pointer = entry_mapping.get(conflict_id, None)
                    if internal_pointer is not None:
                        internal_removal_indicies.append(internal_pointer)
                        internal_document = bulk[internal_pointer]._doc
                        merged_document = self.__class__.merge_func(conflict_value, internal_document)
                        self.logger.debug("Updating document %s", merged_document)
                        self.temp_collection.save(merged_document)

                for offset_index, remove_index in enumerate(sorted(internal_removal_indicies)):
                    self.logger.debug("Popping %s index from bulk upload", remove_index - offset_index)
                    bulk.pop(remove_index - offset_index)

                self.logger.info("Re-attempting bulk write. Inserting %s records ... " % len(bulk))
                self.temp_collection.bulk_write(bulk, ordered=False)
                self.logger.info("OK [%s]" % timesofar(tinner))

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
                bulk_set = [InsertOne(document) for document in self.unique_documents(doc_li)]
                res = self.temp_collection.bulk_write(bulk_set, ordered=False)
                total += res.inserted_count
                self.logger.info("Inserted %s records [%s]" % (res.inserted_count, timesofar(tinner)))
            except BulkWriteError as e:
                self.logger.info(
                    "Inserted %s records, ignoring %d [%s]"
                    % (e.details["nInserted"], len(e.details["writeErrors"]), timesofar(tinner))
                )
            except IntegrityError as integrity_error:
                self.logger.warning(f"Skipping duplicate record. Details: {integrity_error}")
            except Exception as gen_exp:
                self.logger.exception(gen_exp)
                raise gen_exp
            tinner = time.time()
        self.logger.info("Done[%s]" % timesofar(t0))

        return total

    def unique_documents(self, documents: Iterable[dict]) -> List[dict]:
        """
        Generates the set of id values from the provided batch of documents
        due to our documents being unhashable

        We then filter the documents to only the unique ID values which should eliminate
        any uniqueness constraint issues when uploading to the database

        Returns a list of filtered documents
        """
        unique_documents = list({document["_id"]: document for document in documents}.values())
        if len(unique_documents) < len(documents):
            self.logger.debug("Filtered %s documents before upload", len(documents) - len(unique_documents))
        return unique_documents


class NoBatchIgnoreDuplicatedStorage(BasicStorage):
    """
    You should use IgnoreDuplicatedStorage, which works using batch
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


class NoStorage:
    """
    This a kind of a place-holder, this storage will just store nothing...
    (but it will respect storage interface)
    """

    def __init__(self, db_info, dest_col_name, logger):
        db = get_src_db()
        self.temp_collection = db[dest_col_name]
        self.logger = logger

    def process(self, iterable, *args, **kwargs):
        self.logger.info("NoStorage stores nothing, skip...")
        return 0


def get_storage_class(ondups=None):
    if ondups and ondups != "error":
        if ondups == "merge":
            return "biothings.utils.storage.MergerStorage"
        elif ondups == "ignore":
            return "biothings.utils.storage.IgnoreDuplicatedStorage"
    else:
        return "biothings.utils.storage.BasicStorage"
