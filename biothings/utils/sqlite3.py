from functools import wraps
from typing import Dict, List, Any, Iterable, Optional, Union, cast
import collections
import json
import logging
import os
import sqlite3

from pymongo import InsertOne, ReplaceOne, UpdateOne
from typing import Any, Mapping, TypeVar
from functools import wraps

from biothings.utils.common import find_value_in_doc, json_serial
from biothings.utils.dataload import update_dict_recur
from biothings.utils.dotfield import parse_dot_fields
from biothings.utils.hub_db import IDatabase
from biothings.utils.serializer import json_loads

config = None

logger = logging.getLogger(__name__)

DocumentType = TypeVar("DocumentType", bound=Mapping[str, Any])


def requires_config(func):
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        global config
        if not config:
            try:
                from biothings import config as config_mod

                config = config_mod
            except ImportError:
                raise Exception("call biothings.config_for_app() first")
        return func(*args, **kwargs)

    return func_wrapper


@requires_config
def get_hub_db_conn():
    conn = DatabaseClient()
    return conn


@requires_config
def get_src_conn():
    conn = get_hub_db_conn()
    return conn


@requires_config
def get_src_db():
    conn = get_src_conn()
    return conn[config.DATA_SRC_DATABASE]


@requires_config
def get_src_dump():
    conn = get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][getattr(config, "DATA_SRC_DUMP_COLLECTION", "src_dump")]


@requires_config
def get_src_master():
    conn = get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.DATA_SRC_MASTER_COLLECTION]


def get_src_build():
    conn = get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.DATA_SRC_BUILD_COLLECTION]


def get_src_build_config():
    conn = get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.DATA_SRC_BUILD_COLLECTION + "_config"]


def get_data_plugin():
    conn = get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.DATA_PLUGIN_COLLECTION]


def get_api():
    conn = get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.API_COLLECTION]


def get_cmd():
    conn = get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.CMD_COLLECTION]


def get_event():
    conn = get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][getattr(config, "EVENT_COLLECTION", "event")]


def get_hub_config():
    conn = get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][getattr(config, "HUB_CONFIG_COLLECTION", "hub_config")]


def get_last_command():
    try:
        db = get_cmd()
        res = db.get_conn().execute("SELECT MAX(_id) FROM cmd").fetchall()
        assert res[0][0], "No command ID found, bootstrap ?"
        return {"_id": res[0][0]}
    except Exception:
        return {"_id": 1}


def get_source_fullname(col_name):
    """
    Assuming col_name is a collection created from an upload process,
    find the main source & sub_source associated.
    """
    src_dump = get_src_dump()
    info = None
    for doc in src_dump.find():
        if col_name in doc.get("upload", {}).get("jobs", {}).keys():
            info = doc
    if info:
        name = info["_id"]
        if name != col_name:
            # col_name was a sub-source name
            return "%s.%s" % (name, col_name)
        else:
            return name


class Database(IDatabase):
    def __init__(self, db_folder, name=None):
        super(Database, self).__init__()
        if not name:
            self.name = config.DATA_HUB_DB_DATABASE
        else:
            self.name = name
        self.dbfile = os.path.join(db_folder, self.name)
        self.cols = {}

    @property
    def address(self):
        return self.dbfile

    def get_conn(self):
        return sqlite3.connect(self.dbfile)

    def collection_names(self):
        tables = self.get_conn().execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        return [name[0] for name in tables]

    def create_collection(self, colname):
        return self[colname]

    def create_if_needed(self, table):
        existings = [
            tname[0]
            for tname in self.get_conn()
            .execute("SELECT name FROM sqlite_master WHERE type='table' and " + "name = ?", (table,))
            .fetchall()
        ]
        if table not in existings:
            # TODO: injection...
            self.get_conn().execute("CREATE TABLE %s (_id TEXT PRIMARY KEY, document TEXT)" % table).fetchone()

    def __getitem__(self, colname):
        if colname not in self.cols:
            self.create_if_needed(colname)
            self.cols[colname] = Collection(colname, self)
        return self.cols[colname]


class DatabaseClient(IDatabase):
    def __init__(self):
        super().__init__()
        self.sqlite_db_folder = config.HUB_DB_BACKEND["sqlite_db_folder"]

        if not os.path.exists(self.sqlite_db_folder):
            os.makedirs(self.sqlite_db_folder)
        self.name = None
        self.dbfile = None
        self.cols = {}

    def __getitem__(self, name):
        return Database(self.sqlite_db_folder, name)


class Sqlite3BulkWriteResult:
    """
    An object wrapper for bulk API write results

    This mimics the structure defined by pymongo defined
    by `pymongo.results.BulkWriteResult`
    """

    __slots__ = ("__bulk_api_result",)

    def __init__(self, bulk_api_result: Iterable[Any]):
        self.__bulk_api_result = bulk_api_result

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__bulk_api_result!r}"

    @property
    def bulk_api_result(self) -> Iterable[Any]:
        """
        The raw bulk API result
        """
        return self.__bulk_api_result

    @property
    def inserted_count(self) -> int:
        """
        The number of documents inserted
        """
        return len(self.__bulk_api_result)

    @property
    def matched_count(self) -> int:
        """
        The number of documents matched for an update
        """
        logger.warning("sqlite3 has no concept of a match counted")
        return 0

    @property
    def modified_count(self) -> int:
        """
        The number of documents modified
        """
        return cast(int, self.__bulk_api_result.get("nModified"))

    @property
    def deleted_count(self) -> int:
        """
        The number of documents deleted
        """
        return cast(int, self.__bulk_api_result.get("nRemoved"))

    @property
    def upserted_count(self) -> int:
        """
        The number of documents upserted
        """
        logger.warning("sqlite3 has no concept of a upserted counted")
        return 0

    @property
    def upserted_ids(self) -> Optional[Dict[int, Any]]:
        """
        A map of operation index to the _id of the upserted document
        """
        if self.__bulk_api_result:
            return {upsert["index"]: upsert["_id"] for upsert in self.bulk_api_result["upserted"]}
        return None


class Sqlite3BulkWriteError(Exception):
    """
    Bulk write error specifically targetting the sqlite3
    database backend. We won't get any metrics on the failed insertion
    other than the table and the column, we'll have to do a bit more processing
    on the documents that were attempted to be written. Thus we have a Exception
    that is essentially a wrapper around sqlite3.IntegrityError
    """


class Collection(object):
    def __init__(self, colname, db):
        self.colname = colname
        self.db = db

    def get_conn(self):
        return sqlite3.connect(self.db.dbfile)

    @property
    def name(self):
        return self.colname

    @property
    def database(self):
        return self.db

    def find_one(self, *args, **kwargs):
        if args and len(args) == 1 and isinstance(args[0], Dict):
            if len(args[0]) == 1 and "_id" in args[0]:
                strdoc = (
                    self.get_conn()
                    .execute("SELECT document FROM %s WHERE _id = ?" % self.colname, (args[0]["_id"],))
                    .fetchone()
                )
                if strdoc:
                    return json_loads(strdoc[0])
                else:
                    return None
            else:
                return self.find(*args, find_one=True)
        elif args or kwargs:
            raise NotImplementedError("find(): %s %s" % (repr(args), repr(kwargs)))
        else:
            return self.find(find_one=True)

    def findv2(self, *args, **kwargs):
        """This is a new version of find() that uses json feature of sqlite3, will replace find in the future"""
        start = kwargs.get("start", 0)
        limit = kwargs.get("limit", 10)
        return_total = kwargs.get("return_total", False)  # return (results, total) tuple if True, default False
        return_list = kwargs.get("return_list", False)  # return list instead of generator if True, default False
        conn = self.get_conn()
        tbl_name = self.colname

        if args and len(args) == 1 and isinstance(args[0], Dict) and len(args[0]) > 0:
            # it's key/value search, args[0] like {"a.b": "test", "a.b.c", "value"}
            sub_queries = []
            for k, v in args[0].items():
                if "*" in v or "?" in v:
                    _v = v.replace("*", "%").replace("?", "_")
                    _v = f"LIKE '{_v}'"
                else:
                    _v = f"= '{v}'"
                if k == "_id":
                    where = f"(_id {_v})"
                    sub_query = f"SELECT _id FROM {tbl_name} WHERE {where}"
                elif "." in k:
                    # nested field name like a.b.c, we will use json_tree.fullkey to match
                    # Here is an example for the query {"object.symbol": "BRD1"}:
                    # SELECT document FROM TISSUES, json_tree(TISSUES.document)
                    # WHERE (json_tree.fullkey LIKE '$.%object%.%symbol%' AND json_tree.value = 'BRD1')
                    k = k.replace(".", "%.%")
                    k = f"$.%{k}%"
                    where = f"(json_tree.fullkey LIKE '{k}' AND json_tree.value {_v})"
                    sub_query = f"SELECT _id FROM {tbl_name}, json_tree({tbl_name}.document) WHERE {where}"
                else:
                    # just a top level field, we will use json_each.key to match
                    # _v matches the value directly using LIKE or = (for a scalar field);
                    # _v2 matches the double-quoted value using LIKE (for an array field)
                    # Here is an example for the query {"ancestors": "CHEBI:75771"}:
                    # "SELECT _id FROM chebi, json_each(chebi.document)
                    # WHERE (json_each.key = 'ancestors' AND
                    #       (json_each.value = 'CHEBI:75771' OR json_each.value LIKE '%"CHEBI:75771"%')
                    # )
                    _v2 = _v.replace("LIKE '", "LIKE '%\"").replace("= '", "LIKE '%\"")
                    _v2 = _v2[:-1] + "\"%'"
                    where = f"(json_each.key = '{k}' AND (json_each.value {_v} OR json_each.value {_v2}))"
                    sub_query = f"SELECT _id FROM {tbl_name}, json_each({tbl_name}.document) WHERE {where}"
                sub_queries.append(sub_query)
            if sub_queries:
                if len(sub_queries) == 1:
                    query = sub_queries[0].replace("SELECT _id FROM", "SELECT document FROM")
                else:
                    # JOIN multiple sub queries:
                    # Here is an example for the query: q=object.symbol:BRD1%20AND%20subject.id:BTO:0000017
                    # SELECT document FROM TISSUES WHERE _id IN
                    #   (SELECT _id FROM
                    #       (SELECT _id FROM TISSUES, json_tree(TISSUES.document)
                    #           WHERE (json_tree.fullkey LIKE '$.%object%.%symbol%' AND json_tree.value = 'BRD1')
                    #       ) AS subq0
                    #       INNER JOIN
                    #       (SELECT _id FROM TISSUES, json_tree(TISSUES.document)
                    #           WHERE (json_tree.fullkey LIKE '$.%subject%.%id%' AND json_tree.value = 'BTO:0000017')
                    #       ) AS subq1
                    #       USING (_id)
                    #   )
                    query = f"SELECT _id FROM ({sub_queries[0]}) AS subq0"
                    for i, sub_query in enumerate(sub_queries[1:]):
                        query += f" INNER JOIN ({sub_query}) AS subq{i+1} USING (_id)"
                    query = f"SELECT document FROM {tbl_name} WHERE _id IN ({query})"
        elif not args or len(args) == 1 and len(args[0]) == 0:
            # nothing or empty dict
            query = f"SELECT document FROM {tbl_name}"
        else:
            raise NotImplementedError("find: args=%s kwargs=%s" % (repr(args), repr(kwargs)))

        # include limit and offset
        _query = query + f" LIMIT {limit} OFFSET {start}"
        logger.debug('SQLite query: "%s"', _query)
        results = (json_loads(doc[0]) for doc in conn.execute(_query))  # results is a generator
        if return_list:
            results = List(results)
        if return_total:
            # get total count without limit and offset
            total = conn.execute(query.replace("SELECT document FROM", "SELECT COUNT(*) FROM")).fetchone()[0]
            return results, total
        else:
            return results

    def find(self, *args, **kwargs):
        results = []
        if args and len(args) == 1 and isinstance(args[0], Dict) and len(args[0]) > 0:
            # it's key/value search, let's iterate
            for doc in self.get_conn().execute("SELECT document FROM %s" % self.colname).fetchall():
                found = []
                doc = json_loads(doc[0])
                for k, v in args[0].items():
                    _found = find_value_in_doc(k, v, doc)
                    found.append(_found)
                if all(found):
                    if "find_one" in kwargs:
                        return doc
                    else:
                        results.append(doc)
            if "limit" in kwargs:
                start = kwargs.get("start", 0)
                end = start + kwargs.get("limit", 0)
                return results[start:end]
            return results
        elif not args or len(args) == 1 and len(args[0]) == 0:
            # nothing or empty dict
            results = [
                json_loads(doc[0])
                for doc in self.get_conn().execute("SELECT document FROM %s" % self.colname).fetchall()
            ]
            if "limit" in kwargs:
                start = kwargs.get("start", 0)
                end = start + kwargs.get("limit", 0)
                return results[start:end]
            return results
        else:
            raise NotImplementedError("find: args=%s kwargs=%s" % (repr(args), repr(kwargs)))

    def id_search(self, id_collection: List[str]) -> List[tuple]:
        """
        Method for bulk searching against the _id column in the database.
        Primarily used for determining the culprit to the uniqueness integrity violation
        from a bulk write.

        args:
            id_collection: List of strings representing the document _id value

        output:
        """
        discovered_id = []
        with self.get_conn() as conn:
            for id_value in id_collection:
                try:
                    id_verification_query = f"SELECT _id, document FROM {self.colname} WHERE _id = '{id_value}'"
                    discovered_id.extend(conn.execute(id_verification_query).fetchall())
                except Exception as gen_exp:
                    logger.exception(gen_exp)
                    logger.warning("Skipping %s for id search", id_value)
        return discovered_id

    def insert_one(self, doc: Dict, *args, **kwargs) -> None:
        """
        single-document insert into the database

        Leverages the execute function using the question mark style for specifying
        the values to insert into the table
        https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.execute
        https://docs.python.org/3/library/sqlite3.html#how-to-use-placeholders-to-bind-values-in-sql-queries
        """
        with self.get_conn() as conn:
            try:
                parameters = (doc["_id"], json.dumps(doc, default=json_serial))
                sql_statement = f"INSERT INTO {self.colname} (_id,document) VALUES (?,?)"
                conn.execute(sql_statement, parameters)
            except sqlite3.IntegrityError as integrity_err:
                logger.exception(integrity_err)
                logger.error("Unable to complete transation (check the _id value for uniqueness). Document: %s", doc)
                raise integrity_err

    def insert(self, docs: Iterable[Dict], *args, **kwargs) -> None:
        """
        multi-document insert into the database

        Leverages the executemany function using the named-placeholders for specifying
        the values to insert into the table
        https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.executemany
        https://docs.python.org/3/library/sqlite3.html#how-to-use-placeholders-to-bind-values-in-sql-queries

        We first have to transform the list of documents into a representation we can handle
        so we create a collection with the following structure:
        {_id: <id>, repr: <json.dumps>}

        We then call executemany to insert the entire collection into the database

        If we get an integrity error, it's often due to an non-unique ID value associated with the
        document. The integrity error instance itself doesn't give the actual failing ID associated
        with the integrity error so we have to infer from the data we have to see if we can find
        anything. We can create a collections.Counter to track the frequency of each _id in the set
        that we're provided. If we find any nearby instances that are duplicated then we can log it
        for debugging purposes
        """
        with self.get_conn() as conn:
            rendered_documents = [{"_id": doc["_id"], "repr": json.dumps(doc, default=json_serial)} for doc in docs]
            try:
                sql_statement = f"INSERT INTO {self.colname} (_id,document) VALUES (:_id, :repr)"
                conn.executemany(sql_statement, rendered_documents)
            except sqlite3.IntegrityError as integrity_err:
                logger.exception(integrity_err)
                id_counter = collections.Counter([doc["_id"] for doc in rendered_documents])
                discovered_non_unique_id = List(
                    filter(lambda id_frequency: id_frequency[1] > 1, id_counter.most_common(10))
                )
                if len(discovered_non_unique_id) > 0:
                    logger.error("Discovered non-unique id values: %s", discovered_non_unique_id)
                raise integrity_err

    def bulk_write(
        self, docs: Iterable[Union[Dict, InsertOne, ReplaceOne, UpdateOne]], *args, **kwargs
    ) -> Sqlite3BulkWriteResult:
        """
        "Overridden method" to mimic the structure of mongodb as our design followed a lot of
        patterns dictated by mongodb initially. If we wish to support sqlite3 with the command line
        tooling then we have to structure this method similarly.

        Across our codebase we have several potential different operations that are normally
        associated with pymongo that are used with bulk_write. These are `InsertOne`, 'ReplaceOne`,
        and 'UpdateOne`. We have to ensure on the sqlite3 version of bulk_write to expect handling
        documents of this type
        """
        raw_documents = []
        for document in docs:
            try:
                raw_documents.append(document._doc)
            except Exception as gen_exp:
                breakpoint()
                pass

        try:
            self.insert(raw_documents)
        except sqlite3.IntegrityError as integrity_error:
            raise Sqlite3BulkWriteError from integrity_error
        return Sqlite3BulkWriteResult(raw_documents)

    def update_one(self, query, what, upsert=False):
        assert len(what) == 1 and (
            "$set" in what or "$unset" in what or "$push" in what
        ), "$set/$unset/$push operators not found"
        doc = self.find_one(query)
        if doc:
            if "$set" in what:
                # parse_dot_fields uses json.dumps internally, we can to make
                # sure everything is serializable first
                what = json.loads(json.dumps(what, default=json_serial))
                what = parse_dot_fields(what["$set"])
                doc = update_dict_recur(doc, what)
            elif "$unset" in what:
                for keytounset in what["$unset"].keys():
                    doc.pop(keytounset, None)
            elif "$push" in what:
                for listkey, elem in what["$push"].items():
                    assert "." not in listkey, "$push not supported for nested keys: %s" % listkey
                    doc.setdefault(listkey, []).append(elem)
            self.save(doc)
        elif upsert:
            assert "$set" in what
            query.update(what["$set"])
            self.save(query)

    def update(self, query, what, upsert=False):
        docs = self.find(query)
        for doc in docs:
            self.update_one({"_id": doc["_id"]}, what, upsert)

    def save(self, doc):
        if self.find_one({"_id": doc["_id"]}):
            with self.get_conn() as conn:
                conn.execute(
                    "UPDATE %s SET document = ? WHERE _id = ?" % self.colname,
                    (json.dumps(doc, default=json_serial), doc["_id"]),
                )
                conn.commit()
        else:
            self.insert_one(doc)

    def replace_one(self, query, doc, upsert=False):
        assert "_id" in query
        orig = self.find_one(query)
        if orig:
            orig["_id"] = query["_id"]
            with self.get_conn() as conn:
                conn.execute(
                    "UPDATE %s SET document = ? WHERE _id = ?" % self.colname,
                    (json.dumps(doc, default=json_serial), orig["_id"]),
                )
                conn.commit()
        elif upsert:
            doc["_id"] = query["_id"]
            self.save(doc)

    def remove(self, query):
        docs = self.find(query)
        with self.get_conn() as conn:
            for doc in docs:
                conn.execute("DELETE FROM %s WHERE _id = ?" % self.colname, (doc["_id"],)).fetchone()
            conn.commit()

    def rename(self, new_name, dropTarget=False):
        with self.get_conn() as conn:
            if dropTarget:
                conn.execute(f"DROP TABLE IF EXISTS {new_name}")
            conn.execute(f"ALTER TABLE {self.colname} RENAME TO {new_name}").fetchall()

    def count(self):
        return self.get_conn().execute("SELECT count(_id) FROM %s" % self.colname).fetchone()[0]

    def drop(self):
        self.get_conn().execute("DROP TABLE %s" % self.colname).fetchall()

    def __getitem__(self, _id):
        return self.find_one({"_id": _id})

    def __getstate__(self):
        self.__dict__.pop("db", None)
        return self.__dict__
