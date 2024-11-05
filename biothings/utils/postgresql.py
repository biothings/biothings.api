import json
import logging
from typing import Any, Dict, Iterable, Optional

import psycopg2
import psycopg2.extras
from psycopg2 import sql

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, dbname: str, user: str, password: str, host: str = 'localhost', port: int = 5432):
        self.connection_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port,
        }
        self.connection = psycopg2.connect(**self.connection_params)
        self.collections = {}

    def __getitem__(self, name):
        return Collection(self, name)

    def close(self):
        self.connection.close()


class Collection:
    def __init__(self, colname: str, database: Database):
        self.colname = colname
        self.database = database
        self.connection = database.connection
        self.ensure_table_exists()

    @property
    def name(self):
        return self.colname

    def get_cursor(self):
        return self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def ensure_table_exists(self):
        with self.get_cursor() as cursor:
            try:
                cursor.execute(
                    sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {} (
                            _id TEXT PRIMARY KEY,
                            document JSONB
                        )
                    """).format(sql.Identifier(self.colname))
                )
                self.connection.commit()
            except Exception as e:
                self.connection.rollback()
                print(f"Error creating table {self.colname}: {e}")
                raise

    def _build_where_clause(self, query: Dict[str, Any]):
        """
        Build a WHERE clause and parameters for SQL queries from a query dictionary.
        Supports simple equality conditions.
        """
        clauses = []
        params = []
        for key, value in query.items():
            if key == '_id':
                clauses.append("_id = %s")
                params.append(value)
            else:
                # Handle JSONB field querying
                clauses.append(f"document ->> %s = %s")
                params.extend([key, str(value)])
        where_clause = "WHERE " + " AND ".join(clauses) if clauses else ""
        return where_clause, params

    def find_one(self, query: Optional[Dict[str, Any]] = None):
        """
        Find a single document matching the query.
        """
        with self.get_cursor() as cursor:
            where_clause, params = self._build_where_clause(query or {})
            sql = f"SELECT document FROM {self.colname} {where_clause} LIMIT 1"
            cursor.execute(sql, params)
            result = cursor.fetchone()
            return result['document'] if result else None

    def find(self, query: Optional[Dict[str, Any]] = None) -> Iterable[Dict[str, Any]]:
        """
        Find all documents matching the query.
        """
        with self.get_cursor() as cursor:
            where_clause, params = self._build_where_clause(query or {})
            sql = f"SELECT document FROM {self.colname} {where_clause}"
            cursor.execute(sql, params)
            for row in cursor:
                yield row['document']

    def insert_one(self, doc: Dict[str, Any], *args, **kwargs) -> None:
        """
        Insert a single document into the collection.
        """
        _id = doc.get('_id')
        if not _id:
            raise ValueError("Document must have an '_id' field")
        with self.get_cursor() as cursor:
            cursor.execute(f"""
                INSERT INTO {self.colname} (_id, document)
                VALUES (%s, %s::jsonb)
            """, (_id, json.dumps(doc)))
            self.connection.commit()

    def update_many(self, query: Dict[str, Any], update: Dict[str, Any]):
        """
        Update multiple documents matching the query.
        Only supports the "$set" operation.
        """
        if '$set' not in update:
            raise NotImplementedError(
                "Only the '$set' update operation is implemented")

        set_fields = update['$set']
        set_clauses = []
        set_params = []
        for key, value in set_fields.items():
            path = '{' + ','.join(key.split('.')) + '}'
            set_clauses.append(
                f"document = jsonb_set(document, %s, %s::jsonb, true)")
            set_params.extend([path, json.dumps(value)])

        where_clause, where_params = self._build_where_clause(query)
        sql = f"""
            UPDATE {self.colname}
            SET {', '.join(set_clauses)}
            {where_clause}
        """

        with self.get_cursor() as cursor:
            cursor.execute(sql, set_params + where_params)
            self.connection.commit()

    def replace_one(self, query: Dict[str, Any], doc: Dict[str, Any], upsert: bool = False):
        """
        Replace a single document matching the query.
        If upsert is True, insert the document if it doesn't exist.
        """
        _id = doc.get('_id')
        if not _id:
            raise ValueError("Document must have an '_id' field")

        where_clause, params = self._build_where_clause(query)
        with self.get_cursor() as cursor:
            cursor.execute(
                f"SELECT COUNT(*) FROM {self.colname} {where_clause}", params)
            count = cursor.fetchone()[0]

            if count > 0:
                cursor.execute(f"""
                    UPDATE {self.colname}
                    SET document = %s::jsonb
                    {where_clause}
                """, [json.dumps(doc)] + params)
            elif upsert:
                cursor.execute(f"""
                    INSERT INTO {self.colname} (_id, document)
                    VALUES (%s, %s::jsonb)
                """, (_id, json.dumps(doc)))
            self.connection.commit()

    def remove(self, query: Dict[str, Any]):
        """
        Remove documents matching the query.
        """
        where_clause, params = self._build_where_clause(query)
        sql = f"DELETE FROM {self.colname} {where_clause}"
        with self.get_cursor() as cursor:
            cursor.execute(sql, params)
            self.connection.commit()

    def remove_many(self, query: Dict[str, Any]):
        """
        Alias for remove; removes multiple documents.
        """
        self.remove(query)

    def rename(self, new_name: str, dropTarget: bool = False):
        with self.get_cursor() as cursor:
            if dropTarget:
                query = sql.SQL("DROP TABLE IF EXISTS {}").format(
                    sql.Identifier(new_name)
                )
                cursor.execute(query)
            query = sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                sql.Identifier(self.colname),
                sql.Identifier(new_name)
            )
            cursor.execute(query)
        self.colname = new_name

    def count(self) -> int:
        with self.get_cursor() as cursor:
            cursor.execute(f"SELECT COUNT(_id) FROM {self.colname}")
            return cursor.fetchone()[0]

    def drop(self):
        with self.get_cursor() as cursor:
            query = sql.SQL("DROP TABLE IF EXISTS {}").format(
                sql.Identifier(self.colname)
            )
            cursor.execute(query)

    def __getitem__(self, _id: str):
        return self.find_one({"_id": _id})

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("connection", None)
        state.pop("database", None)
        return state

    def exists(self):
        with self.get_cursor() as cursor:
            query = sql.SQL(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)"
            )
            cursor.execute(query, (self.colname.lower(),))
            return cursor.fetchone()[0]

    def list_tables(self):
        with self.get_cursor() as cursor:
            cursor.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
            return [row[0] for row in cursor.fetchall()]
