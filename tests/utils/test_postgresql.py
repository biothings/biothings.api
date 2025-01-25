import os

import psycopg2
import psycopg2.errors
import pytest

from biothings.utils.postgresql import Collection, Database


@pytest.fixture(scope="module")
def db():
    # Set up the test database connection
    dbname = os.getenv("PG_DBNAME")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT")
    db = Database(dbname=dbname, user=user, password=password, host=host, port=port)
    db.connection.autocommit = True
    yield db
    # Teardown the database connection
    db.close()


@pytest.fixture()
def collection(db):
    # Define collection names
    original_name = "testcollection"
    renamed_name = "renamed_collection"

    # Create fresh collection instances
    collection = Collection(original_name, db)
    renamed_collection = Collection(renamed_name, db)

    # Drop both collections before each test
    collection.drop()
    renamed_collection.drop()

    # Ensure the original collection exists
    collection.ensure_table_exists()

    yield collection  # Provide the fixture to the test

    # Recreate collection instances for teardown
    collection = Collection(original_name, db)
    renamed_collection = Collection(renamed_name, db)

    # Drop both collections after each test
    collection.drop()
    renamed_collection.drop()


def test_insert_one(collection):
    doc = {"_id": "1", "name": "Test Document"}
    collection.insert_one(doc)
    result = collection.find_one({"_id": "1"})
    assert result == doc


def test_find_one(collection):
    doc1 = {"_id": "1", "name": "Document 1"}
    doc2 = {"_id": "2", "name": "Document 2"}
    collection.insert_one(doc1)
    collection.insert_one(doc2)
    result = collection.find_one({"name": "Document 2"})
    assert result == doc2


def test_find(collection):
    doc1 = {"_id": "1", "name": "Document 1"}
    doc2 = {"_id": "2", "name": "Document 2"}
    doc3 = {"_id": "3", "name": "Document 3"}
    collection.insert_one(doc1)
    collection.insert_one(doc2)
    collection.insert_one(doc3)
    results = list(collection.find({"name": "Document 2"}))
    assert len(results) == 1
    assert results[0] == doc2


def test_update_many(collection):
    doc1 = {"_id": "1", "name": "Document", "value": 1}
    doc2 = {"_id": "2", "name": "Document", "value": 2}
    collection.insert_one(doc1)
    collection.insert_one(doc2)
    collection.update_many({"name": "Document"}, {"$set": {"value": 100}})
    results = list(collection.find({"name": "Document"}))
    assert len(results) == 2
    for doc in results:
        assert doc["value"] == 100


def test_replace_one(collection):
    doc1 = {"_id": "1", "name": "Document 1"}
    doc2 = {"_id": "2", "name": "Document 2"}
    collection.insert_one(doc1)
    collection.replace_one({"_id": "1"}, doc2)
    result = collection.find_one({"_id": "1"})
    assert result == doc2


def test_replace_one_upsert(collection):
    doc = {"_id": "1", "name": "Document"}
    collection.replace_one({"_id": "1"}, doc, upsert=True)
    result = collection.find_one({"_id": "1"})
    assert result == doc


def test_remove(collection):
    doc1 = {"_id": "1", "name": "Document 1"}
    doc2 = {"_id": "2", "name": "Document 2"}
    collection.insert_one(doc1)
    collection.insert_one(doc2)
    collection.remove({"_id": "1"})
    result = collection.find_one({"_id": "1"})
    assert result is None
    result = collection.find_one({"_id": "2"})
    assert result == doc2


def test_remove_many(collection):
    doc1 = {"_id": "1", "group": "A"}
    doc2 = {"_id": "2", "group": "A"}
    doc3 = {"_id": "3", "group": "B"}
    collection.insert_one(doc1)
    collection.insert_one(doc2)
    collection.insert_one(doc3)
    collection.remove_many({"group": "A"})
    results = list(collection.find({}))
    assert len(results) == 1
    assert results[0] == doc3


def test_rename(collection, db):
    doc = {"_id": "1", "name": "Document"}
    collection.insert_one(doc)
    old_name = collection.colname
    new_name = "renamed_collection"

    collection.rename(new_name)

    assert collection.colname == new_name

    result = collection.find_one({"_id": "1"})
    assert result == doc

    old_collection = Collection(old_name, db)

    if old_collection.exists():
        # If it exists, check if it's empty
        result = old_collection.find_one({"_id": "1"})
        assert result is None
    else:
        # If it doesn't exist, the test passes
        pass


def test_count(collection):
    doc1 = {"_id": "1"}
    doc2 = {"_id": "2"}
    collection.insert_one(doc1)
    collection.insert_one(doc2)
    count = collection.count()
    assert count == 2


def test_drop(collection):
    doc = {"_id": "1"}
    collection.insert_one(doc)
    collection.drop()
    # Ensure table no longer exists
    with collection.get_cursor() as cursor:
        cursor.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", (collection.colname,)
        )
        exists = cursor.fetchone()[0]
        assert not exists


def test_document_with_no_id(collection):
    doc = {"name": "No ID Document"}
    with pytest.raises(ValueError):
        collection.insert_one(doc)


def test_update_with_unsupported_operation(collection):
    doc = {"_id": "1", "name": "Document"}
    collection.insert_one(doc)
    with pytest.raises(NotImplementedError):
        collection.update_many({"_id": "1"}, {"$unset": {"name": ""}})


def test_insert_duplicate_id(collection, db):
    doc1 = {"_id": "1", "name": "Document 1"}
    doc2 = {"_id": "1", "name": "Document 2"}
    collection.insert_one(doc1)
    with pytest.raises(psycopg2.errors.UniqueViolation):
        collection.insert_one(doc2)
    db.connection.rollback()


def test_getitem(collection):
    doc = {"_id": "1", "name": "Document"}
    collection.insert_one(doc)
    result = collection["1"]
    assert result == doc


if __name__ == "__main__":
    pytest.main(["-v", "postgres_testing.py"])
