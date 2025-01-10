# DatabaseCollectionTesting

# Author: Amiteshk Sharma
# https://github.com/amiteshksharma/Database-Testing

import pymongo

# from jsonpath_ng import jsonpath, parse


class DatabaseCollectionTesting:
    """
    Constructor that takes in three items
    db_url      - string - the mongoDB url to connect to
    db          - string - name of DB to use
    collection  - string - name of collection in db
    """

    def __init__(self, db_url, db, collection):
        if "mongo" not in db_url:
            raise ValueError

        self.database_url = db_url
        self.client = pymongo.MongoClient(db_url)
        self.db = self.client[db]
        self.collection = self.db[collection]

    # test to see if there exists only 1 item for an ID
    # _id - the document _id to query
    def test_field_unique_id(self, _id):
        get_item = self.collection.find({"_id": _id})
        item = list(get_item)
        assert len(item) == 1

    # check for all items within a taxid
    # taxid - the taxid of the document to query
    def test_field_taxid(self, taxid):
        get_items = self.collection.find({"taxid": taxid})
        item_list = list(get_items)
        assert len(item_list) >= 1

    # check all documents with the same taxid value
    def test_documents_taxid(self, taxid):
        get_documents = self.collection.find({"taxid": taxid})
        doc_list = list(get_documents)

        sub_list = ["_id", "taxid", "name", "ensembl", "symbol"]

        for doc in doc_list:
            keys = doc.keys()
            if not all(x in keys for x in sub_list):
                # may not contain a name attribute
                if "name" not in keys:
                    pass
                # may not contain the ensembl attribute
                elif "ensembl" not in keys:
                    pass
                # may not contain the symbol attribute
                elif "symbol" not in keys:
                    pass
                else:
                    # assert False
                    raise AssertionError()

        assert True

    # check an _id and make sure it does not exist
    # _id - the document _id to query
    def test_field_does_not_exist(self, _id):
        get_items = self.collection.find({"_id": _id})
        item_list = list(get_items)
        assert len(item_list) == 0

    # Check number of documents is correct
    # expected count - the expected count of documents with specific taxid
    def test_total_document_count(self, expected_count):
        get_all_document = self.collection.find()
        document_list = list(get_all_document)
        assert len(document_list) == expected_count

    # check the indices for the mongoDB database
    def test_database_index(self):
        get_indices = self.collection.index_information()
        indices_list = list(get_indices)
        size = len(indices_list)

        # if only size 1, then it only has _id_ index
        if size == 1:
            assert all(x in indices_list for x in ["_id_"])
        elif size == 3:
            assert all(x in indices_list for x in ["_id_", "taxid_1", "entrezgene_1"])

    # test the name attribute on randomly selected items in the database
    def test_document_name(self):
        random_docs = self.collection.aggregate([{"$sample": {"size": 10}}])

        count = 0
        for doc in random_docs:
            if "name" in doc:
                count = count + 1
            else:
                print("_id for document with no name: " + doc["_id"])

        assert count == 10


if __name__ == "__main__":
    c = DatabaseCollectionTesting("mongodb://su05:27017", "genedoc", "mygene_allspecies_20191111_eeesndlz")
    c.test_documents_taxid(29302)
