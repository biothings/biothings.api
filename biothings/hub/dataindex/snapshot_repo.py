
import elasticsearch

class Repository():

    def __init__(self, client, repository):
        # Corresponds to HTTP operations on
        # /_snapshot/<repository>

        self.client = client
        self.repository = repository

    def exists(self):
        try:
            self.client.snapshot.get_repository(self.repository)
        except elasticsearch.exceptions.NotFoundError:
            return False
        return True

    def create(self, **body):
        self.client.snapshot.create_repository(self.repository, **body)

    def delete(self):
        self.client.snapshot.delete_repository(self.repository)

    def __str__(self):
        return (
            f"<Repository {'READY' if self.exists() else 'MISSING'}"
            f" name='{self.repository}'"
            f" client={self.client}"
            f">"
        )

def test_01():
    from elasticsearch import Elasticsearch
    client = Elasticsearch()
    snapshot = Repository(client, "mynews")
    print(snapshot)

def test_02():
    from elasticsearch import Elasticsearch
    client = Elasticsearch()
    snapshot = Repository(client, "______")
    print(snapshot)


if __name__ == "__main__":
    test_01()
