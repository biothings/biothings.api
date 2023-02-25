
import elasticsearch


class Repository():

    def __init__(self, client, repository):
        # Corresponds to HTTP operations on
        # /_snapshot/<repository>

        self.client = client
        self.name = repository

    def exists(self):
        try:
            self.client.snapshot.get_repository(self.name)
        except elasticsearch.exceptions.NotFoundError:
            return False
        return True

    def create(self, **body):
        # https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-s3-client.html
        return self.client.snapshot.create_repository(self.name, body=body)

    def delete(self):
        self.client.snapshot.delete_repository(self.name)

    def verify(self):
        return self.client.snapshot.verify_repository(self.name)

    def __str__(self):
        return (
            f"<Repository {'READY' if self.exists() else 'MISSING'}"
            f" name='{self.name}'"
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
