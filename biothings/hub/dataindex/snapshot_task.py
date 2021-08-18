
class Snapshot():

    def __init__(self, client, repository, snapshot):
        # Corresponds to HTTP operations on
        # /_snapshot/<repository>/<snapshot>

        self.client = client
        self.repository = repository
        self.snapshot = snapshot

    def exists(self):
        return bool(self.client.snapshot.get(
            self.repository, self.snapshot,
            ignore_unavailable=True
        )["snapshots"])

    def create(self, indices):
        self.client.snapshot.create(
            self.repository, self.snapshot,
            {
                "indices": indices,
                "include_global_state": False
            }
        )

    def state(self):
        snapshots = self.client.snapshot.get(
            self.repository, self.snapshot,
            ignore_unavailable=True
        )["snapshots"]

        if snapshots:  # [{...}]
            return snapshots[0]["state"]

        return "N/A"

    def delete(self):
        self.client.snapshot.delete(
            self.repository, self.snapshot)

    def __str__(self):
        return (
            f"<Snapshot {self.state()}"
            f" repository='{self.repository}'"
            f" snapshot='{self.snapshot}'"
            f" client={self.client}"
            f">"
        )

def test_01():
    from elasticsearch import Elasticsearch
    client = Elasticsearch()
    snapshot = Snapshot(client, "mynews", "mynews_202012280220_vsdevjdk")
    print(snapshot)

def test_02():
    from elasticsearch import Elasticsearch
    client = Elasticsearch()
    snapshot = Snapshot(client, "mynews", "____________________________")
    print(snapshot)


if __name__ == "__main__":
    test_02()
