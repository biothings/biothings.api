"""
Tests the indexer ability through elasticsearch
"""

from typing import Callable, Tuple

import elasticsearch
import pytest


@pytest.mark.xfail(reason="WIP: Have to figure out mongoDB / elasticsearch setup")
def test_task_index(task_clients: Tuple[Callable, Callable]):
    from biothings.hub.dataindex.indexer_task import IndexingTask

    task = IndexingTask(
        es=task_clients[0],
        mongo=task_clients[1],
        ids=("0999b13cb8026aba", "1111647aaf9c70b4", "1c9828073bad510c"),
    )
    task.index()


@pytest.mark.xfail(reason="WIP: Have to figure out mongoDB / elasticsearch setup")
def test_task_resume(task_clients: Tuple[Callable, Callable]):
    from biothings.hub.dataindex.indexer_task import IndexingTask

    task = IndexingTask(
        es=task_clients[0],
        mongo=task_clients[1],
        ids=(
            "0999b13cb8026aba",
            "1111647aaf9c70b4",
            "1c9828073bad510c",
            "1f447d7fc6dcc2cf",
            "27e81a308e4e04da",
        ),
    )
    task.resume()


@pytest.mark.xfail(reason="WIP: Have to figure out mongoDB / elasticsearch setup")
def test_elasticsearch_index():
    from biothings.hub.dataindex.indexer_task import ESIndex

    elasticsearch_host = "http://localhost:9200"
    index_name = "mynews_202105261855_5ffxvchx"
    elasticsearch_client = elasticsearch.Elasticsearch(hosts=elasticsearch_host)
    index_instance = ESIndex(elasticsearch_client, index_name)
    print(index_instance.doc_type)
    print(
        list(
            index_instance.mget(
                [
                    "0999b13cb8026aba",
                    "1111647aaf9c70b4",
                    "________________",
                ]
            )
        )
    )
    print(list(index_instance.mexists(["0999b13cb8026aba", "1111647aaf9c70b4", "________________"])))
