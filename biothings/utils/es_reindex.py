import logging
import time
from typing import Optional, Union

from biothings.utils.es import ESIndexer

logger = logging.getLogger(__name__)


def reindex(
    src_index: str,
    target_index: Optional[str] = None,
    settings: Optional[dict] = None,
    mappings: Optional[dict] = None,
    alias: Optional[Union[bool, str]] = None,
    delete_src: Optional[bool] = False,
    task_check_interval: Optional[int] = 15,
    src_indexer_kwargs: Optional[dict] = None,
    target_indexer_kwargs: Optional[dict] = None,
    reindex_kwargs: Optional[dict] = None,
) -> None:
    """
    This helper function helps to reindex an existing index by transferring both the settings, mappings and docs.
    Mappings and settings from the src index will be used to create a new empty target index first.
    Then Elasticsearch's reindex API will be used to transfer all docs to the new one.
    Optionally, the alias can be switched over to the new index too.
    This is useful when we need to migrate existing indices created from the srcer ES version to the current ES version.

    Parameters:
        src_index: name of the src index

        target_index: name of the new index, use <src_index_name>_reindexed as default if None

        settings: if provided as a dict, update the settings with the provided dictionary.
                    Otherwise, keep the same from the src_index
        mapping: if provided as a dict, update the settings with the provided mappings.
                    Otherwise, keep the same from the src_index
        alias: if True, switch the alias from src_index to target_index.
                If src_index has no alias, apply the <src_index_name> as the alias;
                if a string value, apply it as the alias instead
        delete_src: If True, delete the src_index after everything is done,
        task_check_interval: the interval to check reindex job status, default is 15s
        src_indexer_kwargs: a dict contains infor to construct ESIndexer for src index
        target_indexer_kwargs: a dict contains infor to construct ESIndexer for target index
        reindex_kwargs: a dict contains extra parameters passed to reindex, e.g. {"timeout": "5m",
                        "slices": 10}. see
        https://www.elastic.co/guide/en/elasticsearch/reference/7.17/docs-reindex.html#docs-reindex-api-query-params
                        for available parameters. Note that "wait_for_completion" parameter
                        will always be set to False, so we will run reindex asynchronously

    Returns:
        None
    """

    logger.info("Starting reindex...")

    # create indexer objects
    logger.info("Create src index obj")
    src_indexer_kwargs = src_indexer_kwargs or {}
    src_index_obj = ESIndexer(index=src_index, **src_indexer_kwargs)
    assert src_index_obj.exists_index(), f"src index '{src_index}' does not exists."
    # clone settings, mappings from src index
    logger.info("Clone src settings, and src_mapping, and update with supplied values")
    src_settings = src_index_obj.get_settings(src_index_obj._index) or {}
    src_mappings = src_index_obj.get_mapping() or {}
    src_index_obj.number_of_shards = src_settings.get("number_of_shards", 1)
    src_index_obj.number_of_replicas = src_settings.get("number_of_replicas", 0)

    # update src settings, mappings with supplied values.
    if settings:
        src_settings.update(**settings)
    if mappings:
        src_mappings.update(**mappings)

    logger.info("Create target index obj")
    target_index = target_index or f"{src_index}_reindexed"
    target_indexer_kwargs = target_indexer_kwargs or {}
    if "number_of_shards" not in target_indexer_kwargs:
        target_indexer_kwargs["number_of_shards"] = src_index_obj.number_of_shards
    if "number_of_replicas" not in target_indexer_kwargs:
        target_indexer_kwargs["number_of_replicas"] = src_index_obj.number_of_replicas
    target_index_obj = ESIndexer(index=target_index, **target_indexer_kwargs)
    assert not target_index_obj.exists_index(), f"target index '{target_index}' exists."

    # create target index with src settings, mappings
    logger.info("Set target index's settings, mappings")
    target_index_obj.create_index(mapping=src_mappings, extra_settings=src_settings[src_index_obj._index]["settings"])

    # Reindex from src index to target index
    logger.info("Reindex from src index to target index")
    is_remote = src_index_obj.es_host != target_index_obj.es_host
    reindex_kwargs = reindex_kwargs or {}
    # always run reindex asynchronously, otherwise reindex method below tends to be timeout eventually
    reindex_kwargs["wait_for_completion"] = False
    result = target_index_obj.reindex(src_index_obj, is_remote=is_remote, **reindex_kwargs)
    # result: {'task': '4ohWVeJGQVq5lZCNJgkPag:149689'}
    task_id = result.get("task")
    logger.info('Reindex job started [task id: "%s"]:', task_id)
    # check task status till it's completed
    while 1:
        task_obj = target_index_obj._es.tasks.get(task_id)
        task_status = task_obj.get("task", {}).get("status", {})
        logger.info(
            "\t[Progress] completed: %s, batches: %s, created: %s",
            task_obj.get("completed"),
            task_status.get("batches"),
            task_status.get("created"),
        )
        if task_obj.get("completed", False) is True:
            break
        time.sleep(task_check_interval)

    # Refresh and flush target index
    logger.info("Flush and refresh target index")
    target_index_obj.flush_and_refresh()

    # Check doc counts to make sure src_index, new index are equals
    if "max_docs" in reindex_kwargs:
        # if max_docs is set, the target_index will only contain that number of docs
        count_src_docs = reindex_kwargs["max_docs"]
    else:
        count_src_docs = src_index_obj.count()
    count_target_docs = target_index_obj.count()
    assert (
        count_src_docs == count_target_docs
    ), "Number of docs in target index not equal to number of docs in src index"
    logger.info("Verified number of docs in target index is equal to number of docs in src index")

    # Set target index's alias
    if alias:
        logger.info("Update target index's alias")
        if isinstance(alias, str):
            new_alias = alias
        else:
            try:
                alias_data = src_index_obj.get_alias(index=src_index_obj._index)
                new_alias = list(alias_data[src_index_obj._index]["aliases"].keys())[0]
            except:  # noqa
                # default to src_index name if src index is deleted or has no alias.
                new_alias = src_index_obj._index
        target_index_obj.update_alias(new_alias)

    # Delete src index
    if delete_src:
        logger.info("Delete src alias")
        src_index_obj.delete_index()

    logger.info("Finished!")
