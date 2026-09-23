"""
Derive '_exists_:<object field>' aliases for a newly built index.

Elasticsearch runs an exists query on an object field as an OR over every
leaf field under it. If a subfield is present in every document that has the
object, an exists query on that subfield returns the same documents, with a
fraction of the cost. This module finds that subfield for each object field
and saves the result in the index's `_meta.exists_field_aliases`, where the
web tier uses it to rewrite the query.
"""

import logging
import time

logger = logging.getLogger(__name__)

META_KEY = "exists_field_aliases"

# object fields with fewer subfields than this are not scanned
DEFAULT_MIN_SUBFIELDS = 8

# how many nested levels to scan, from the root (0 = root only, None = all).
MAX_OBJECT_DEPTH = 1

# subfield counts requested per _msearch batch
_COUNT_BATCH = 100


def _collect_subfields(properties, prefix=""):
    """Dotted paths of every queryable leaf below an object (skips unindexed/disabled fields)."""
    subfields = []
    for name, spec in (properties or {}).items():
        if not isinstance(spec, dict) or spec.get("enabled") is False:
            continue
        path = f"{prefix}{name}"
        if "properties" in spec:
            subfields.extend(_collect_subfields(spec["properties"], f"{path}."))
        elif spec.get("index") is not False:
            subfields.append(path)
    return subfields


def _object_fields(properties, max_depth, prefix="", depth=0):
    """
    [(object_field, [subfield, ...]), ...] for every object field down to
    max_depth levels below the root. A parent and its nested child are both
    reported, since they need separate aliases.
    """
    objects = []
    for name, spec in (properties or {}).items():
        if not isinstance(spec, dict) or spec.get("enabled") is False:
            continue
        if "properties" not in spec:
            continue  # leaf field, no alias needed
        path = f"{prefix}{name}"
        objects.append((path, _collect_subfields(spec["properties"], f"{path}.")))
        if max_depth is None or depth < max_depth:
            objects.extend(_object_fields(spec["properties"], max_depth, f"{path}.", depth + 1))
    return objects


def _objects_to_scan(mapping_properties, min_subfields, max_depth=MAX_OBJECT_DEPTH):
    """Object fields with at least min_subfields subfields, largest first."""
    objects = [
        (path, subfields)
        for path, subfields in _object_fields(mapping_properties, max_depth)
        if len(subfields) >= min_subfields
    ]
    return sorted(objects, key=lambda item: len(item[1]), reverse=True)


async def _count_subfields(client, index, subfields):
    """{subfield: doc count}, batched through _msearch."""
    counts = {}
    for start in range(0, len(subfields), _COUNT_BATCH):
        batch = subfields[start : start + _COUNT_BATCH]
        body = []
        for subfield in batch:
            body.append({"index": index})
            body.append({"query": {"exists": {"field": subfield}}, "size": 0, "track_total_hits": True})
        responses = (await client.msearch(body=body))["responses"]
        for subfield, response in zip(batch, responses):
            if "error" in response:
                logger.warning("Count failed for '%s': %s", subfield, response["error"])
                continue
            counts[subfield] = response["hits"]["total"]["value"]
    return counts


def _pick_alias(candidates):
    """Deterministic tie-break among valid subfields: shallowest, then shortest, then alphabetical."""
    return min(candidates, key=lambda subfield: (subfield.count("."), len(subfield), subfield))


async def _derive_one_alias(client, index, field, subfields, logger):
    """Return the subfield to use as the alias for one object field, or None if none matches."""
    started = time.time()
    counts = await _count_subfields(client, index, subfields)
    if not counts:
        return None

    # number of documents that have the object
    response = await client.search(
        index=index, body={"query": {"exists": {"field": field}}, "size": 0, "track_total_hits": True}
    )
    total = response["hits"]["total"]["value"]
    elapsed = time.time() - started

    if any(count > total for count in counts.values()):
        # a subfield cannot have more documents than its parent; the mapping is stale
        logger.warning("Skipping '%s': subfield count exceeds object count, mapping may be stale", field)
        return None

    matching = [subfield for subfield, count in counts.items() if count == total]
    if not matching:
        logger.info(
            "  %s -> no subfield present in all %s docs, left as is (%s subfields, scanned in %.1fs)",
            field,
            total,
            len(subfields),
            elapsed,
        )
        return None

    alias = _pick_alias(matching)
    logger.info(
        "  %s -> %s (%s/%s subfields present in all %s docs, scanned in %.1fs)",
        field,
        alias,
        len(matching),
        len(subfields),
        total,
        elapsed,
    )
    return alias


async def derive_exists_field_aliases(
    client,
    index,
    min_subfields=DEFAULT_MIN_SUBFIELDS,
    max_depth=MAX_OBJECT_DEPTH,
    logger=logger,
):
    """
    Return {object_field: subfield} for one index. Runs one exists query per
    scanned object field, so it should run once, right after indexing.
    """
    mappings = await client.indices.get_mapping(index=index)
    # the index name resolves to one concrete index
    properties = next(iter(mappings.values()))["mappings"].get("properties", {})

    targets = _objects_to_scan(properties, min_subfields, max_depth)
    logger.info(
        "Deriving _exists_ aliases for %s object field(s) in '%s' (max_depth=%s)",
        len(targets),
        index,
        "all" if max_depth is None else max_depth,
    )

    aliases = {}
    for field, subfields in targets:
        alias = await _derive_one_alias(client, index, field, subfields, logger)
        if alias:
            aliases[field] = alias

    return aliases


async def store_exists_field_aliases(client, index, aliases, logger=logger):
    """Save the alias map in the index `_meta`, keeping the other keys."""
    mappings = await client.indices.get_mapping(index=index)
    meta = next(iter(mappings.values()))["mappings"].get("_meta", {})
    meta[META_KEY] = aliases
    await client.indices.put_mapping(index=index, body={"_meta": meta})
    logger.info("Stored %s _exists_ alias(es) in '%s' _meta", len(aliases), index)
