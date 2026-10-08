"""
Find subfields that match the same documents as their parent object.

Store these aliases in `_meta.exists_field_aliases` so the web query builder
can replace object exists queries with subfield exists queries.
"""

import logging
import time

logger = logging.getLogger(__name__)

META_KEY = "exists_field_aliases"

# Skip objects with fewer than this many candidate leaf fields.
DEFAULT_MIN_SUBFIELDS = 8

# Object depth to scan: 0 includes top-level objects only; None includes all depths.
MAX_OBJECT_DEPTH = 1

# Number of subfield counts per _msearch request.
_COUNT_BATCH = 100


def _collect_subfields(properties, prefix=""):
    """Return leaf paths, skipping disabled fields and fields with index=False."""
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
    Return (object path, leaf paths) pairs through max_depth.

    Top-level objects have depth 0; None includes all depths.
    """
    objects = []
    for name, spec in (properties or {}).items():
        if not isinstance(spec, dict) or spec.get("enabled") is False:
            continue
        if "properties" not in spec:
            continue
        path = f"{prefix}{name}"
        objects.append((path, _collect_subfields(spec["properties"], f"{path}.")))
        if max_depth is None or depth < max_depth:
            objects.extend(_object_fields(spec["properties"], max_depth, f"{path}.", depth + 1))
    return objects


def _objects_to_scan(mapping_properties, min_subfields, max_depth=MAX_OBJECT_DEPTH):
    """Return objects meeting min_subfields, ordered by decreasing leaf count."""
    objects = [
        (path, subfields)
        for path, subfields in _object_fields(mapping_properties, max_depth)
        if len(subfields) >= min_subfields
    ]
    return sorted(objects, key=lambda item: len(item[1]), reverse=True)


async def _count_subfields(client, index, subfields):
    """Return document counts by subfield using batched _msearch requests."""
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
    """Choose by depth, then path length, then alphabetical order."""
    return min(candidates, key=lambda subfield: (subfield.count("."), len(subfield), subfield))


async def _derive_one_alias(client, index, field, subfields, logger):
    """Return a subfield with the same document count as the object, or None."""
    started = time.time()
    counts = await _count_subfields(client, index, subfields)
    if not counts:
        return None

    response = await client.search(
        index=index, body={"query": {"exists": {"field": field}}, "size": 0, "track_total_hits": True}
    )
    total = response["hits"]["total"]["value"]
    elapsed = time.time() - started

    if any(count > total for count in counts.values()):
        # Counts are inconsistent if a subfield matches more documents than its parent.
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
    Return {object_field: subfield} aliases after indexing is complete.

    Count matches for each candidate object and its leaf fields.
    """
    mappings = await client.indices.get_mapping(index=index)
    # The caller must provide a single concrete index.
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
    """Store aliases in `_meta` without replacing its other entries."""
    mappings = await client.indices.get_mapping(index=index)
    meta = next(iter(mappings.values()))["mappings"].get("_meta", {})
    meta[META_KEY] = aliases
    await client.indices.put_mapping(index=index, body={"_meta": meta})
    logger.info("Stored %s _exists_ alias(es) in '%s' _meta", len(aliases), index)
